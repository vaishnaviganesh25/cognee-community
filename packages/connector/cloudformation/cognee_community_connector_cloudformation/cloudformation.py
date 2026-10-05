"""AWS CloudFormation connector for cognee — a ``dlt`` source that turns stacks into memory.

Each CloudFormation stack becomes one markdown document (what it deploys, its
parameters, outputs, tags, drift status and recent failures), ingested through
cognee's document path so it goes through normal cognify::

    import cognee
    from cognee_community_connector_cloudformation import cloudformation_source

    await cognee.remember(
        cloudformation_source(regions=["ap-south-1"]),
        dataset_name="aws_stacks",
        primary_key="id",
        write_disposition="merge",   # REQUIRED for incremental sync
    )

Design
------
* **Auth** — the standard boto3 credential chain (env vars, shared profile,
  ``aws login`` session, instance/role credentials). Only read-only calls are
  made: ``ListStacks``, ``DescribeStacks``, ``ListStackResources`` and
  ``DescribeStackEvents``. Templates are never fetched, because template
  bodies can contain hard-coded secrets.
* **Primary key** — the stack ARN (``StackId``). A stack deleted and recreated
  under the same name gets a new ARN, so the two are separate documents.
* **Incremental sync** — ``ListStacks`` returns every stack's status and
  last-change time in one paginated call. The connector stores a per-stack
  fingerprint in dlt resource state and only describes stacks whose
  fingerprint changed, so an unchanged account costs one listing per region.
* **Forget-on-delete** — a stack that was synced before but is no longer
  active (``DELETE_COMPLETE``, or out of scope after a filter change) is
  emitted with the ``_deleted`` hard-delete marker. dlt drops the row on
  ``merge`` and cognee's orphan cleanup removes the document from memory.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import datetime
from typing import Any
from urllib.parse import quote

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

logger = get_logger("cloudformation_connector")

CLOUDFORMATION_SOURCE_NAME = "cloudformation"
DEFAULT_RESOURCE_NAME = "cloudformation_stacks"

# ListStacks keeps deleted stacks for 90 days with this status.
_DELETED_STATUS = "DELETE_COMPLETE"

# DescribeStackEvents returns the newest events first. Reading a bounded
# window keeps a long-lived stack with thousands of events from turning every
# sync into an unbounded crawl.
_MAX_EVENTS_SCANNED = 200

_EXTRA_HINT = (
    "The CloudFormation connector requires boto3 and dlt: "
    "pip install cognee-community-connector-cloudformation"
)


# ---------------------------------------------------------------------------
# AWS client
# ---------------------------------------------------------------------------
def _make_client(region: str, profile_name: str | None) -> Any:
    """Build a read-only CloudFormation client for ``region``.

    Adaptive retry mode backs off on ``Throttling`` errors, which
    ``DescribeStackEvents`` hits quickly on accounts with many stacks.
    """
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise ImportError(_EXTRA_HINT) from exc

    session = boto3.Session(profile_name=profile_name)
    return session.client(
        "cloudformation",
        region_name=region,
        config=Config(retries={"mode": "adaptive", "max_attempts": 10}),
    )


def _default_region(profile_name: str | None) -> str | None:
    try:
        import boto3
    except ImportError as exc:
        raise ImportError(_EXTRA_HINT) from exc
    return boto3.Session(profile_name=profile_name).region_name


def _paginate(client: Any, operation: str, result_key: str, **kwargs: Any) -> Iterator[dict]:
    paginator = client.get_paginator(operation)
    for page in paginator.paginate(**kwargs):
        yield from page.get(result_key, []) or []


# ---------------------------------------------------------------------------
# Rendering (pure)
# ---------------------------------------------------------------------------
def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value else ""


def _changed_at(summary: dict) -> str:
    return _iso(summary.get("LastUpdatedTime") or summary.get("CreationTime"))


def _drift_status(stack: dict) -> str:
    return (stack.get("DriftInformation") or {}).get("StackDriftStatus") or "NOT_CHECKED"


def _fingerprint(summary: dict) -> str:
    """Identify one observable version of a stack.

    Drift detection updates the drift status without touching
    ``LastUpdatedTime``, so the status is part of the fingerprint.
    """
    return "|".join((_changed_at(summary), summary.get("StackStatus", ""), _drift_status(summary)))


def _console_url(region: str, stack_id: str) -> str:
    return (
        f"https://{region}.console.aws.amazon.com/cloudformation/home"
        f"?region={region}#/stacks/stackinfo?stackId={quote(stack_id, safe='')}"
    )


def _bullets(items: Iterable[str]) -> list[str]:
    lines = [f"- {item}" for item in items]
    return lines or ["- (none)"]


def render_stack(
    stack: dict,
    resources: list[dict],
    failures: list[dict],
    *,
    region: str,
    include_parameter_values: bool = True,
) -> str:
    """Render a described stack as deterministic markdown.

    Lists are sorted by key so the same stack state always produces the same
    text: a byte-identical document keeps its content hash, and cognee skips
    re-cognifying it.
    """
    name = stack.get("StackName", "")
    lines = [
        f"CloudFormation stack `{name}` in region {region}.",
        "",
        f"- Status: {stack.get('StackStatus', '')}",
    ]
    if stack.get("StackStatusReason"):
        lines.append(f"- Status reason: {stack['StackStatusReason']}")
    lines.append(f"- Created: {_iso(stack.get('CreationTime'))}")
    if stack.get("LastUpdatedTime"):
        lines.append(f"- Last updated: {_iso(stack['LastUpdatedTime'])}")
    lines.append(f"- Drift status: {_drift_status(stack)}")
    if stack.get("ParentId"):
        lines.append(f"- Nested stack of: {stack['ParentId']}")
    if stack.get("EnableTerminationProtection"):
        lines.append("- Termination protection: enabled")

    if stack.get("Description"):
        lines += ["", "## Description", stack["Description"].strip()]

    parameters = sorted(stack.get("Parameters") or [], key=lambda p: p.get("ParameterKey", ""))
    lines += ["", "## Parameters"]
    if include_parameter_values:
        # ParameterValue, never ResolvedValue: for SSM-backed parameters the
        # resolved value is read from Parameter Store and may be sensitive.
        lines += _bullets(
            f"{p.get('ParameterKey')} = {p.get('ParameterValue', '')}" for p in parameters
        )
    else:
        lines += _bullets(str(p.get("ParameterKey")) for p in parameters)

    lines += ["", "## Outputs"]
    output_lines = []
    for output in sorted(stack.get("Outputs") or [], key=lambda o: o.get("OutputKey", "")):
        line = f"{output.get('OutputKey')} = {output.get('OutputValue', '')}"
        if output.get("ExportName"):
            line += f" (exported as {output['ExportName']})"
        if output.get("Description"):
            line += f": {output['Description']}"
        output_lines.append(line)
    lines += _bullets(output_lines)

    lines += ["", "## Tags"]
    lines += _bullets(
        f"{tag.get('Key')} = {tag.get('Value', '')}"
        for tag in sorted(stack.get("Tags") or [], key=lambda t: t.get("Key", ""))
    )

    lines += ["", "## Resources"]
    resource_lines = []
    for res in sorted(resources, key=lambda r: r.get("LogicalResourceId", "")):
        line = (
            f"{res.get('LogicalResourceId')} ({res.get('ResourceType')}): "
            f"{res.get('ResourceStatus', '')}"
        )
        if res.get("PhysicalResourceId"):
            line += f", physical id {res['PhysicalResourceId']}"
        resource_lines.append(line)
    lines += _bullets(resource_lines)

    if failures:
        lines += ["", "## Recent failures"]
        lines += _bullets(
            f"{_iso(event.get('Timestamp'))} {event.get('LogicalResourceId')} "
            f"({event.get('ResourceType')}) {event.get('ResourceStatus')}: "
            f"{event.get('ResourceStatusReason', '')}"
            for event in failures
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CloudFormation reads
# ---------------------------------------------------------------------------
def _recent_failures(client: Any, stack_id: str, limit: int) -> list[dict]:
    if limit <= 0:
        return []
    failures: list[dict] = []
    for scanned, event in enumerate(
        _paginate(client, "describe_stack_events", "StackEvents", StackName=stack_id), start=1
    ):
        if str(event.get("ResourceStatus", "")).endswith("_FAILED"):
            failures.append(event)
            if len(failures) >= limit:
                break
        if scanned >= _MAX_EVENTS_SCANNED:
            break
    return failures


def _stack_row(
    client: Any,
    summary: dict,
    *,
    region: str,
    max_failure_events: int,
    include_parameter_values: bool,
) -> dict[str, Any]:
    stack_id = summary["StackId"]
    stack = client.describe_stacks(StackName=stack_id)["Stacks"][0]
    resources = list(
        _paginate(client, "list_stack_resources", "StackResourceSummaries", StackName=stack_id)
    )
    failures = _recent_failures(client, stack_id, max_failure_events)
    return {
        "id": stack_id,
        "title": f"CloudFormation stack {stack.get('StackName', '')} ({region})",
        "url": _console_url(region, stack_id),
        "content": render_stack(
            stack,
            resources,
            failures,
            region=region,
            include_parameter_values=include_parameter_values,
        ),
        "region": region,
        "stack_name": stack.get("StackName", ""),
        "_deleted": False,
    }


def _deleted_row(stack_id: str) -> dict[str, Any]:
    return {"id": stack_id, "_deleted": True}


# ---------------------------------------------------------------------------
# Sync (pure given clients + a state dict — unit-testable)
# ---------------------------------------------------------------------------
def sync_stacks(
    clients: dict[str, Any],
    state: dict,
    *,
    stack_name_prefix: str | None = None,
    include_nested: bool = True,
    max_stacks: int = 500,
    max_failure_events: int = 10,
    include_parameter_values: bool = True,
) -> Iterator[dict[str, Any]]:
    """Yield rows for new or changed stacks, then hard-delete markers.

    ``state["fingerprints"]`` maps each synced stack ARN to its fingerprint
    from the previous run. It is only rewritten after the listing and every
    describe call succeeded, so a failed run is retried in full next time.
    """
    known: dict[str, str] = dict(state.get("fingerprints") or {})
    active: dict[str, tuple[str, dict]] = {}
    confirmed_deleted: set[str] = set()

    for region, client in clients.items():
        for summary in _paginate(client, "list_stacks", "StackSummaries"):
            if stack_name_prefix and not summary.get("StackName", "").startswith(stack_name_prefix):
                continue
            if not include_nested and summary.get("ParentId"):
                continue
            if summary.get("StackStatus") == _DELETED_STATUS:
                confirmed_deleted.add(summary["StackId"])
                continue
            active[summary["StackId"]] = (region, summary)

    if len(active) > max_stacks:
        raise ValueError(
            f"Found {len(active)} CloudFormation stacks in scope, more than "
            f"max_stacks={max_stacks}. Narrow the scope with regions= or "
            "stack_name_prefix=, or raise max_stacks."
        )

    fingerprints: dict[str, str] = {}
    changed = 0
    for stack_id, (region, summary) in active.items():
        fingerprint = _fingerprint(summary)
        fingerprints[stack_id] = fingerprint
        if known.get(stack_id) == fingerprint:
            continue
        yield _stack_row(
            clients[region],
            summary,
            region=region,
            max_failure_events=max_failure_events,
            include_parameter_values=include_parameter_values,
        )
        changed += 1

    gone = set(known) - set(active)
    if not active and known:
        # An empty listing is far more often a wrong region or missing
        # permission than a real wipe. Only forget stacks that AWS explicitly
        # reports as deleted; keep the rest until a listing shows them again.
        unconfirmed = gone - confirmed_deleted
        if unconfirmed:
            logger.warning(
                "CloudFormation: listing returned no active stacks; keeping %d previously "
                "synced stack(s) that are not reported as DELETE_COMPLETE.",
                len(unconfirmed),
            )
            fingerprints.update({stack_id: known[stack_id] for stack_id in unconfirmed})
            gone &= confirmed_deleted

    for stack_id in sorted(gone):
        yield _deleted_row(stack_id)

    state["fingerprints"] = fingerprints
    logger.info("CloudFormation: %d changed stack(s), %d deletion(s).", changed, len(gone))


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def cloudformation_source(
    *,
    regions: list[str] | None = None,
    stack_name_prefix: str | None = None,
    include_nested: bool = True,
    include_parameter_values: bool = True,
    max_stacks: int = 500,
    max_failure_events: int = 10,
    profile_name: str | None = None,
    resource_name: str = DEFAULT_RESOURCE_NAME,
    clients: dict[str, Any] | None = None,
):
    """Return a ``dlt`` resource that yields CloudFormation stacks for ``remember``.

    Args:
        regions: AWS regions to sync. Defaults to the session's configured
            region (``AWS_REGION`` / profile ``region``).
        stack_name_prefix: Only sync stacks whose name starts with this.
        include_nested: Also sync nested stacks (those with a ``ParentId``).
        include_parameter_values: Render parameter values, not just names.
            ``NoEcho`` parameters are always masked by AWS.
        max_stacks: Abort before describing anything if more active stacks
            than this are in scope.
        max_failure_events: Failed events to keep per stack (0 disables).
        profile_name: Named AWS profile to use instead of the default chain.
        resource_name: dlt table name. Use a distinct name per account when
            several accounts sync into the same dataset.
        clients: ``{region: client}`` mapping of pre-built CloudFormation
            clients, mainly for tests. Overrides ``regions``/``profile_name``.

    Returns:
        A ``dlt`` resource with ``primary_key="id"``, ``write_disposition="merge"``
        and an ``_deleted`` hard-delete column. Pass ``write_disposition="merge"``
        to ``cognee.remember`` as well, since cognee's default is ``replace``.
    """
    try:
        import dlt
    except ImportError as exc:
        raise ImportError(_EXTRA_HINT) from exc

    if getattr(dlt_utils, "DOCUMENT_SYNC_VERSION", 0) < 1:
        raise RuntimeError(
            "The CloudFormation connector needs a cognee build with table-scoped dlt "
            "document cleanup (cognee>=1.6). Upgrade cognee."
        )

    if clients is None:
        resolved_regions = regions or [_default_region(profile_name)]
        if not all(resolved_regions):
            raise ValueError(
                "No AWS region configured: pass regions=[...] or set AWS_REGION / a profile region."
            )
        region_list = sorted(set(resolved_regions))
    else:
        region_list = sorted(clients)

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        # Rows with _deleted=True are removed from the dlt destination on merge,
        # which is what lets cognee's orphan cleanup forget the stack.
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def cloudformation_stacks():
        resolved_clients = clients or {
            region: _make_client(region, profile_name) for region in region_list
        }
        yield from sync_stacks(
            resolved_clients,
            dlt.current.resource_state(),
            stack_name_prefix=stack_name_prefix,
            include_nested=include_nested,
            max_stacks=max_stacks,
            max_failure_events=max_failure_events,
            include_parameter_values=include_parameter_values,
        )

    resource = cloudformation_stacks()
    setattr(resource, DOCUMENT_SOURCE_ATTR, CLOUDFORMATION_SOURCE_NAME)
    setattr(resource, dlt_utils.PIPELINE_SCOPE_ATTR, resource_name)
    return resource
