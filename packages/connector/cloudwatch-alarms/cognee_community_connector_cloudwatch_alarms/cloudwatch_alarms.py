"""Amazon CloudWatch Alarms connector for cognee — a ``dlt`` source that turns alarms into memory.

Each alarm becomes one markdown document (what it watches, its threshold, its
current state and its recent state changes), ingested through cognee's
document path so it goes through normal cognify::

    import cognee
    from cognee_community_connector_cloudwatch_alarms import cloudwatch_alarms_source

    await cognee.remember(
        cloudwatch_alarms_source(regions=["ap-south-1"]),
        dataset_name="aws_alarms",
        primary_key="id",
        write_disposition="merge",   # REQUIRED for incremental sync
    )

Design
------
* **Auth** — the standard boto3 credential chain. Only read-only calls are
  made: ``DescribeAlarms`` and ``DescribeAlarmHistory``.
* **Primary key** — the alarm ARN.
* **Incremental sync** — ``DescribeAlarms`` returns every alarm with its state
  and the time its configuration and state last changed. The connector keeps
  a per-alarm fingerprint in dlt resource state and only fetches history for
  alarms whose fingerprint changed, so an unchanged account costs one
  paginated listing per region.
* **Forget-on-delete** — an alarm synced before but missing from this run's
  listing is emitted with the ``_deleted`` hard-delete marker. dlt drops the
  row on ``merge`` and cognee's orphan cleanup removes the document. CloudWatch
  keeps alarm history for 30 days, so older state changes never appear.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import datetime
from typing import Any
from urllib.parse import quote

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

logger = get_logger("cloudwatch_alarms_connector")

CLOUDWATCH_ALARMS_SOURCE_NAME = "cloudwatch-alarms"
DEFAULT_RESOURCE_NAME = "cloudwatch_alarms"

_EXTRA_HINT = (
    "The CloudWatch Alarms connector requires boto3 and dlt: "
    "pip install cognee-community-connector-cloudwatch-alarms"
)


# ---------------------------------------------------------------------------
# AWS client
# ---------------------------------------------------------------------------
def _make_client(region: str, profile_name: str | None) -> Any:
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise ImportError(_EXTRA_HINT) from exc

    session = boto3.Session(profile_name=profile_name)
    return session.client(
        "cloudwatch",
        region_name=region,
        config=Config(retries={"mode": "adaptive", "max_attempts": 10}),
    )


def _default_region(profile_name: str | None) -> str | None:
    try:
        import boto3
    except ImportError as exc:
        raise ImportError(_EXTRA_HINT) from exc
    return boto3.Session(profile_name=profile_name).region_name


def _list_alarms(client: Any, alarm_name_prefix: str | None) -> Iterator[tuple[str, dict]]:
    """Yield ``(kind, alarm)`` for every metric and composite alarm."""
    kwargs: dict[str, Any] = {"AlarmTypes": ["MetricAlarm", "CompositeAlarm"]}
    if alarm_name_prefix:
        kwargs["AlarmNamePrefix"] = alarm_name_prefix
    for page in client.get_paginator("describe_alarms").paginate(**kwargs):
        for alarm in page.get("MetricAlarms") or []:
            yield "metric", alarm
        for alarm in page.get("CompositeAlarms") or []:
            yield "composite", alarm


def _recent_state_changes(client: Any, alarm_name: str, limit: int) -> list[dict]:
    if limit <= 0:
        return []
    response = client.describe_alarm_history(
        AlarmName=alarm_name,
        HistoryItemType="StateUpdate",
        ScanBy="TimestampDescending",
        MaxRecords=min(limit, 100),
    )
    return list(response.get("AlarmHistoryItems") or [])[:limit]


# ---------------------------------------------------------------------------
# Rendering (pure)
# ---------------------------------------------------------------------------
def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value else ""


def _bullets(items: Iterable[str]) -> list[str]:
    lines = [f"- {item}" for item in items]
    return lines or ["- (none)"]


def _fingerprint(alarm: dict) -> str:
    """Identify one observable version of an alarm: its config and its state."""
    return "|".join(
        (
            _iso(alarm.get("AlarmConfigurationUpdatedTimestamp")),
            alarm.get("StateValue", ""),
            _iso(alarm.get("StateUpdatedTimestamp")),
        )
    )


def _console_url(region: str, alarm_name: str) -> str:
    return (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
        f"#alarmsV2:alarm/{quote(alarm_name, safe='')}"
    )


def _metric_lines(alarm: dict) -> list[str]:
    lines = []
    if alarm.get("MetricName"):
        lines.append(f"Metric: {alarm.get('Namespace', '')}/{alarm['MetricName']}")
    statistic = alarm.get("Statistic") or alarm.get("ExtendedStatistic")
    if statistic:
        lines.append(f"Statistic: {statistic}")
    if alarm.get("Period"):
        lines.append(f"Period: {alarm['Period']} seconds")
    if alarm.get("ComparisonOperator"):
        threshold = alarm.get("Threshold", alarm.get("ThresholdMetricId", ""))
        lines.append(f"Condition: {alarm['ComparisonOperator']} {threshold}")
    if alarm.get("EvaluationPeriods"):
        evaluation = f"Evaluation periods: {alarm['EvaluationPeriods']}"
        if alarm.get("DatapointsToAlarm"):
            evaluation += f" ({alarm['DatapointsToAlarm']} datapoints to alarm)"
        lines.append(evaluation)
    if alarm.get("TreatMissingData"):
        lines.append(f"Missing data: {alarm['TreatMissingData']}")
    for query in alarm.get("Metrics") or []:
        stat = query.get("MetricStat") or {}
        metric = stat.get("Metric") or {}
        detail = query.get("Expression") or (
            f"{metric.get('Namespace', '')}/{metric.get('MetricName', '')} {stat.get('Stat', '')}"
        )
        lines.append(f"Query {query.get('Id', '')}: {detail.strip()}")
    return lines


def render_alarm(kind: str, alarm: dict, history: list[dict], *, region: str) -> str:
    """Render an alarm as deterministic markdown.

    Dimensions and actions are sorted so the same alarm state always produces
    the same text; a byte-identical document keeps its content hash and cognee
    skips re-cognifying it.
    """
    name = alarm.get("AlarmName", "")
    lines = [
        f"CloudWatch {kind} alarm `{name}` in region {region}.",
        "",
        f"- State: {alarm.get('StateValue', '')}",
    ]
    if alarm.get("StateReason"):
        lines.append(f"- State reason: {alarm['StateReason']}")
    if alarm.get("StateUpdatedTimestamp"):
        lines.append(f"- State changed: {_iso(alarm['StateUpdatedTimestamp'])}")
    if alarm.get("AlarmConfigurationUpdatedTimestamp"):
        lines.append(
            f"- Configuration updated: {_iso(alarm['AlarmConfigurationUpdatedTimestamp'])}"
        )
    lines.append(f"- Actions enabled: {'yes' if alarm.get('ActionsEnabled') else 'no'}")

    if alarm.get("AlarmDescription"):
        lines += ["", "## Description", alarm["AlarmDescription"].strip()]

    if kind == "composite":
        lines += ["", "## Rule", alarm.get("AlarmRule", "")]
    else:
        lines += ["", "## Condition"]
        lines += _bullets(_metric_lines(alarm))
        lines += ["", "## Dimensions"]
        lines += _bullets(
            f"{d.get('Name')} = {d.get('Value', '')}"
            for d in sorted(alarm.get("Dimensions") or [], key=lambda d: d.get("Name", ""))
        )

    lines += ["", "## Actions"]
    lines += _bullets(
        f"{label}: {arn}"
        for label, key in (
            ("On ALARM", "AlarmActions"),
            ("On OK", "OKActions"),
            ("On INSUFFICIENT_DATA", "InsufficientDataActions"),
        )
        for arn in sorted(alarm.get(key) or [])
    )

    if history:
        lines += ["", "## Recent state changes"]
        lines += _bullets(
            f"{_iso(item.get('Timestamp'))} {item.get('HistorySummary', '')}" for item in history
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Sync (pure given clients + a state dict — unit-testable)
# ---------------------------------------------------------------------------
def sync_alarms(
    clients: dict[str, Any],
    state: dict,
    *,
    alarm_name_prefix: str | None = None,
    max_alarms: int = 1000,
    max_history_items: int = 10,
) -> Iterator[dict[str, Any]]:
    """Yield rows for new or changed alarms, then hard-delete markers.

    ``state["fingerprints"]`` maps each synced alarm ARN to its fingerprint
    from the previous run. It is only rewritten after the listing and every
    history call succeeded, so a failed run is retried in full next time.
    """
    known: dict[str, str] = dict(state.get("fingerprints") or {})
    active: dict[str, tuple[str, str, dict]] = {}

    for region, client in clients.items():
        for kind, alarm in _list_alarms(client, alarm_name_prefix):
            active[alarm["AlarmArn"]] = (region, kind, alarm)

    if len(active) > max_alarms:
        raise ValueError(
            f"Found {len(active)} CloudWatch alarms in scope, more than "
            f"max_alarms={max_alarms}. Narrow the scope with regions= or "
            "alarm_name_prefix=, or raise max_alarms."
        )

    fingerprints: dict[str, str] = {}
    changed = 0
    for alarm_arn in sorted(active):
        region, kind, alarm = active[alarm_arn]
        fingerprint = _fingerprint(alarm)
        fingerprints[alarm_arn] = fingerprint
        if known.get(alarm_arn) == fingerprint:
            continue
        name = alarm.get("AlarmName", "")
        history = _recent_state_changes(clients[region], name, max_history_items)
        yield {
            "id": alarm_arn,
            "title": f"CloudWatch alarm {name} ({region})",
            "url": _console_url(region, name),
            "content": render_alarm(kind, alarm, history, region=region),
            "region": region,
            "alarm_name": name,
            "state": alarm.get("StateValue", ""),
            "_deleted": False,
        }
        changed += 1

    gone = set(known) - set(active)
    if not active and known:
        # An empty listing is far more often a wrong region or a missing
        # permission than every alarm being deleted at once. Keep everything
        # until a listing shows alarms again.
        logger.warning(
            "CloudWatch: listing returned no alarms; keeping %d previously synced alarm(s).",
            len(known),
        )
        fingerprints = known
        gone = set()

    for alarm_arn in sorted(gone):
        yield {"id": alarm_arn, "_deleted": True}

    state["fingerprints"] = fingerprints
    logger.info("CloudWatch: %d changed alarm(s), %d deletion(s).", changed, len(gone))


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def cloudwatch_alarms_source(
    *,
    regions: list[str] | None = None,
    alarm_name_prefix: str | None = None,
    max_alarms: int = 1000,
    max_history_items: int = 10,
    profile_name: str | None = None,
    resource_name: str = DEFAULT_RESOURCE_NAME,
    clients: dict[str, Any] | None = None,
):
    """Return a ``dlt`` resource that yields CloudWatch alarms for ``remember``.

    Args:
        regions: AWS regions to sync. Defaults to the session's configured
            region (``AWS_REGION`` / profile ``region``).
        alarm_name_prefix: Only sync alarms whose name starts with this.
        max_alarms: Abort before fetching any history if more alarms than this
            are in scope.
        max_history_items: State changes kept per alarm (0 disables history).
        profile_name: Named AWS profile to use instead of the default chain.
        resource_name: dlt table name. Use a distinct name per account when
            several accounts sync into the same dataset.
        clients: ``{region: client}`` mapping of pre-built CloudWatch clients,
            mainly for tests. Overrides ``regions``/``profile_name``.

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
            "The CloudWatch Alarms connector needs a cognee build with table-scoped dlt "
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
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def cloudwatch_alarms():
        resolved_clients = clients or {
            region: _make_client(region, profile_name) for region in region_list
        }
        yield from sync_alarms(
            resolved_clients,
            dlt.current.resource_state(),
            alarm_name_prefix=alarm_name_prefix,
            max_alarms=max_alarms,
            max_history_items=max_history_items,
        )

    resource = cloudwatch_alarms()
    setattr(resource, DOCUMENT_SOURCE_ATTR, CLOUDWATCH_ALARMS_SOURCE_NAME)
    setattr(resource, dlt_utils.PIPELINE_SCOPE_ATTR, resource_name)
    return resource
