"""AWS CloudTrail connector for cognee — a ``dlt`` source that turns account changes into memory.

Each write-type management event (who changed what, when, and whether it
failed) becomes one markdown document, ingested through cognee's document
path so it goes through normal cognify::

    import cognee
    from cognee_community_connector_cloudtrail import cloudtrail_source

    await cognee.remember(
        cloudtrail_source(regions=["ap-south-1"]),
        dataset_name="aws_audit",
        primary_key="id",
        write_disposition="merge",   # REQUIRED for incremental sync
    )

Design
------
* **Auth** — the standard boto3 credential chain. The only call made is the
  read-only ``LookupEvents``, on the free 90-day event history. No trail has
  to be configured.
* **Scope** — write-type management events only. Read events would dominate
  the graph and add little to "who changed this?". Data events are not part
  of the event history at all.
* **Primary key** — ``cloudtrail:<region>:<EventId>``.
* **Incremental sync** — a per-region time cursor in dlt resource state.
  Each run looks up events from the previous cursor, minus a short overlap
  because CloudTrail can deliver events a few minutes late. Event ids already
  synced are skipped, so the overlap never duplicates a document.
* **Retention, not deletion** — CloudTrail never deletes an event; it ages
  out of the 90-day history. Synced events older than ``retention_days`` are
  emitted with the ``_deleted`` hard-delete marker, so cognee forgets them on
  the same schedule.
* **Rate limit** — ``LookupEvents`` allows two requests per second per
  account per region. Calls are spaced at least half a second apart.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

logger = get_logger("cloudtrail_connector")

CLOUDTRAIL_SOURCE_NAME = "cloudtrail"
DEFAULT_RESOURCE_NAME = "cloudtrail_events"

# CloudTrail keeps management events in the event history for 90 days.
_HISTORY_DAYS = 90

# Events usually reach the event history within minutes. Re-reading this
# window on every run catches events delivered after the previous run ended.
_DELIVERY_OVERLAP = timedelta(minutes=15)

# LookupEvents is limited to two requests per second per account per region.
_MIN_SECONDS_BETWEEN_CALLS = 0.5

_EXTRA_HINT = (
    "The CloudTrail connector requires boto3 and dlt: "
    "pip install cognee-community-connector-cloudtrail"
)


def _utcnow() -> datetime:
    return datetime.now(UTC)


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
        "cloudtrail",
        region_name=region,
        config=Config(retries={"mode": "adaptive", "max_attempts": 10}),
    )


def _default_region(profile_name: str | None) -> str | None:
    try:
        import boto3
    except ImportError as exc:
        raise ImportError(_EXTRA_HINT) from exc
    return boto3.Session(profile_name=profile_name).region_name


class _Pacer:
    """Space calls at least ``interval`` seconds apart (first call is free)."""

    def __init__(self, sleep: Callable[[float], None], interval: float):
        self._sleep = sleep
        self._interval = interval
        self._first = True

    def wait(self) -> None:
        if self._first:
            self._first = False
            return
        self._sleep(self._interval)


def _lookup_events(
    client: Any,
    *,
    start: datetime,
    end: datetime,
    event_names: list[str] | None,
    pacer: _Pacer,
) -> Iterator[dict]:
    """Yield events in ``[start, end]``.

    ``LookupEvents`` accepts a single lookup attribute, so either the server
    filters on ``ReadOnly=false``, or it filters on each event name and the
    read-only check happens in ``sync_events``.
    """
    if event_names:
        attribute_sets = [
            [{"AttributeKey": "EventName", "AttributeValue": name}] for name in event_names
        ]
    else:
        attribute_sets = [[{"AttributeKey": "ReadOnly", "AttributeValue": "false"}]]

    for attributes in attribute_sets:
        kwargs: dict[str, Any] = {
            "LookupAttributes": attributes,
            "StartTime": start,
            "EndTime": end,
            "MaxResults": 50,
        }
        while True:
            pacer.wait()
            response = client.lookup_events(**kwargs)
            yield from response.get("Events") or []
            token = response.get("NextToken")
            if not token:
                break
            kwargs["NextToken"] = token


# ---------------------------------------------------------------------------
# Rendering (pure)
# ---------------------------------------------------------------------------
def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value) if value else ""


def _parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _bullets(items: Iterable[str]) -> list[str]:
    lines = [f"- {item}" for item in items]
    return lines or ["- (none)"]


def _detail(event: dict) -> dict:
    try:
        return json.loads(event.get("CloudTrailEvent") or "{}")
    except ValueError:
        return {}


def _actor(event: dict, detail: dict) -> str:
    identity = detail.get("userIdentity") or {}
    issuer = ((identity.get("sessionContext") or {}).get("sessionIssuer") or {}).get("arn")
    actor = identity.get("arn") or issuer or event.get("Username") or "unknown"
    if identity.get("type"):
        actor += f" ({identity['type']})"
    return actor


def render_event(event: dict, *, region: str, include_request_parameters: bool = False) -> str:
    """Render one CloudTrail event as deterministic markdown.

    The access key id is never rendered. Request parameters are opt-in: they
    describe what was changed, but for some APIs they carry values a team
    would rather not copy into memory.
    """
    detail = _detail(event)
    name = event.get("EventName") or detail.get("eventName", "")
    source = event.get("EventSource") or detail.get("eventSource", "")

    lines = [
        f"CloudTrail event `{name}` from {source} in region {region}.",
        "",
        f"- Time: {_iso(event.get('EventTime'))}",
        f"- Actor: {_actor(event, detail)}",
    ]
    if detail.get("sourceIPAddress"):
        lines.append(f"- Source IP: {detail['sourceIPAddress']}")
    if detail.get("userAgent"):
        lines.append(f"- User agent: {detail['userAgent']}")
    if detail.get("errorCode"):
        lines.append(f"- Result: failed with {detail['errorCode']}")
        if detail.get("errorMessage"):
            lines.append(f"- Error message: {detail['errorMessage']}")
    else:
        lines.append("- Result: succeeded")

    lines += ["", "## Resources"]
    lines += _bullets(
        f"{r.get('ResourceName', '')} ({r.get('ResourceType', 'unknown type')})"
        for r in sorted(
            event.get("Resources") or [],
            key=lambda r: (r.get("ResourceType", ""), r.get("ResourceName", "")),
        )
    )

    if include_request_parameters and detail.get("requestParameters"):
        lines += [
            "",
            "## Request parameters",
            "```json",
            json.dumps(detail["requestParameters"], indent=2, sort_keys=True, default=str),
            "```",
        ]

    return "\n".join(lines)


def _event_id(region: str, event: dict) -> str:
    return f"cloudtrail:{region}:{event['EventId']}"


def _console_url(region: str, event: dict) -> str:
    return (
        f"https://{region}.console.aws.amazon.com/cloudtrailv2/home?region={region}"
        f"#/events/{event['EventId']}"
    )


# ---------------------------------------------------------------------------
# Sync (pure given clients + a state dict — unit-testable)
# ---------------------------------------------------------------------------
def sync_events(
    clients: dict[str, Any],
    state: dict,
    *,
    event_names: list[str] | None = None,
    lookback_days: int = 7,
    retention_days: int = _HISTORY_DAYS,
    max_events: int = 5000,
    include_request_parameters: bool = False,
    now: datetime | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[dict[str, Any]]:
    """Yield rows for new write events, then hard-delete markers for expired ones.

    ``state["cursors"]`` maps each region to the end of the previous run's
    window, and ``state["events"]`` maps each synced event id to its event
    time. Both are only rewritten after every lookup succeeded, so a failed
    run is retried in full next time.
    """
    if not 1 <= lookback_days <= _HISTORY_DAYS:
        raise ValueError(f"lookback_days must be between 1 and {_HISTORY_DAYS}.")
    if not 1 <= retention_days <= _HISTORY_DAYS:
        raise ValueError(f"retention_days must be between 1 and {_HISTORY_DAYS}.")

    now = now or _utcnow()
    cutoff = now - timedelta(days=retention_days)
    oldest_available = now - timedelta(days=_HISTORY_DAYS)
    cursors: dict[str, str] = dict(state.get("cursors") or {})
    known: dict[str, str] = dict(state.get("events") or {})
    pacer = _Pacer(sleep, _MIN_SECONDS_BETWEEN_CALLS)

    new_rows: list[tuple[str, dict[str, Any]]] = []
    seen: dict[str, str] = {}
    for region, client in clients.items():
        if region in cursors:
            start = _parse(cursors[region]) - _DELIVERY_OVERLAP
        else:
            start = now - timedelta(days=lookback_days)
        start = max(start, oldest_available, cutoff)

        for event in _lookup_events(
            client, start=start, end=now, event_names=event_names, pacer=pacer
        ):
            if str(event.get("ReadOnly", "false")).lower() == "true":
                continue
            event_id = _event_id(region, event)
            if event_id in known or event_id in seen:
                continue
            event_time = _iso(event.get("EventTime"))
            if event_time and _parse(event_time) < cutoff:
                continue
            seen[event_id] = event_time
            if len(seen) > max_events:
                raise ValueError(
                    f"Found more than max_events={max_events} new CloudTrail events. "
                    "Narrow the scope with lookback_days=, event_names= or regions=, "
                    "or raise max_events."
                )
            name = event.get("EventName", "")
            new_rows.append(
                (
                    event_time,
                    {
                        "id": event_id,
                        "title": f"CloudTrail {name} by {event.get('Username') or 'unknown'} "
                        f"({region})",
                        "url": _console_url(region, event),
                        "content": render_event(
                            event,
                            region=region,
                            include_request_parameters=include_request_parameters,
                        ),
                        "region": region,
                        "event_name": name,
                        "event_time": event_time,
                        "_deleted": False,
                    },
                )
            )
        cursors[region] = now.isoformat()

    for _, row in sorted(new_rows, key=lambda item: (item[0], item[1]["id"])):
        yield row

    expired = sorted(
        event_id for event_id, event_time in known.items() if _parse(event_time) < cutoff
    )
    for event_id in expired:
        yield {"id": event_id, "_deleted": True}

    for event_id in expired:
        del known[event_id]
    known.update(seen)
    state["cursors"] = cursors
    state["events"] = known
    logger.info("CloudTrail: %d new event(s), %d expired.", len(seen), len(expired))


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def cloudtrail_source(
    *,
    regions: list[str] | None = None,
    event_names: list[str] | None = None,
    lookback_days: int = 7,
    retention_days: int = _HISTORY_DAYS,
    max_events: int = 5000,
    include_request_parameters: bool = False,
    profile_name: str | None = None,
    resource_name: str = DEFAULT_RESOURCE_NAME,
    clients: dict[str, Any] | None = None,
):
    """Return a ``dlt`` resource that yields CloudTrail write events for ``remember``.

    Args:
        regions: AWS regions to sync. Defaults to the session's configured
            region (``AWS_REGION`` / profile ``region``). Global services such
            as IAM log their events in ``us-east-1``.
        event_names: Only sync these event names (for example
            ``["AuthorizeSecurityGroupIngress"]``). Defaults to every write event.
        lookback_days: How far back the first run reads (1 to 90).
        retention_days: Events older than this are forgotten (1 to 90).
        max_events: Abort before emitting anything if a run finds more new
            events than this.
        include_request_parameters: Also render each call's request parameters.
        profile_name: Named AWS profile to use instead of the default chain.
        resource_name: dlt table name. Use a distinct name per account when
            several accounts sync into the same dataset.
        clients: ``{region: client}`` mapping of pre-built CloudTrail clients,
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
            "The CloudTrail connector needs a cognee build with table-scoped dlt document "
            "cleanup (cognee>=1.6). Upgrade cognee."
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
    def cloudtrail_events():
        resolved_clients = clients or {
            region: _make_client(region, profile_name) for region in region_list
        }
        yield from sync_events(
            resolved_clients,
            dlt.current.resource_state(),
            event_names=event_names,
            lookback_days=lookback_days,
            retention_days=retention_days,
            max_events=max_events,
            include_request_parameters=include_request_parameters,
        )

    resource = cloudtrail_events()
    setattr(resource, DOCUMENT_SOURCE_ATTR, CLOUDTRAIL_SOURCE_NAME)
    setattr(resource, dlt_utils.PIPELINE_SCOPE_ATTR, resource_name)
    return resource
