"""Tests for the CloudTrail connector. No real AWS account is used.

moto does not implement ``LookupEvents``, so sync tests run against a small
in-memory fake that honours the time window, the lookup attribute and
pagination. A botocore ``Stubber`` test checks that the requests the connector
sends are valid against the real CloudTrail API model. The last test drives
the source through cognee's own ``ingest_dlt_source`` into a temporary sqlite
destination, the same path ``cognee.remember`` uses.
"""

import importlib
import json
import shutil
import tempfile
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import boto3
import pytest
from botocore.stub import Stubber

from cognee_community_connector_cloudtrail import cloudtrail as cloudtrail_module
from cognee_community_connector_cloudtrail.cloudtrail import (
    CLOUDTRAIL_SOURCE_NAME,
    cloudtrail_source,
    render_event,
    sync_events,
)

REGION = "us-east-1"
NOW = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)


def _event(
    event_id,
    when,
    name="AuthorizeSecurityGroupIngress",
    read_only=False,
    error=None,
    username="alice",
):
    detail = {
        "eventName": name,
        "eventSource": "ec2.amazonaws.com",
        "sourceIPAddress": "203.0.113.7",
        "userAgent": "aws-cli/2.17",
        "userIdentity": {
            "type": "IAMUser",
            "arn": f"arn:aws:iam::123456789012:user/{username}",
            "accessKeyId": "AKIAEXAMPLESECRET",
        },
        "requestParameters": {"groupId": "sg-123", "ipPermissions": {"items": []}},
    }
    if error:
        detail["errorCode"] = error
        detail["errorMessage"] = "You are not authorized to perform this operation."
    return {
        "EventId": event_id,
        "EventName": name,
        "EventSource": "ec2.amazonaws.com",
        "EventTime": when,
        "ReadOnly": "true" if read_only else "false",
        "Username": username,
        "AccessKeyId": "AKIAEXAMPLESECRET",
        "Resources": [
            {"ResourceType": "AWS::EC2::SecurityGroup", "ResourceName": "sg-123"},
            {"ResourceType": "AWS::EC2::Instance", "ResourceName": "i-9"},
        ],
        "CloudTrailEvent": json.dumps(detail),
    }


class FakeCloudTrail:
    """Implements LookupEvents semantics the connector relies on."""

    def __init__(self, events, page_size=2):
        self.events = list(events)
        self.page_size = page_size
        self.calls = []

    def lookup_events(self, **kwargs):
        self.calls.append(kwargs)
        [attribute] = kwargs["LookupAttributes"]
        key, value = attribute["AttributeKey"], attribute["AttributeValue"]
        matches = [
            e
            for e in self.events
            if kwargs["StartTime"] <= e["EventTime"] <= kwargs["EndTime"]
            and (e["ReadOnly"] == value if key == "ReadOnly" else e["EventName"] == value)
        ]
        matches.sort(key=lambda e: e["EventTime"], reverse=True)
        offset = int(kwargs.get("NextToken") or 0)
        page = matches[offset : offset + self.page_size]
        response = {"Events": page}
        if offset + self.page_size < len(matches):
            response["NextToken"] = str(offset + self.page_size)
        return response


def _sync(client, state, now=NOW, **kwargs):
    kwargs.setdefault("sleep", lambda seconds: None)
    return list(sync_events({REGION: client}, state, now=now, **kwargs))


def _id(event_id):
    return f"cloudtrail:{REGION}:{event_id}"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def test_render_event_shows_actor_resources_and_result():
    text = render_event(_event("e1", NOW), region=REGION)

    assert "CloudTrail event `AuthorizeSecurityGroupIngress` from ec2.amazonaws.com" in text
    assert "- Actor: arn:aws:iam::123456789012:user/alice (IAMUser)" in text
    assert "- Source IP: 203.0.113.7" in text
    assert "- Result: succeeded" in text
    assert text.index("i-9 (AWS::EC2::Instance)") < text.index("sg-123 (AWS::EC2::SecurityGroup)")


def test_render_event_reports_failures():
    text = render_event(_event("e1", NOW, error="UnauthorizedOperation"), region=REGION)

    assert "- Result: failed with UnauthorizedOperation" in text
    assert "You are not authorized" in text


def test_render_event_never_includes_access_key():
    text = render_event(_event("e1", NOW), region=REGION, include_request_parameters=True)

    assert "AKIAEXAMPLESECRET" not in text


def test_request_parameters_are_opt_in():
    assert "## Request parameters" not in render_event(_event("e1", NOW), region=REGION)

    text = render_event(_event("e1", NOW), region=REGION, include_request_parameters=True)
    assert '"groupId": "sg-123"' in text


def test_render_event_is_deterministic():
    event = _event("e1", NOW)
    reordered = {**event, "Resources": list(reversed(event["Resources"]))}

    assert render_event(event, region=REGION) == render_event(reordered, region=REGION)


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
def test_first_sync_reads_write_events_in_the_lookback_window():
    client = FakeCloudTrail(
        [
            _event("recent", NOW - timedelta(hours=1)),
            _event("older", NOW - timedelta(days=3)),
            _event("read", NOW - timedelta(hours=2), name="DescribeInstances", read_only=True),
            _event("too-old", NOW - timedelta(days=10)),
        ]
    )
    state = {}
    rows = _sync(client, state, lookback_days=7)

    assert [row["id"] for row in rows] == [_id("older"), _id("recent")]
    assert rows[1]["title"] == f"CloudTrail AuthorizeSecurityGroupIngress by alice ({REGION})"
    assert rows[1]["_deleted"] is False
    assert state["cursors"] == {REGION: NOW.isoformat()}
    assert set(state["events"]) == {_id("older"), _id("recent")}


def test_second_run_reads_only_new_events_and_skips_the_overlap():
    client = FakeCloudTrail([_event("first", NOW - timedelta(minutes=5))])
    state = {}
    _sync(client, state)

    later = NOW + timedelta(hours=1)
    client.events.append(_event("second", later - timedelta(minutes=10)))
    rows = _sync(client, state, now=later)

    assert [row["id"] for row in rows] == [_id("second")]
    assert client.calls[-1]["StartTime"] == NOW - timedelta(minutes=15)


def test_unchanged_history_emits_nothing():
    client = FakeCloudTrail([_event("first", NOW - timedelta(hours=1))])
    state = {}
    _sync(client, state)

    assert _sync(client, state, now=NOW + timedelta(minutes=30)) == []


def test_events_past_retention_become_tombstones():
    client = FakeCloudTrail(
        [_event("old", NOW - timedelta(days=5)), _event("new", NOW - timedelta(hours=1))]
    )
    state = {}
    _sync(client, state, retention_days=30)

    rows = _sync(client, state, now=NOW + timedelta(days=26), retention_days=30)

    assert rows == [{"id": _id("old"), "_deleted": True}]
    assert set(state["events"]) == {_id("new")}


def test_event_name_filter_drops_read_only_events():
    client = FakeCloudTrail(
        [
            _event("sg", NOW - timedelta(hours=1)),
            _event("other", NOW - timedelta(hours=1), name="RunInstances"),
        ]
    )
    rows = _sync(client, {}, event_names=["AuthorizeSecurityGroupIngress"])

    assert [row["id"] for row in rows] == [_id("sg")]
    assert client.calls[0]["LookupAttributes"] == [
        {"AttributeKey": "EventName", "AttributeValue": "AuthorizeSecurityGroupIngress"}
    ]


def test_calls_are_paced_under_the_rate_limit():
    client = FakeCloudTrail(
        [_event(f"e{i}", NOW - timedelta(minutes=i)) for i in range(5)], page_size=2
    )
    pauses = []
    rows = _sync(client, {}, sleep=pauses.append)

    assert len(rows) == 5
    assert len(client.calls) == 3
    assert pauses == [0.5, 0.5]


def test_max_events_guard_aborts_and_leaves_state_untouched():
    client = FakeCloudTrail([_event(f"e{i}", NOW - timedelta(minutes=i)) for i in range(3)])
    previous = (NOW - timedelta(hours=1)).isoformat()
    state = {"cursors": {REGION: previous}, "events": {}}

    with pytest.raises(ValueError, match="max_events=2"):
        _sync(client, state, max_events=2)

    assert state == {"cursors": {REGION: previous}, "events": {}}


@pytest.mark.parametrize("field", ["lookback_days", "retention_days"])
def test_windows_are_limited_to_the_90_day_history(field):
    with pytest.raises(ValueError, match=field):
        _sync(FakeCloudTrail([]), {}, **{field: 91})


def test_requests_match_the_real_cloudtrail_api_model():
    client = boto3.client("cloudtrail", region_name=REGION)
    start = NOW - timedelta(days=7)
    with Stubber(client) as stub:
        stub.add_response(
            "lookup_events",
            {"Events": [], "NextToken": "page-2"},
            {
                "LookupAttributes": [{"AttributeKey": "ReadOnly", "AttributeValue": "false"}],
                "StartTime": start,
                "EndTime": NOW,
                "MaxResults": 50,
            },
        )
        stub.add_response(
            "lookup_events",
            {"Events": []},
            {
                "LookupAttributes": [{"AttributeKey": "ReadOnly", "AttributeValue": "false"}],
                "StartTime": start,
                "EndTime": NOW,
                "MaxResults": 50,
                "NextToken": "page-2",
            },
        )

        assert _sync(client, {}) == []
        stub.assert_no_pending_responses()


# ---------------------------------------------------------------------------
# Factory + cognee ingestion path
# ---------------------------------------------------------------------------
def test_source_declares_document_mode_and_pipeline_scope():
    from cognee.tasks.ingestion.dlt_utils import PIPELINE_SCOPE_ATTR, document_source_tag

    source = cloudtrail_source(clients={REGION: SimpleNamespace()}, resource_name="acct_a")

    assert document_source_tag(source) == CLOUDTRAIL_SOURCE_NAME
    assert getattr(source, PIPELINE_SCOPE_ATTR) == "acct_a"


@pytest.mark.asyncio
async def test_cognee_ingestion_syncs_and_forgets(tmp_path, monkeypatch, request):
    dlt = pytest.importorskip("dlt")
    ingest = importlib.import_module("cognee.tasks.ingestion.ingest_dlt_source")

    config = SimpleNamespace(
        db_provider="sqlite",
        db_path=str(tmp_path),
        db_host="",
        db_port=0,
        db_username="",
        db_password="",
    )
    monkeypatch.setattr(ingest, "get_relational_config", lambda: config)
    monkeypatch.setattr(
        ingest,
        "get_dlt_destination",
        lambda dlt_db_name: dlt.destinations.sqlalchemy(
            credentials={"database": str(tmp_path / dlt_db_name), "drivername": "sqlite"},
        ),
    )
    # dlt nests deep paths under pipelines_dir; a short root keeps them under
    # the Windows MAX_PATH limit.
    pipelines_dir = tempfile.mkdtemp(prefix="ct")
    request.addfinalizer(lambda: shutil.rmtree(pipelines_dir, ignore_errors=True))
    pipeline_factory = dlt.pipeline
    monkeypatch.setattr(
        dlt,
        "pipeline",
        lambda **kw: pipeline_factory(pipelines_dir=pipelines_dir, **kw),
    )
    monkeypatch.setattr(cloudtrail_module.time, "sleep", lambda seconds: None)

    clock = {"now": NOW}
    monkeypatch.setattr(cloudtrail_module, "_utcnow", lambda: clock["now"])
    client = FakeCloudTrail(
        [_event("old", NOW - timedelta(days=5)), _event("new", NOW - timedelta(hours=1))]
    )

    async def run():
        return await ingest.ingest_dlt_source(
            cloudtrail_source(clients={REGION: client}, retention_days=30),
            "aws_audit",
            primary_key="id",
            write_disposition="merge",
            max_rows_per_table=0,
        )

    first = await run()
    assert {row.primary_key_value for row in first} == {_id("old"), _id("new")}

    clock["now"] = NOW + timedelta(hours=1)
    unchanged = await run()
    assert {row.primary_key_value for row in unchanged} == {_id("old"), _id("new")}

    clock["now"] = NOW + timedelta(days=26)
    after_expiry = await run()
    assert {row.primary_key_value for row in after_expiry} == {_id("new")}
