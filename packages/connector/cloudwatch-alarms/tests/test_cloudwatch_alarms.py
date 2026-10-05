"""Tests for the CloudWatch Alarms connector. No real AWS account is used.

* Rendering tests are pure.
* Sync tests run against moto's in-memory CloudWatch (``mock_aws``) for the
  incremental fingerprint and forget-on-delete. moto does not implement
  ``DescribeAlarmHistory``, so those tests pass ``max_history_items=0`` and a
  tiny fake client covers the history path.
* The last test drives the source through cognee's own ``ingest_dlt_source``
  into a temporary sqlite destination, the same path ``cognee.remember`` uses.
"""

import importlib
import shutil
import tempfile
from datetime import UTC, datetime
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

from cognee_community_connector_cloudwatch_alarms.cloudwatch_alarms import (
    CLOUDWATCH_ALARMS_SOURCE_NAME,
    cloudwatch_alarms_source,
    render_alarm,
    sync_alarms,
)

REGION = "us-east-1"


@pytest.fixture
def cw():
    with mock_aws():
        yield boto3.client("cloudwatch", region_name=REGION)


def _put(client, name, threshold=100.0, description="Orders queue is backing up"):
    client.put_metric_alarm(
        AlarmName=name,
        AlarmDescription=description,
        Namespace="AWS/SQS",
        MetricName="ApproximateNumberOfMessagesVisible",
        Dimensions=[{"Name": "QueueName", "Value": f"{name}-q"}],
        Statistic="Average",
        Period=300,
        EvaluationPeriods=2,
        Threshold=threshold,
        ComparisonOperator="GreaterThanThreshold",
        AlarmActions=["arn:aws:sns:us-east-1:123456789012:oncall"],
    )


def _arn(name):
    return f"arn:aws:cloudwatch:{REGION}:123456789012:alarm:{name}"


def _sync(client, state, **kwargs):
    kwargs.setdefault("max_history_items", 0)
    return list(sync_alarms({REGION: client}, state, **kwargs))


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def _alarm(**overrides):
    alarm = {
        "AlarmName": "orders-depth",
        "AlarmArn": _arn("orders-depth"),
        "AlarmDescription": "Orders queue is backing up",
        "StateValue": "ALARM",
        "StateReason": "Threshold Crossed: 1 datapoint [250.0] was greater than 100.0",
        "StateUpdatedTimestamp": datetime(2026, 2, 3, tzinfo=UTC),
        "ActionsEnabled": True,
        "Namespace": "AWS/SQS",
        "MetricName": "ApproximateNumberOfMessagesVisible",
        "Statistic": "Average",
        "Period": 300,
        "EvaluationPeriods": 2,
        "Threshold": 100.0,
        "ComparisonOperator": "GreaterThanThreshold",
        "Dimensions": [
            {"Name": "QueueName", "Value": "orders"},
            {"Name": "Env", "Value": "prod"},
        ],
        "AlarmActions": ["arn:aws:sns:us-east-1:1:z", "arn:aws:sns:us-east-1:1:a"],
    }
    alarm.update(overrides)
    return alarm


def test_render_metric_alarm_is_sorted_and_deterministic():
    alarm = _alarm()
    text = render_alarm("metric", alarm, [], region=REGION)

    reordered = _alarm(
        Dimensions=list(reversed(alarm["Dimensions"])),
        AlarmActions=list(reversed(alarm["AlarmActions"])),
    )
    assert text == render_alarm("metric", reordered, [], region=REGION)
    assert text.index("Env = prod") < text.index("QueueName = orders")
    assert text.index("sns:us-east-1:1:a") < text.index("sns:us-east-1:1:z")
    assert "Metric: AWS/SQS/ApproximateNumberOfMessagesVisible" in text
    assert "Condition: GreaterThanThreshold 100.0" in text
    assert "- State: ALARM" in text


def test_render_composite_alarm_shows_rule():
    alarm = {
        "AlarmName": "checkout-down",
        "StateValue": "OK",
        "AlarmRule": 'ALARM("orders-depth") OR ALARM("payments-5xx")',
    }
    text = render_alarm("composite", alarm, [], region=REGION)

    assert "CloudWatch composite alarm `checkout-down`" in text
    assert '## Rule\nALARM("orders-depth") OR ALARM("payments-5xx")' in text
    assert "## Dimensions" not in text


def test_render_alarm_lists_recent_state_changes():
    history = [
        {
            "Timestamp": datetime(2026, 2, 3, tzinfo=UTC),
            "HistorySummary": "Alarm updated from OK to ALARM",
        }
    ]
    text = render_alarm("metric", _alarm(), history, region=REGION)

    assert "## Recent state changes" in text
    assert "2026-02-03T00:00:00+00:00 Alarm updated from OK to ALARM" in text


# ---------------------------------------------------------------------------
# Sync against moto
# ---------------------------------------------------------------------------
def test_first_sync_emits_every_alarm(cw):
    _put(cw, "orders-depth")
    _put(cw, "payments-5xx")

    state = {}
    rows = _sync(cw, state)

    assert {row["id"] for row in rows} == {_arn("orders-depth"), _arn("payments-5xx")}
    row = next(r for r in rows if r["alarm_name"] == "orders-depth")
    assert row["_deleted"] is False
    assert row["title"] == f"CloudWatch alarm orders-depth ({REGION})"
    assert "QueueName = orders-depth-q" in row["content"]
    assert set(state["fingerprints"]) == {row["id"] for row in rows}


def test_unchanged_account_emits_nothing(cw):
    _put(cw, "orders-depth")
    state = {}
    _sync(cw, state)

    assert _sync(cw, state) == []


def test_state_change_resyncs_only_that_alarm(cw):
    _put(cw, "orders-depth")
    _put(cw, "payments-5xx")
    state = {}
    _sync(cw, state)

    cw.set_alarm_state(AlarmName="orders-depth", StateValue="ALARM", StateReason="Queue backed up")
    rows = _sync(cw, state)

    assert [row["id"] for row in rows] == [_arn("orders-depth")]
    assert rows[0]["state"] == "ALARM"
    assert "State reason: Queue backed up" in rows[0]["content"]


def test_threshold_change_resyncs_alarm(cw):
    _put(cw, "orders-depth")
    state = {}
    _sync(cw, state)

    _put(cw, "orders-depth", threshold=500.0)
    rows = _sync(cw, state)

    assert [row["id"] for row in rows] == [_arn("orders-depth")]
    assert "Condition: GreaterThanThreshold 500.0" in rows[0]["content"]


def test_deleted_alarm_becomes_tombstone(cw):
    _put(cw, "orders-depth")
    _put(cw, "payments-5xx")
    state = {}
    _sync(cw, state)

    cw.delete_alarms(AlarmNames=["orders-depth"])
    rows = _sync(cw, state)

    assert rows == [{"id": _arn("orders-depth"), "_deleted": True}]
    assert set(state["fingerprints"]) == {_arn("payments-5xx")}


def test_prefix_filter_limits_scope(cw):
    _put(cw, "prod-orders")
    _put(cw, "dev-orders")

    rows = _sync(cw, {}, alarm_name_prefix="prod-")

    assert [row["id"] for row in rows] == [_arn("prod-orders")]


def test_max_alarms_guard_aborts(cw):
    _put(cw, "a")
    _put(cw, "b")

    with pytest.raises(ValueError, match="max_alarms=1"):
        _sync(cw, {}, max_alarms=1)


def test_failed_sync_leaves_state_untouched(cw):
    _put(cw, "orders-depth")
    state = {"fingerprints": {"arn:old": "x"}}

    with pytest.raises(ValueError):
        _sync(cw, state, max_alarms=0)

    assert state == {"fingerprints": {"arn:old": "x"}}


def test_empty_listing_keeps_previous_alarms(cw):
    state = {"fingerprints": {_arn("orders-depth"): "x"}}

    assert _sync(cw, state) == []
    assert state["fingerprints"] == {_arn("orders-depth"): "x"}


# ---------------------------------------------------------------------------
# History with a fake client
# ---------------------------------------------------------------------------
class FakePaginator:
    def __init__(self, page):
        self._page = page

    def paginate(self, **kwargs):
        yield self._page


class FakeCloudWatch:
    def __init__(self, metric_alarms, history):
        self.metric_alarms = metric_alarms
        self.history = history
        self.history_calls = []

    def get_paginator(self, operation):
        assert operation == "describe_alarms"
        return FakePaginator({"MetricAlarms": self.metric_alarms, "CompositeAlarms": []})

    def describe_alarm_history(self, **kwargs):
        self.history_calls.append(kwargs)
        return {"AlarmHistoryItems": self.history}


def test_history_is_fetched_only_for_changed_alarms():
    history = [
        {
            "Timestamp": datetime(2026, 2, 3, tzinfo=UTC),
            "HistorySummary": "Alarm updated from OK to ALARM",
        }
    ]
    client = FakeCloudWatch([_alarm()], history)
    state = {}

    [row] = list(sync_alarms({REGION: client}, state, max_history_items=5))
    assert "Alarm updated from OK to ALARM" in row["content"]
    assert client.history_calls == [
        {
            "AlarmName": "orders-depth",
            "HistoryItemType": "StateUpdate",
            "ScanBy": "TimestampDescending",
            "MaxRecords": 5,
        }
    ]

    assert list(sync_alarms({REGION: client}, state, max_history_items=5)) == []
    assert len(client.history_calls) == 1


# ---------------------------------------------------------------------------
# Factory + cognee ingestion path
# ---------------------------------------------------------------------------
def test_source_declares_document_mode_and_pipeline_scope():
    from cognee.tasks.ingestion.dlt_utils import PIPELINE_SCOPE_ATTR, document_source_tag

    source = cloudwatch_alarms_source(clients={REGION: SimpleNamespace()}, resource_name="acct_a")

    assert document_source_tag(source) == CLOUDWATCH_ALARMS_SOURCE_NAME
    assert getattr(source, PIPELINE_SCOPE_ATTR) == "acct_a"


@pytest.mark.asyncio
async def test_cognee_ingestion_syncs_and_forgets(cw, tmp_path, monkeypatch, request):
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
    pipelines_dir = tempfile.mkdtemp(prefix="cwa")
    request.addfinalizer(lambda: shutil.rmtree(pipelines_dir, ignore_errors=True))
    pipeline_factory = dlt.pipeline
    monkeypatch.setattr(
        dlt,
        "pipeline",
        lambda **kw: pipeline_factory(pipelines_dir=pipelines_dir, **kw),
    )

    async def run():
        return await ingest.ingest_dlt_source(
            cloudwatch_alarms_source(clients={REGION: cw}, max_history_items=0),
            "aws_alarms",
            primary_key="id",
            write_disposition="merge",
            max_rows_per_table=0,
        )

    _put(cw, "orders-depth")
    _put(cw, "payments-5xx")
    first = await run()
    assert {row.primary_key_value for row in first} == {_arn("orders-depth"), _arn("payments-5xx")}

    unchanged = await run()
    assert {row.primary_key_value for row in unchanged} == {
        _arn("orders-depth"),
        _arn("payments-5xx"),
    }

    cw.delete_alarms(AlarmNames=["orders-depth"])
    after_delete = await run()
    assert {row.primary_key_value for row in after_delete} == {_arn("payments-5xx")}
