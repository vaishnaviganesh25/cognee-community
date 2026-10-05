"""Tests for the CloudFormation connector. No real AWS account is used.

* Rendering tests are pure.
* Sync tests run against moto's in-memory CloudFormation (``mock_aws``) for the
  incremental cursor and forget-on-delete, plus a tiny fake client for edge
  cases moto can't produce (failed events, an empty listing).
* The last test drives the source through cognee's own ``ingest_dlt_source``
  into a temporary sqlite destination, the same path ``cognee.remember`` uses.
"""

import importlib
import json
import shutil
import tempfile
from datetime import UTC, datetime
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

from cognee_community_connector_cloudformation.cloudformation import (
    CLOUDFORMATION_SOURCE_NAME,
    cloudformation_source,
    render_stack,
    sync_stacks,
)

REGION = "us-east-1"


def _template(name, description="Orders queue"):
    return json.dumps(
        {
            "Description": description,
            "Parameters": {"QueueName": {"Type": "String"}},
            "Resources": {
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": {"Ref": "QueueName"}},
                }
            },
            # Export names must be unique per region, so derive one per stack.
            "Outputs": {"QueueUrl": {"Value": {"Ref": "Queue"}, "Export": {"Name": f"{name}-url"}}},
        }
    )


def _create(client, name, queue=None, description="Orders queue"):
    return client.create_stack(
        StackName=name,
        TemplateBody=_template(name, description),
        Parameters=[{"ParameterKey": "QueueName", "ParameterValue": queue or f"{name}-q"}],
        Tags=[{"Key": "team", "Value": "payments"}],
    )["StackId"]


@pytest.fixture
def cfn():
    with mock_aws():
        yield boto3.client("cloudformation", region_name=REGION)


def _sync(client, state, **kwargs):
    return list(sync_stacks({REGION: client}, state, **kwargs))


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def _stack(**overrides):
    stack = {
        "StackName": "orders",
        "StackStatus": "UPDATE_ROLLBACK_COMPLETE",
        "StackStatusReason": "Resource Queue failed",
        "CreationTime": datetime(2026, 1, 1, tzinfo=UTC),
        "Description": "Orders service",
        "Parameters": [
            {"ParameterKey": "Zeta", "ParameterValue": "z"},
            {"ParameterKey": "Alpha", "ParameterValue": "a", "ResolvedValue": "resolved-secret"},
        ],
        "Outputs": [{"OutputKey": "Url", "OutputValue": "https://x", "ExportName": "orders-url"}],
        "Tags": [{"Key": "team", "Value": "payments"}],
    }
    stack.update(overrides)
    return stack


def test_render_stack_is_sorted_and_deterministic():
    resources = [
        {"LogicalResourceId": "Topic", "ResourceType": "AWS::SNS::Topic", "ResourceStatus": "OK"},
        {"LogicalResourceId": "Queue", "ResourceType": "AWS::SQS::Queue", "ResourceStatus": "OK"},
    ]
    text = render_stack(_stack(), resources, [], region=REGION)

    assert text == render_stack(_stack(), list(reversed(resources)), [], region=REGION)
    assert text.index("Alpha = a") < text.index("Zeta = z")
    assert text.index("Queue (AWS::SQS::Queue)") < text.index("Topic (AWS::SNS::Topic)")
    assert "Url = https://x (exported as orders-url)" in text
    assert "team = payments" in text
    assert "Status reason: Resource Queue failed" in text


def test_render_stack_never_uses_resolved_parameter_values():
    text = render_stack(_stack(), [], [], region=REGION)
    assert "resolved-secret" not in text


def test_render_stack_can_hide_parameter_values():
    text = render_stack(_stack(), [], [], region=REGION, include_parameter_values=False)
    assert "- Alpha" in text
    assert "Alpha = a" not in text


# ---------------------------------------------------------------------------
# Sync against moto
# ---------------------------------------------------------------------------
def test_first_sync_emits_every_active_stack(cfn):
    orders = _create(cfn, "orders")
    billing = _create(cfn, "billing")

    state = {}
    rows = _sync(cfn, state)

    assert {row["id"] for row in rows} == {orders, billing}
    row = next(r for r in rows if r["id"] == orders)
    assert row["_deleted"] is False
    assert row["title"] == f"CloudFormation stack orders ({REGION})"
    assert "QueueName = orders-q" in row["content"]
    assert "Queue (AWS::SQS::Queue)" in row["content"]
    assert set(state["fingerprints"]) == {orders, billing}


def test_unchanged_account_emits_nothing(cfn):
    _create(cfn, "orders")
    state = {}
    _sync(cfn, state)

    assert _sync(cfn, state) == []


def test_only_updated_stack_is_resynced(cfn):
    orders = _create(cfn, "orders")
    _create(cfn, "billing")
    state = {}
    _sync(cfn, state)

    cfn.update_stack(
        StackName="orders",
        TemplateBody=_template("orders"),
        Parameters=[{"ParameterKey": "QueueName", "ParameterValue": "orders-q-v2"}],
    )
    rows = _sync(cfn, state)

    assert [row["id"] for row in rows] == [orders]
    assert "QueueName = orders-q-v2" in rows[0]["content"]


def test_deleted_stack_becomes_tombstone(cfn):
    orders = _create(cfn, "orders")
    billing = _create(cfn, "billing")
    state = {}
    _sync(cfn, state)

    cfn.delete_stack(StackName="orders")
    rows = _sync(cfn, state)

    assert rows == [{"id": orders, "_deleted": True}]
    assert set(state["fingerprints"]) == {billing}


def test_prefix_filter_limits_scope(cfn):
    prod = _create(cfn, "prod-orders")
    _create(cfn, "dev-orders")

    rows = _sync(cfn, {}, stack_name_prefix="prod-")

    assert [row["id"] for row in rows] == [prod]


def test_max_stacks_guard_aborts_before_describing(cfn):
    _create(cfn, "a")
    _create(cfn, "b")

    with pytest.raises(ValueError, match="max_stacks=1"):
        _sync(cfn, {}, max_stacks=1)


def test_failed_sync_leaves_state_untouched(cfn):
    _create(cfn, "orders")
    state = {"fingerprints": {"arn:old": "x"}}

    with pytest.raises(ValueError):
        _sync(cfn, state, max_stacks=0)

    assert state == {"fingerprints": {"arn:old": "x"}}


# ---------------------------------------------------------------------------
# Edge cases with a fake client
# ---------------------------------------------------------------------------
class FakePaginator:
    def __init__(self, items, key):
        self._items, self._key = items, key

    def paginate(self, **kwargs):
        yield {self._key: self._items}


class FakeCfn:
    def __init__(self, summaries, events=()):
        self.summaries = summaries
        self.events = list(events)

    def get_paginator(self, operation):
        return {
            "list_stacks": FakePaginator(self.summaries, "StackSummaries"),
            "list_stack_resources": FakePaginator([], "StackResourceSummaries"),
            "describe_stack_events": FakePaginator(self.events, "StackEvents"),
        }[operation]

    def describe_stacks(self, StackName):  # noqa: N803 - mirrors the boto3 signature
        summary = next(s for s in self.summaries if s["StackId"] == StackName)
        return {"Stacks": [summary]}


def _summary(stack_id, status="CREATE_COMPLETE"):
    return {
        "StackId": stack_id,
        "StackName": stack_id.split("/")[-1],
        "StackStatus": status,
        "CreationTime": datetime(2026, 1, 1, tzinfo=UTC),
    }


def test_failure_events_are_rendered():
    events = [
        {
            "LogicalResourceId": "Queue",
            "ResourceType": "AWS::SQS::Queue",
            "ResourceStatus": "UPDATE_FAILED",
            "ResourceStatusReason": "Queue name already exists",
            "Timestamp": datetime(2026, 2, 1, tzinfo=UTC),
        },
        {"LogicalResourceId": "Queue", "ResourceStatus": "UPDATE_IN_PROGRESS"},
    ]
    client = FakeCfn([_summary("stack/orders")], events)

    [row] = list(sync_stacks({REGION: client}, {}))

    assert "## Recent failures" in row["content"]
    assert "UPDATE_FAILED: Queue name already exists" in row["content"]
    assert "UPDATE_IN_PROGRESS" not in row["content"]


def test_empty_listing_only_forgets_confirmed_deletions():
    state = {"fingerprints": {"stack/a": "x", "stack/b": "y"}}
    client = FakeCfn([_summary("stack/a", status="DELETE_COMPLETE")])

    rows = list(sync_stacks({REGION: client}, state))

    assert rows == [{"id": "stack/a", "_deleted": True}]
    assert state["fingerprints"] == {"stack/b": "y"}


# ---------------------------------------------------------------------------
# Factory + cognee ingestion path
# ---------------------------------------------------------------------------
def test_source_declares_document_mode_and_pipeline_scope():
    from cognee.tasks.ingestion.dlt_utils import PIPELINE_SCOPE_ATTR, document_source_tag

    source = cloudformation_source(clients={REGION: SimpleNamespace()}, resource_name="acct_a")

    assert document_source_tag(source) == CLOUDFORMATION_SOURCE_NAME
    assert getattr(source, PIPELINE_SCOPE_ATTR) == "acct_a"


@pytest.mark.asyncio
async def test_cognee_ingestion_syncs_and_forgets(cfn, tmp_path, monkeypatch, request):
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
    pipelines_dir = tempfile.mkdtemp(prefix="cfn")
    request.addfinalizer(lambda: shutil.rmtree(pipelines_dir, ignore_errors=True))
    pipeline_factory = dlt.pipeline
    monkeypatch.setattr(
        dlt,
        "pipeline",
        lambda **kw: pipeline_factory(pipelines_dir=pipelines_dir, **kw),
    )

    async def run():
        return await ingest.ingest_dlt_source(
            cloudformation_source(clients={REGION: cfn}),
            "aws_stacks",
            primary_key="id",
            write_disposition="merge",
            max_rows_per_table=0,
        )

    orders = _create(cfn, "orders")
    billing = _create(cfn, "billing")
    first = await run()
    assert {row.primary_key_value for row in first} == {orders, billing}

    # Nothing changed: no new rows are extracted, but the staged corpus is
    # still read back so cognee keeps both documents.
    unchanged = await run()
    assert {row.primary_key_value for row in unchanged} == {orders, billing}

    cfn.delete_stack(StackName="orders")
    after_delete = await run()
    assert {row.primary_key_value for row in after_delete} == {billing}
