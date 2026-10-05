"""Tests for the DynamoDB connector. No real AWS account is used.

* Rendering tests are pure.
* Sync tests run against moto's in-memory DynamoDB (``mock_aws``) for the
  opt-in scope, the incremental hash cursor and forget-on-delete.
* The last test drives the source through cognee's own ``ingest_dlt_source``
  into a temporary sqlite destination, the same path ``cognee.remember`` uses.
"""

import importlib
import shutil
import tempfile
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

from cognee_community_connector_dynamodb.dynamodb import (
    DYNAMODB_SOURCE_NAME,
    DynamoDBTable,
    dynamodb_source,
    render_item,
    sync_items,
)

REGION = "us-east-1"
CONFIG = DynamoDBTable(
    "app-config",
    text_attribute="body",
    title_attribute="name",
    updated_at_attribute="updated_at",
)


@pytest.fixture
def ddb():
    with mock_aws():
        client = boto3.client("dynamodb", region_name=REGION)
        client.create_table(
            TableName="app-config",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield client


def _put(client, pk, body="Refunds are accepted within 30 days.", **extra):
    item = {
        "pk": {"S": pk},
        "name": {"S": pk.replace("_", " ").title()},
        "updated_at": {"S": "2026-03-01T10:00:00Z"},
        **extra,
    }
    if body is not None:
        item["body"] = {"S": body}
    client.put_item(TableName="app-config", Item=item)


def _id(pk, table="app-config"):
    return f'dynamodb:{REGION}:{table}:{{"pk": "{pk}"}}'


def _sync(client, state, tables=(CONFIG,), **kwargs):
    return list(sync_items(client, state, list(tables), region=REGION, **kwargs))


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def test_render_item_shows_text_and_sorted_attributes():
    attributes = {
        "pk": "refund_policy",
        "body": "Refunds are accepted within 30 days.",
        "updated_at": "2026-03-01",
        "owner": "support",
        "limits": {"days": 30, "currency": "INR"},
        "tags": ["billing", "policy"],
    }
    text = render_item(attributes, CONFIG, ["pk"], region=REGION)

    assert text.startswith('DynamoDB item {"pk": "refund_policy"} in table `app-config`')
    assert "- Updated: 2026-03-01" in text
    assert "## Text\nRefunds are accepted within 30 days." in text
    assert '- limits = {"currency": "INR", "days": 30}' in text
    assert text.index("limits =") < text.index("owner =") < text.index("tags =")
    assert "- pk =" not in text
    assert "- body =" not in text


def test_render_item_respects_exclude_attributes():
    spec = DynamoDBTable("t", text_attribute="body", exclude_attributes=("email",))
    text = render_item(
        {"pk": "a", "body": "hello", "email": "alice@example.com"}, spec, ["pk"], region=REGION
    )

    assert "alice@example.com" not in text


# ---------------------------------------------------------------------------
# Sync against moto
# ---------------------------------------------------------------------------
def test_first_sync_emits_items_with_text(ddb):
    _put(ddb, "refund_policy")
    _put(ddb, "shipping_policy", body="We ship within 2 days.")
    _put(ddb, "feature_flags", body=None)

    state = {}
    rows = _sync(ddb, state)

    assert {row["id"] for row in rows} == {_id("refund_policy"), _id("shipping_policy")}
    row = next(r for r in rows if r["id"] == _id("refund_policy"))
    assert row["title"] == "app-config: Refund Policy"
    assert row["_deleted"] is False
    assert "Refunds are accepted within 30 days." in row["content"]
    assert set(state["hashes"]) == {row["id"] for row in rows}


def test_numbers_sets_and_binary_values_are_handled(ddb):
    _put(
        ddb,
        "limits",
        max_days={"N": "30"},
        ratio={"N": "0.5"},
        regions={"SS": ["us", "in"]},
        blob={"B": b"\x00\x01"},
    )
    [row] = _sync(ddb, {})

    assert "- max_days = 30" in row["content"]
    assert "- ratio = 0.5" in row["content"]
    assert '- regions = ["in", "us"]' in row["content"]
    assert "blob" not in row["content"]


def test_unchanged_table_emits_nothing(ddb):
    _put(ddb, "refund_policy")
    state = {}
    _sync(ddb, state)

    assert _sync(ddb, state) == []


def test_only_edited_item_is_resynced(ddb):
    _put(ddb, "refund_policy")
    _put(ddb, "shipping_policy", body="We ship within 2 days.")
    state = {}
    _sync(ddb, state)

    _put(ddb, "refund_policy", body="Refunds are accepted within 14 days.")
    rows = _sync(ddb, state)

    assert [row["id"] for row in rows] == [_id("refund_policy")]
    assert "within 14 days" in rows[0]["content"]


def test_deleted_item_becomes_tombstone(ddb):
    _put(ddb, "refund_policy")
    _put(ddb, "shipping_policy", body="We ship within 2 days.")
    state = {}
    _sync(ddb, state)

    ddb.delete_item(TableName="app-config", Key={"pk": {"S": "refund_policy"}})
    rows = _sync(ddb, state)

    assert rows == [{"id": _id("refund_policy"), "_deleted": True}]
    assert set(state["hashes"]) == {_id("shipping_policy")}


def test_item_losing_its_text_is_forgotten(ddb):
    _put(ddb, "refund_policy")
    state = {}
    _sync(ddb, state)

    _put(ddb, "refund_policy", body=None)

    assert _sync(ddb, state) == [{"id": _id("refund_policy"), "_deleted": True}]


def test_unknown_table_raises_instead_of_forgetting(ddb):
    _put(ddb, "refund_policy")
    state = {}
    _sync(ddb, state)

    with pytest.raises(ValueError, match="not found"):
        _sync(ddb, state, tables=[DynamoDBTable("app-confg", text_attribute="body")])

    assert set(state["hashes"]) == {_id("refund_policy")}


def test_max_items_guard_aborts_and_leaves_state_untouched(ddb):
    _put(ddb, "a")
    _put(ddb, "b")
    state = {"hashes": {"dynamodb:old": "x"}}

    with pytest.raises(ValueError, match="max_items=1"):
        _sync(ddb, state, max_items=1)

    assert state == {"hashes": {"dynamodb:old": "x"}}


def test_at_least_one_table_is_required(ddb):
    with pytest.raises(ValueError, match="at least one table"):
        _sync(ddb, {}, tables=[])


# ---------------------------------------------------------------------------
# Factory + cognee ingestion path
# ---------------------------------------------------------------------------
def test_source_declares_document_mode_and_pipeline_scope():
    from cognee.tasks.ingestion.dlt_utils import PIPELINE_SCOPE_ATTR, document_source_tag

    source = dynamodb_source(tables=[CONFIG], client=SimpleNamespace(), resource_name="acct_a")

    assert document_source_tag(source) == DYNAMODB_SOURCE_NAME
    assert getattr(source, PIPELINE_SCOPE_ATTR) == "acct_a"


@pytest.mark.asyncio
async def test_cognee_ingestion_syncs_and_forgets(ddb, tmp_path, monkeypatch, request):
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
    pipelines_dir = tempfile.mkdtemp(prefix="ddb")
    request.addfinalizer(lambda: shutil.rmtree(pipelines_dir, ignore_errors=True))
    pipeline_factory = dlt.pipeline
    monkeypatch.setattr(
        dlt,
        "pipeline",
        lambda **kw: pipeline_factory(pipelines_dir=pipelines_dir, **kw),
    )

    async def run():
        return await ingest.ingest_dlt_source(
            dynamodb_source(tables=[CONFIG], client=ddb),
            "app_data",
            primary_key="id",
            write_disposition="merge",
            max_rows_per_table=0,
        )

    _put(ddb, "refund_policy")
    _put(ddb, "shipping_policy", body="We ship within 2 days.")
    first = await run()
    assert {row.primary_key_value for row in first} == {
        _id("refund_policy"),
        _id("shipping_policy"),
    }

    unchanged = await run()
    assert {row.primary_key_value for row in unchanged} == {
        _id("refund_policy"),
        _id("shipping_policy"),
    }

    ddb.delete_item(TableName="app-config", Key={"pk": {"S": "refund_policy"}})
    after_delete = await run()
    assert {row.primary_key_value for row in after_delete} == {_id("shipping_policy")}
