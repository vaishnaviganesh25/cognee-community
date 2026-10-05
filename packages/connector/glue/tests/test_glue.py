"""Tests for the Glue Data Catalog connector. No real AWS account is used.

* Rendering tests are pure.
* Sync tests run against moto's in-memory Glue catalog (``mock_aws``) for the
  incremental hash cursor and forget-on-delete.
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

from cognee_community_connector_glue.glue import (
    GLUE_SOURCE_NAME,
    glue_source,
    render_table,
    sync_tables,
)

REGION = "us-east-1"


@pytest.fixture
def glue():
    with mock_aws():
        yield boto3.client("glue", region_name=REGION)


def _create_db(client, name, description="Sales data"):
    client.create_database(DatabaseInput={"Name": name, "Description": description})


def _table_input(name, columns=None, description="Customer orders"):
    return {
        "Name": name,
        "Description": description,
        "TableType": "EXTERNAL_TABLE",
        "Owner": "data-team",
        "PartitionKeys": [{"Name": "dt", "Type": "string", "Comment": "order date"}],
        "StorageDescriptor": {
            "Columns": columns
            or [
                {"Name": "order_id", "Type": "bigint"},
                {"Name": "email", "Type": "string", "Comment": "customer email"},
            ],
            "Location": f"s3://lake/{name}/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
            },
        },
        "Parameters": {"classification": "json", "db_password": "hunter2"},
    }


def _create_table(client, db, name, **kwargs):
    client.create_table(DatabaseName=db, TableInput=_table_input(name, **kwargs))


def _sync(client, state, **kwargs):
    return list(sync_tables({REGION: client}, state, **kwargs))


def _ids(rows):
    return {row["id"] for row in rows}


def _tid(db, name):
    return f"glue:{REGION}:123456789012:{db}/{name}"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def _table(**overrides):
    table = {"DatabaseName": "sales", **_table_input("orders")}
    table.update(overrides)
    return table


def test_render_table_includes_schema_and_storage():
    text = render_table(_table(), {"Name": "sales", "Description": "Sales data"}, region=REGION)

    assert "Glue Data Catalog table `sales.orders`" in text
    assert "- email (string): customer email" in text
    assert "- dt (string): order date" in text
    assert "- Location: s3://lake/orders/" in text
    assert "SerDe: org.openx.data.jsonserde.JsonSerDe" in text
    assert "## Database description\nSales data" in text
    assert text.index("order_id") < text.index("email")


def test_render_table_is_deterministic_and_sorts_parameters():
    table = _table(Parameters={"zeta": "1", "alpha": "2"})
    text = render_table(table, {"Name": "sales"}, region=REGION)

    assert text == render_table(table, {"Name": "sales"}, region=REGION)
    assert text.index("alpha = 2") < text.index("zeta = 1")


def test_render_table_skips_sensitive_parameters():
    text = render_table(_table(), {"Name": "sales"}, region=REGION)

    assert "classification = json" in text
    assert "hunter2" not in text
    assert "db_password" not in text


def test_render_table_can_hide_parameters():
    text = render_table(_table(), {"Name": "sales"}, region=REGION, include_table_parameters=False)

    assert "## Table parameters" not in text


# ---------------------------------------------------------------------------
# Sync against moto
# ---------------------------------------------------------------------------
def test_first_sync_emits_every_table(glue):
    _create_db(glue, "sales")
    _create_table(glue, "sales", "orders")
    _create_table(glue, "sales", "customers")

    state = {}
    rows = _sync(glue, state)

    assert _ids(rows) == {_tid("sales", "orders"), _tid("sales", "customers")}
    row = next(r for r in rows if r["table_name"] == "orders")
    assert row["_deleted"] is False
    assert row["title"] == f"Glue table sales.orders ({REGION})"
    assert "email (string): customer email" in row["content"]
    assert set(state["hashes"]) == _ids(rows)


def test_unchanged_catalog_emits_nothing(glue):
    _create_db(glue, "sales")
    _create_table(glue, "sales", "orders")
    state = {}
    _sync(glue, state)

    assert _sync(glue, state) == []


def test_only_updated_table_is_resynced(glue):
    _create_db(glue, "sales")
    _create_table(glue, "sales", "orders")
    _create_table(glue, "sales", "customers")
    state = {}
    _sync(glue, state)

    glue.update_table(
        DatabaseName="sales",
        TableInput=_table_input("orders", columns=[{"Name": "refund_flag", "Type": "boolean"}]),
    )
    rows = _sync(glue, state)

    assert [row["id"] for row in rows] == [_tid("sales", "orders")]
    assert "refund_flag (boolean)" in rows[0]["content"]


def test_database_description_change_resyncs_its_tables(glue):
    _create_db(glue, "sales")
    _create_table(glue, "sales", "orders")
    state = {}
    _sync(glue, state)

    glue.update_database(Name="sales", DatabaseInput={"Name": "sales", "Description": "Renamed"})
    rows = _sync(glue, state)

    assert [row["id"] for row in rows] == [_tid("sales", "orders")]
    assert "## Database description\nRenamed" in rows[0]["content"]


def test_dropped_table_becomes_tombstone(glue):
    _create_db(glue, "sales")
    _create_table(glue, "sales", "orders")
    _create_table(glue, "sales", "customers")
    state = {}
    _sync(glue, state)

    glue.delete_table(DatabaseName="sales", Name="orders")
    rows = _sync(glue, state)

    assert rows == [{"id": _tid("sales", "orders"), "_deleted": True}]
    assert set(state["hashes"]) == {_tid("sales", "customers")}


def test_dropped_database_forgets_its_tables(glue):
    _create_db(glue, "sales")
    _create_db(glue, "hr")
    _create_table(glue, "sales", "orders")
    _create_table(glue, "hr", "staff")
    state = {}
    _sync(glue, state, databases=["sales", "hr"])

    glue.delete_table(DatabaseName="hr", Name="staff")
    glue.delete_database(Name="hr")
    rows = _sync(glue, state, databases=["sales", "hr"])

    assert rows == [{"id": _tid("hr", "staff"), "_deleted": True}]


def test_database_allow_list_limits_scope(glue):
    _create_db(glue, "sales")
    _create_db(glue, "hr")
    _create_table(glue, "sales", "orders")
    _create_table(glue, "hr", "staff")

    rows = _sync(glue, {}, databases=["sales"])

    assert _ids(rows) == {_tid("sales", "orders")}


def test_prefix_filter_limits_scope(glue):
    _create_db(glue, "sales")
    _create_table(glue, "sales", "fact_orders")
    _create_table(glue, "sales", "tmp_orders")

    rows = _sync(glue, {}, table_name_prefix="fact_")

    assert _ids(rows) == {_tid("sales", "fact_orders")}


def test_max_tables_guard_aborts(glue):
    _create_db(glue, "sales")
    _create_table(glue, "sales", "a")
    _create_table(glue, "sales", "b")

    with pytest.raises(ValueError, match="max_tables=1"):
        _sync(glue, {}, max_tables=1)


def test_failed_sync_leaves_state_untouched(glue):
    _create_db(glue, "sales")
    _create_table(glue, "sales", "orders")
    state = {"hashes": {"glue:old": "x"}}

    with pytest.raises(ValueError):
        _sync(glue, state, max_tables=0)

    assert state == {"hashes": {"glue:old": "x"}}


def test_empty_catalog_keeps_previous_tables(glue):
    state = {"hashes": {_tid("sales", "orders"): "x"}}

    assert _sync(glue, state) == []
    assert state["hashes"] == {_tid("sales", "orders"): "x"}


# ---------------------------------------------------------------------------
# Factory + cognee ingestion path
# ---------------------------------------------------------------------------
def test_source_declares_document_mode_and_pipeline_scope():
    from cognee.tasks.ingestion.dlt_utils import PIPELINE_SCOPE_ATTR, document_source_tag

    source = glue_source(clients={REGION: SimpleNamespace()}, resource_name="acct_a")

    assert document_source_tag(source) == GLUE_SOURCE_NAME
    assert getattr(source, PIPELINE_SCOPE_ATTR) == "acct_a"


@pytest.mark.asyncio
async def test_cognee_ingestion_syncs_and_forgets(glue, tmp_path, monkeypatch, request):
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
    pipelines_dir = tempfile.mkdtemp(prefix="glue")
    request.addfinalizer(lambda: shutil.rmtree(pipelines_dir, ignore_errors=True))
    pipeline_factory = dlt.pipeline
    monkeypatch.setattr(
        dlt,
        "pipeline",
        lambda **kw: pipeline_factory(pipelines_dir=pipelines_dir, **kw),
    )

    async def run():
        return await ingest.ingest_dlt_source(
            glue_source(clients={REGION: glue}),
            "data_catalog",
            primary_key="id",
            write_disposition="merge",
            max_rows_per_table=0,
        )

    _create_db(glue, "sales")
    _create_table(glue, "sales", "orders")
    _create_table(glue, "sales", "customers")
    orders, customers = _tid("sales", "orders"), _tid("sales", "customers")

    first = await run()
    assert {row.primary_key_value for row in first} == {orders, customers}

    unchanged = await run()
    assert {row.primary_key_value for row in unchanged} == {orders, customers}

    glue.delete_table(DatabaseName="sales", Name="orders")
    after_delete = await run()
    assert {row.primary_key_value for row in after_delete} == {customers}
