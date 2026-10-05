"""AWS Glue Data Catalog connector for cognee — a ``dlt`` source that turns tables into memory.

Each catalog table becomes one markdown document (its database, columns with
types and comments, partition keys, storage location and formats), ingested
through cognee's document path so it goes through normal cognify::

    import cognee
    from cognee_community_connector_glue import glue_source

    await cognee.remember(
        glue_source(regions=["ap-south-1"], databases=["sales"]),
        dataset_name="data_catalog",
        primary_key="id",
        write_disposition="merge",   # REQUIRED for incremental sync
    )

Design
------
* **Auth** — the standard boto3 credential chain. Only read-only calls are
  made: ``GetDatabases``, ``GetDatabase`` and ``GetTables``. Table data is
  never read; this connector ingests the catalog, not the rows.
* **Primary key** — ``glue:<region>:<catalog id>:<database>/<table>``.
* **Incremental sync** — ``GetTables`` already returns full table
  definitions, so the connector renders every table and keeps a hash of each
  document in dlt resource state. Only tables whose document changed are
  emitted. Hashing the document instead of trusting ``UpdateTime`` also picks
  up database-level changes (a new database description) that ``UpdateTime``
  does not reflect.
* **Forget-on-delete** — the catalog has no delete feed. A table synced
  before but missing from this run's listing is emitted with the ``_deleted``
  hard-delete marker. dlt drops the row on ``merge`` and cognee's orphan
  cleanup removes the document from memory.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator
from datetime import datetime
from typing import Any
from urllib.parse import quote

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

logger = get_logger("glue_connector")

GLUE_SOURCE_NAME = "glue"
DEFAULT_RESOURCE_NAME = "glue_tables"

# Table parameters are free-form key/value pairs. Connection strings and
# credentials occasionally end up there, so keys containing these words are
# never rendered.
_SENSITIVE_PARAMETER_WORDS = ("password", "secret", "token", "credential", "passwd", "key")

_EXTRA_HINT = (
    "The Glue connector requires boto3 and dlt: pip install cognee-community-connector-glue"
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
        "glue",
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


def _is_not_found(exc: Exception) -> bool:
    error = getattr(exc, "response", {}).get("Error", {})
    return error.get("Code") == "EntityNotFoundException"


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


def _column_line(column: dict) -> str:
    line = f"{column.get('Name')} ({column.get('Type', 'unknown')})"
    if column.get("Comment"):
        line += f": {column['Comment']}"
    return line


def _is_sensitive(key: str) -> bool:
    lowered = key.lower()
    return any(word in lowered for word in _SENSITIVE_PARAMETER_WORDS)


def render_table(
    table: dict,
    database: dict,
    *,
    region: str,
    include_table_parameters: bool = True,
) -> str:
    """Render a catalog table as deterministic markdown.

    Columns keep their schema order, which carries meaning. Every unordered
    collection (parameters) is sorted, so the same catalog state always
    produces the same text and cognee skips re-cognifying it.
    """
    db_name = table.get("DatabaseName") or database.get("Name", "")
    name = table.get("Name", "")
    storage = table.get("StorageDescriptor") or {}

    lines = [
        f"Glue Data Catalog table `{db_name}.{name}` in region {region}.",
        "",
        f"- Database: {db_name}",
    ]
    if table.get("TableType"):
        lines.append(f"- Table type: {table['TableType']}")
    if table.get("Owner"):
        lines.append(f"- Owner: {table['Owner']}")
    if table.get("CreateTime"):
        lines.append(f"- Created: {_iso(table['CreateTime'])}")
    if table.get("UpdateTime"):
        lines.append(f"- Last updated: {_iso(table['UpdateTime'])}")
    if storage.get("Location"):
        lines.append(f"- Location: {storage['Location']}")

    if table.get("Description"):
        lines += ["", "## Description", table["Description"].strip()]

    if database.get("Description"):
        lines += ["", "## Database description", database["Description"].strip()]

    lines += ["", "## Columns"]
    lines += _bullets(_column_line(c) for c in storage.get("Columns") or [])

    lines += ["", "## Partition keys"]
    lines += _bullets(_column_line(c) for c in table.get("PartitionKeys") or [])

    lines += ["", "## Storage format"]
    serde = (storage.get("SerdeInfo") or {}).get("SerializationLibrary")
    lines += _bullets(
        f"{label}: {value}"
        for label, value in (
            ("Input format", storage.get("InputFormat")),
            ("Output format", storage.get("OutputFormat")),
            ("SerDe", serde),
            ("Compressed", "yes" if storage.get("Compressed") else None),
        )
        if value
    )

    if include_table_parameters:
        parameters = table.get("Parameters") or {}
        lines += ["", "## Table parameters"]
        lines += _bullets(
            f"{key} = {parameters[key]}" for key in sorted(parameters) if not _is_sensitive(key)
        )

    return "\n".join(lines)


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _table_id(region: str, table: dict) -> str:
    catalog = table.get("CatalogId", "")
    return f"glue:{region}:{catalog}:{table.get('DatabaseName', '')}/{table.get('Name', '')}"


def _console_url(region: str, table: dict) -> str:
    db_name = quote(table.get("DatabaseName", ""), safe="")
    name = quote(table.get("Name", ""), safe="")
    return (
        f"https://{region}.console.aws.amazon.com/glue/home?region={region}"
        f"#/v2/data-catalog/tables/view/{name}?database={db_name}"
    )


# ---------------------------------------------------------------------------
# Catalog reads
# ---------------------------------------------------------------------------
def _list_databases(client: Any, databases: list[str] | None) -> list[dict]:
    if databases is None:
        return list(_paginate(client, "get_databases", "DatabaseList"))
    found = []
    for name in databases:
        try:
            found.append(client.get_database(Name=name)["Database"])
        except Exception as exc:
            if not _is_not_found(exc):
                raise
            logger.warning("Glue: database %r not found; its tables are treated as dropped.", name)
    return found


# ---------------------------------------------------------------------------
# Sync (pure given clients + a state dict — unit-testable)
# ---------------------------------------------------------------------------
def sync_tables(
    clients: dict[str, Any],
    state: dict,
    *,
    databases: list[str] | None = None,
    table_name_prefix: str | None = None,
    max_tables: int = 1000,
    include_table_parameters: bool = True,
) -> Iterator[dict[str, Any]]:
    """Yield rows for new or changed tables, then hard-delete markers.

    ``state["hashes"]`` maps each synced table id to the hash of its document
    from the previous run. It is only rewritten after every listing call
    succeeded, so a failed run is retried in full next time.
    """
    known: dict[str, str] = dict(state.get("hashes") or {})
    rows: dict[str, dict[str, Any]] = {}

    for region, client in clients.items():
        for database in _list_databases(client, databases):
            for table in _paginate(
                client, "get_tables", "TableList", DatabaseName=database["Name"]
            ):
                if table_name_prefix and not table.get("Name", "").startswith(table_name_prefix):
                    continue
                table.setdefault("DatabaseName", database["Name"])
                table_id = _table_id(region, table)
                rows[table_id] = {
                    "id": table_id,
                    "title": f"Glue table {table['DatabaseName']}.{table.get('Name', '')} "
                    f"({region})",
                    "url": _console_url(region, table),
                    "content": render_table(
                        table,
                        database,
                        region=region,
                        include_table_parameters=include_table_parameters,
                    ),
                    "region": region,
                    "database": table["DatabaseName"],
                    "table_name": table.get("Name", ""),
                    "_deleted": False,
                }
                if len(rows) > max_tables:
                    raise ValueError(
                        f"Found more than max_tables={max_tables} Glue tables in scope. "
                        "Narrow the scope with databases= or table_name_prefix=, "
                        "or raise max_tables."
                    )

    hashes: dict[str, str] = {}
    changed = 0
    for table_id in sorted(rows):
        row = rows[table_id]
        digest = _content_hash(row["content"])
        hashes[table_id] = digest
        if known.get(table_id) == digest:
            continue
        yield row
        changed += 1

    gone = set(known) - set(rows)
    if not rows and known:
        # An empty catalog is far more often a wrong region or a missing
        # permission than a real wipe, and Glue has no "deleted" status to
        # confirm it. Keep everything until a listing shows tables again.
        logger.warning(
            "Glue: listing returned no tables; keeping %d previously synced table(s).",
            len(known),
        )
        hashes = known
        gone = set()

    for table_id in sorted(gone):
        yield {"id": table_id, "_deleted": True}

    state["hashes"] = hashes
    logger.info("Glue: %d changed table(s), %d deletion(s).", changed, len(gone))


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def glue_source(
    *,
    regions: list[str] | None = None,
    databases: list[str] | None = None,
    table_name_prefix: str | None = None,
    include_table_parameters: bool = True,
    max_tables: int = 1000,
    profile_name: str | None = None,
    resource_name: str = DEFAULT_RESOURCE_NAME,
    clients: dict[str, Any] | None = None,
):
    """Return a ``dlt`` resource that yields Glue catalog tables for ``remember``.

    Args:
        regions: AWS regions to sync. Defaults to the session's configured
            region (``AWS_REGION`` / profile ``region``).
        databases: Only sync these databases. Defaults to every database.
        table_name_prefix: Only sync tables whose name starts with this.
        include_table_parameters: Render table parameters. Keys that look like
            credentials are always skipped.
        max_tables: Abort before emitting anything if more tables than this
            are in scope.
        profile_name: Named AWS profile to use instead of the default chain.
        resource_name: dlt table name. Use a distinct name per account when
            several accounts sync into the same dataset.
        clients: ``{region: client}`` mapping of pre-built Glue clients,
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
            "The Glue connector needs a cognee build with table-scoped dlt document "
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
    def glue_tables():
        resolved_clients = clients or {
            region: _make_client(region, profile_name) for region in region_list
        }
        yield from sync_tables(
            resolved_clients,
            dlt.current.resource_state(),
            databases=databases,
            table_name_prefix=table_name_prefix,
            max_tables=max_tables,
            include_table_parameters=include_table_parameters,
        )

    resource = glue_tables()
    setattr(resource, DOCUMENT_SOURCE_ATTR, GLUE_SOURCE_NAME)
    setattr(resource, dlt_utils.PIPELINE_SCOPE_ATTR, resource_name)
    return resource
