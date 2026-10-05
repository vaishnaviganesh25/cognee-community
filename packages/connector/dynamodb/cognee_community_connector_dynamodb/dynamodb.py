"""Amazon DynamoDB connector for cognee — a ``dlt`` source that turns chosen items into memory.

Each item of a table the user opts in becomes one markdown document (its key,
the attribute that holds its text, and its other plain attributes), ingested
through cognee's document path so it goes through normal cognify::

    import cognee
    from cognee_community_connector_dynamodb import DynamoDBTable, dynamodb_source

    await cognee.remember(
        dynamodb_source(
            tables=[DynamoDBTable("app-config", text_attribute="body")],
            region="ap-south-1",
        ),
        dataset_name="app_data",
        primary_key="id",
        write_disposition="merge",   # REQUIRED for incremental sync
    )

Design
------
* **Opt-in scope** — nothing is read until a table is named, and every table
  names the attribute that holds the document text. Items without that
  attribute are skipped, so a table of unrelated records is never ingested
  wholesale.
* **Auth** — the standard boto3 credential chain. Only read-only calls are
  made: ``DescribeTable`` and ``Scan``. The connector never writes.
* **Primary key** — ``dynamodb:<region>:<table>:<item key as JSON>``.
* **Incremental sync** — DynamoDB has no change timestamp of its own, and a
  ``Scan`` filter is applied after items are read, so a filtered scan costs
  the same read capacity as a full one. The connector does one paginated scan
  per table, renders each item and keeps a hash of each document in dlt
  resource state. Only items whose document changed are emitted.
* **Forget-on-delete** — the same scan sees every key, so an item synced
  before but missing now (deleted, or no longer carrying the text attribute)
  is emitted with the ``_deleted`` hard-delete marker. dlt drops the row on
  ``merge`` and cognee's orphan cleanup removes the document from memory.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

logger = get_logger("dynamodb_connector")

DYNAMODB_SOURCE_NAME = "dynamodb"
DEFAULT_RESOURCE_NAME = "dynamodb_items"

_BINARY_TYPES = ("B", "BS")

_EXTRA_HINT = (
    "The DynamoDB connector requires boto3 and dlt: pip install cognee-community-connector-dynamodb"
)


@dataclass(frozen=True)
class DynamoDBTable:
    """One table to sync.

    Args:
        name: Table name.
        text_attribute: Attribute holding the document text. Items without it
            (or with a non-string value) are skipped.
        title_attribute: Attribute used as the document title. Defaults to
            the item key.
        updated_at_attribute: Attribute holding a last-updated value, rendered
            in the document when present.
        exclude_attributes: Attributes never rendered, for example personal
            data stored next to the text.
    """

    name: str
    text_attribute: str
    title_attribute: str | None = None
    updated_at_attribute: str | None = None
    exclude_attributes: tuple[str, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# AWS client
# ---------------------------------------------------------------------------
def _make_client(region: str | None, profile_name: str | None) -> Any:
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise ImportError(_EXTRA_HINT) from exc

    session = boto3.Session(profile_name=profile_name)
    return session.client(
        "dynamodb",
        region_name=region,
        config=Config(retries={"mode": "adaptive", "max_attempts": 10}),
    )


def _client_region(client: Any) -> str:
    return getattr(getattr(client, "meta", None), "region_name", None) or "unknown"


def _key_attributes(client: Any, table: str) -> list[str]:
    try:
        description = client.describe_table(TableName=table)["Table"]
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if code == "ResourceNotFoundException":
            raise ValueError(
                f"DynamoDB table {table!r} not found in region {_client_region(client)}. "
                "Check the table name and region."
            ) from exc
        raise
    return [key["AttributeName"] for key in description["KeySchema"]]


def _scan(client: Any, table: str) -> Iterator[dict]:
    for page in client.get_paginator("scan").paginate(TableName=table):
        yield from page.get("Items") or []


# ---------------------------------------------------------------------------
# Rendering (pure)
# ---------------------------------------------------------------------------
def _plain(value: Any) -> Any:
    """Turn deserialized DynamoDB values into JSON-friendly Python values."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, set):
        return sorted(_plain(v) for v in value)
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    return value


def _deserialize(item: dict) -> dict[str, Any]:
    from boto3.dynamodb.types import TypeDeserializer

    deserializer = TypeDeserializer()
    return {
        name: _plain(deserializer.deserialize(value))
        for name, value in item.items()
        if not any(binary in value for binary in _BINARY_TYPES)
    }


def _format_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, default=str)


def _bullets(items: Iterable[str]) -> list[str]:
    lines = [f"- {item}" for item in items]
    return lines or ["- (none)"]


def item_key(attributes: dict[str, Any], key_names: list[str]) -> str:
    return json.dumps({name: attributes.get(name) for name in key_names}, sort_keys=True)


def render_item(
    attributes: dict[str, Any],
    spec: DynamoDBTable,
    key_names: list[str],
    *,
    region: str,
) -> str:
    """Render one item as deterministic markdown.

    Attributes are sorted by name and nested values are dumped with sorted
    keys, so the same item always produces the same text.
    """
    key = item_key(attributes, key_names)
    lines = [f"DynamoDB item {key} in table `{spec.name}` (region {region}).", ""]
    if spec.updated_at_attribute and spec.updated_at_attribute in attributes:
        lines += [f"- Updated: {_format_value(attributes[spec.updated_at_attribute])}", ""]

    lines += ["## Text", str(attributes[spec.text_attribute]).strip()]

    hidden = {spec.text_attribute, *key_names, *spec.exclude_attributes}
    lines += ["", "## Attributes"]
    lines += _bullets(
        f"{name} = {_format_value(attributes[name])}"
        for name in sorted(attributes)
        if name not in hidden
    )
    return "\n".join(lines)


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Sync (pure given a client + a state dict — unit-testable)
# ---------------------------------------------------------------------------
def sync_items(
    client: Any,
    state: dict,
    tables: list[DynamoDBTable],
    *,
    region: str,
    max_items: int = 10000,
) -> Iterator[dict[str, Any]]:
    """Yield rows for new or changed items, then hard-delete markers.

    ``state["hashes"]`` maps each synced item id to the hash of its document
    from the previous run. It is only rewritten after every scan succeeded,
    so a failed run is retried in full next time.
    """
    if not tables:
        raise ValueError("Name at least one table: tables=[DynamoDBTable(...)].")

    known: dict[str, str] = dict(state.get("hashes") or {})
    rows: dict[str, dict[str, Any]] = {}
    scanned = 0

    for spec in tables:
        key_names = _key_attributes(client, spec.name)
        for raw in _scan(client, spec.name):
            scanned += 1
            if scanned > max_items:
                raise ValueError(
                    f"Scanned more than max_items={max_items} DynamoDB items. "
                    "Sync fewer tables or raise max_items."
                )
            attributes = _deserialize(raw)
            if not isinstance(attributes.get(spec.text_attribute), str):
                continue
            # Binary key attributes are dropped by _deserialize, and a key
            # without them would not identify the item.
            if any(name not in attributes for name in key_names):
                continue
            key = item_key(attributes, key_names)
            item_id = f"dynamodb:{region}:{spec.name}:{key}"
            title_value = attributes.get(spec.title_attribute) if spec.title_attribute else None
            title = _format_value(title_value) if title_value is not None else key
            rows[item_id] = {
                "id": item_id,
                "title": f"{spec.name}: {title}",
                "content": render_item(attributes, spec, key_names, region=region),
                "region": region,
                "table_name": spec.name,
                "item_key": key,
                "_deleted": False,
            }

    hashes: dict[str, str] = {}
    changed = 0
    for item_id in sorted(rows):
        row = rows[item_id]
        digest = _content_hash(row["content"])
        hashes[item_id] = digest
        if known.get(item_id) == digest:
            continue
        yield row
        changed += 1

    gone = sorted(set(known) - set(rows))
    for item_id in gone:
        yield {"id": item_id, "_deleted": True}

    state["hashes"] = hashes
    logger.info("DynamoDB: %d changed item(s), %d deletion(s).", changed, len(gone))


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def dynamodb_source(
    *,
    tables: list[DynamoDBTable],
    region: str | None = None,
    max_items: int = 10000,
    profile_name: str | None = None,
    resource_name: str = DEFAULT_RESOURCE_NAME,
    client: Any = None,
):
    """Return a ``dlt`` resource that yields DynamoDB items for ``remember``.

    Args:
        tables: Tables to sync. Nothing else is read.
        region: AWS region of the tables. Defaults to the session's configured
            region (``AWS_REGION`` / profile ``region``).
        max_items: Abort before emitting anything if the scans read more items
            than this.
        profile_name: Named AWS profile to use instead of the default chain.
        resource_name: dlt table name. Use a distinct name per account or
            region when several sync into the same dataset.
        client: A pre-built DynamoDB client, mainly for tests. Overrides
            ``region``/``profile_name``.

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
            "The DynamoDB connector needs a cognee build with table-scoped dlt document "
            "cleanup (cognee>=1.6). Upgrade cognee."
        )
    if not tables:
        raise ValueError("Name at least one table: tables=[DynamoDBTable(...)].")

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def dynamodb_items():
        resolved_client = client or _make_client(region, profile_name)
        resolved_region = _client_region(resolved_client)
        if resolved_region == "unknown":
            raise ValueError(
                "No AWS region configured: pass region=... or set AWS_REGION / a profile region."
            )
        yield from sync_items(
            resolved_client,
            dlt.current.resource_state(),
            list(tables),
            region=resolved_region,
            max_items=max_items,
        )

    resource = dynamodb_items()
    setattr(resource, DOCUMENT_SOURCE_ATTR, DYNAMODB_SOURCE_NAME)
    setattr(resource, dlt_utils.PIPELINE_SCOPE_ATTR, resource_name)
    return resource
