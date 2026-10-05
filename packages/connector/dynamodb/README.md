# cognee-community-connector-dynamodb

An Amazon DynamoDB data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync the items you choose into memory and ask "what does the `refund_policy` item in
`app-config` say, and when was it last updated?"

It exposes a `dlt` source you hand to `cognee.remember(...)`. Each item of a table you opt in
becomes one markdown document (its key, the attribute holding its text, and its other plain
attributes) that goes through cognee's normal cognify pipeline.

## Install

```bash
pip install cognee-community-connector-dynamodb
# or, from this monorepo:
cd packages/connector/dynamodb && uv sync --extra test
```

Requires `cognee>=1.6` (table-scoped dlt document cleanup).

## Usage

```python
import cognee
from cognee_community_connector_dynamodb import DynamoDBTable, dynamodb_source

await cognee.remember(
    dynamodb_source(
        tables=[
            DynamoDBTable(
                "app-config",
                text_attribute="body",
                title_attribute="name",
                updated_at_attribute="updated_at",
            ),
            DynamoDBTable(
                "support-tickets", text_attribute="summary", exclude_attributes=("email",)
            ),
        ],
        region="ap-south-1",
    ),
    dataset_name="app_data",
    primary_key="id",
    write_disposition="merge",  # required: cognee's default "replace" breaks incremental sync
)

answer = await cognee.search(
    query_text="What is our refund policy and when did it last change?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["app_data"],
)
```

See `examples/example.py` for a runnable version.

`dynamodb_source` arguments:

| Argument | Default | Meaning |
|---|---|---|
| `tables` | required | Tables to sync; nothing else is read |
| `region` | session region | Region of the tables |
| `max_items` | `10000` | Abort before emitting anything if the scans read more items |
| `profile_name` | `None` | Named AWS profile instead of the default credential chain |
| `resource_name` | `dynamodb_items` | dlt table name; use one per account or region if several share a dataset |

`DynamoDBTable` fields:

| Field | Default | Meaning |
|---|---|---|
| `name` | required | Table name |
| `text_attribute` | required | String attribute holding the document text; items without it are skipped |
| `title_attribute` | item key | Attribute used as the document title |
| `updated_at_attribute` | `None` | Attribute rendered as the item's last-updated value |
| `exclude_attributes` | `()` | Attributes never rendered |

## Setup

Credentials come from the standard boto3 chain: environment variables, `AWS_PROFILE` / shared
config, or an instance/task role. If you sign in with `aws login`, botocore also needs its CRT
extra: `pip install "botocore[crt]"`. The connector only needs this read-only policy, scoped to
the tables you sync:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["dynamodb:DescribeTable", "dynamodb:Scan"],
      "Resource": "arn:aws:dynamodb:ap-south-1:123456789012:table/app-config"
    }
  ]
}
```

Set `LLM_API_KEY` as for any cognee run.

## How sync and forget-on-delete work

- **One scan per table.** DynamoDB has no built-in change timestamp, and a `Scan` filter is
  applied after items are read, so a filtered scan costs the same read capacity as a full one.
  The connector does one paginated scan per table per run. Each scan consumes read capacity in
  proportion to the table's size, so sync small, document-like tables and schedule runs
  accordingly.
- **Incremental.** Each item is rendered and a SHA-256 hash of the document is kept in dlt
  state. Only items whose document changed are emitted, so unchanged items are never
  re-cognified, whether or not the application maintains an `updated_at` attribute.
- **Forget-on-delete.** The same scan sees every key. An item synced before but missing now
  (deleted, or no longer carrying `text_attribute`) is emitted with the `_deleted` hard-delete
  marker. dlt removes the row on `merge`, and cognee's orphan cleanup removes the document from
  the graph and vector stores. Removing a table from `tables` forgets its items the same way.
- **Safe failure.** State is only saved when a run finishes. A table that does not exist raises
  an error instead of wiping its items from memory.

## Privacy

- Only tables you name are read, and only items carrying `text_attribute` are ingested.
- Binary attributes are never rendered. Use `exclude_attributes` for anything else that should
  stay out of memory, such as emails or phone numbers.
- The connector never writes to DynamoDB.

## Testing

```bash
uv run pytest tests/
```

Tests use [moto](https://github.com/getmoto/moto) (`mock_aws`) with dummy credentials, so they
never touch a real account. They cover rendering and type handling, the opt-in scope, the
incremental hash cursor, forget-on-delete, the size guard, and a full run through cognee's
`ingest_dlt_source`.
