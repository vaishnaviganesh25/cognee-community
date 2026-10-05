# cognee-community-connector-glue

An AWS Glue Data Catalog data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync your table metadata into memory and ask "which table has customer emails, and how is
`sales.orders` partitioned?"

It exposes a `dlt` source you hand to `cognee.remember(...)`. Each catalog table becomes one
markdown document (database, columns with types and comments, partition keys, S3 location,
storage format and table parameters) that goes through cognee's normal cognify pipeline.

## Install

```bash
pip install cognee-community-connector-glue
# or, from this monorepo:
cd packages/connector/glue && uv sync --extra test
```

Requires `cognee>=1.6` (table-scoped dlt document cleanup).

## Usage

```python
import cognee
from cognee_community_connector_glue import glue_source

await cognee.remember(
    glue_source(regions=["ap-south-1"], databases=["sales"]),
    dataset_name="data_catalog",
    primary_key="id",
    write_disposition="merge",  # required: cognee's default "replace" breaks incremental sync
)

answer = await cognee.search(
    query_text="Which tables contain customer email addresses?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["data_catalog"],
)
```

See `examples/example.py` for a runnable version.

| Argument | Default | Meaning |
|---|---|---|
| `regions` | session region | Regions to sync |
| `databases` | all databases | Only sync these databases |
| `table_name_prefix` | `None` | Only tables whose name starts with this |
| `include_table_parameters` | `True` | Render table parameters (credential-like keys are always skipped) |
| `max_tables` | `1000` | Abort before emitting anything if more tables are in scope |
| `profile_name` | `None` | Named AWS profile instead of the default credential chain |
| `resource_name` | `glue_tables` | dlt table name; use one per account if several share a dataset |

## Setup

Credentials come from the standard boto3 chain: environment variables, `AWS_PROFILE` / shared
config, or an instance/task role. If you sign in with `aws login`, botocore also needs its CRT
extra: `pip install "botocore[crt]"`. The connector only needs this read-only policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabases",
        "glue:GetDatabase",
        "glue:GetTables"
      ],
      "Resource": "*"
    }
  ]
}
```

If the catalog is governed by Lake Formation, the principal also needs `DESCRIBE` on the
databases and tables it should see.

Set `LLM_API_KEY` as for any cognee run.

## How sync and forget-on-delete work

- **Incremental.** `GetTables` already returns full table definitions, so a run costs one
  `GetDatabases` call plus one paginated `GetTables` call per database. The connector renders
  each table and keeps a SHA-256 hash of the document in dlt state. Only tables whose document
  changed are emitted. Hashing the document instead of trusting `UpdateTime` also catches
  changes to the database description, which `UpdateTime` does not reflect.
- **Forget-on-delete.** The catalog has no delete feed. A table synced before but missing from
  this run's listing (dropped, its database dropped, or out of scope after you change
  `databases` / `table_name_prefix`) is emitted with the `_deleted` hard-delete marker. dlt
  removes the row on `merge`, and cognee's orphan cleanup removes the document from the graph
  and vector stores.
- **Safe failure.** State is only saved when a run finishes. If the listing comes back with no
  tables at all while tables were synced before (wrong region, missing permission), nothing is
  forgotten.
- **Stable documents.** Rendering is deterministic, so an unchanged table keeps its content
  hash and is not re-cognified.

## Privacy

- Table data is never read. Only catalog metadata is ingested.
- Table parameter keys containing `password`, `secret`, `token`, `credential` or `key` are never
  rendered. Pass `include_table_parameters=False` to drop parameters entirely.
- Column comments and descriptions are ingested as-is. Don't put secrets in them.

## Testing

```bash
uv run pytest tests/
```

Tests use [moto](https://github.com/getmoto/moto) (`mock_aws`) with dummy credentials, so they
never touch a real account. They cover rendering, the incremental hash cursor, forget-on-delete
for dropped tables and databases, the scope and size guards, and a full run through cognee's
`ingest_dlt_source`.
