# cognee-community-connector-cloudformation

An AWS CloudFormation data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync your stacks into memory and ask "what does the `orders-prod` stack deploy, and why did
its last update fail?"

It exposes a `dlt` source you hand to `cognee.remember(...)`. Each stack becomes one markdown
document (status, description, parameters, outputs, tags, drift status, resources and recent
failure events) that goes through cognee's normal cognify pipeline.

## Install

```bash
pip install cognee-community-connector-cloudformation
# or, from this monorepo:
cd packages/connector/cloudformation && uv sync --extra test
```

Requires `cognee>=1.6` (table-scoped dlt document cleanup).

## Usage

```python
import cognee
from cognee_community_connector_cloudformation import cloudformation_source

await cognee.remember(
    cloudformation_source(regions=["ap-south-1"], stack_name_prefix="prod-"),
    dataset_name="aws_stacks",
    primary_key="id",
    write_disposition="merge",  # required: cognee's default "replace" breaks incremental sync
)

answer = await cognee.search(
    query_text="Which stacks did the payments team deploy and what do they export?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["aws_stacks"],
)
```

See `examples/example.py` for a runnable version.

| Argument | Default | Meaning |
|---|---|---|
| `regions` | session region | Regions to sync |
| `stack_name_prefix` | `None` | Only stacks whose name starts with this |
| `include_nested` | `True` | Also sync nested stacks |
| `include_parameter_values` | `True` | Render parameter values, not only names |
| `max_stacks` | `500` | Abort before describing anything if more stacks are in scope |
| `max_failure_events` | `10` | Failed events kept per stack (`0` disables) |
| `profile_name` | `None` | Named AWS profile instead of the default credential chain |
| `resource_name` | `cloudformation_stacks` | dlt table name; use one per account if several share a dataset |

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
        "cloudformation:ListStacks",
        "cloudformation:DescribeStacks",
        "cloudformation:ListStackResources",
        "cloudformation:DescribeStackEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

Set `LLM_API_KEY` as for any cognee run.

## How sync and forget-on-delete work

- **Incremental.** `ListStacks` returns every stack's status and last-change time in one
  paginated call per region. The connector keeps a fingerprint per stack (last-change time,
  status, drift status) in dlt state and only calls `DescribeStacks`, `ListStackResources` and
  `DescribeStackEvents` for stacks whose fingerprint changed. An unchanged account costs one
  listing per region.
- **Forget-on-delete.** A previously synced stack that is no longer active (`DELETE_COMPLETE`,
  or out of scope after you change `regions` / `stack_name_prefix`) is emitted with the
  `_deleted` hard-delete marker. dlt removes the row on `merge`, and cognee's orphan cleanup
  removes the document from the graph and vector stores.
- **Safe failure.** State is only saved when a run finishes. If the listing comes back empty
  while stacks were synced before (wrong region, missing permission), only stacks that AWS
  explicitly reports as `DELETE_COMPLETE` are forgotten.
- **Stable documents.** Rendering is deterministic, so an unchanged stack keeps its content
  hash and is not re-cognified.

## Privacy

- Templates are never fetched: template bodies often contain hard-coded secrets.
- Parameters marked `NoEcho` are masked by AWS. The connector renders `ParameterValue`, never
  the `ResolvedValue` that CloudFormation reads from SSM Parameter Store.
- Pass `include_parameter_values=False` to keep only parameter names.
- Outputs and tags are ingested as-is. Don't put secrets in outputs.

## Testing

```bash
uv run pytest tests/
```

Tests use [moto](https://github.com/getmoto/moto) (`mock_aws`) with dummy credentials, so they
never touch a real account. They cover rendering, the incremental fingerprint cursor,
forget-on-delete, the scope and size guards, and a full run through cognee's
`ingest_dlt_source`.
