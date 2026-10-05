# cognee-community-connector-cloudwatch-alarms

An Amazon CloudWatch Alarms data-source connector for
[cognee](https://github.com/topoteretes/cognee): sync your alarms into memory and ask "which
alarms fired during Tuesday's outage, and how often does `orders-depth` flap?"

It exposes a `dlt` source you hand to `cognee.remember(...)`. Each alarm becomes one markdown
document (state and reason, the metric or composite rule it watches, threshold, dimensions,
actions and recent state changes) that goes through cognee's normal cognify pipeline.

## Install

```bash
pip install cognee-community-connector-cloudwatch-alarms
# or, from this monorepo:
cd packages/connector/cloudwatch-alarms && uv sync --extra test
```

Requires `cognee>=1.6` (table-scoped dlt document cleanup).

## Usage

```python
import cognee
from cognee_community_connector_cloudwatch_alarms import cloudwatch_alarms_source

await cognee.remember(
    cloudwatch_alarms_source(regions=["ap-south-1"], alarm_name_prefix="prod-"),
    dataset_name="aws_alarms",
    primary_key="id",
    write_disposition="merge",  # required: cognee's default "replace" breaks incremental sync
)

answer = await cognee.search(
    query_text="Which alarms are firing right now and what do they watch?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["aws_alarms"],
)
```

See `examples/example.py` for a runnable version.

| Argument | Default | Meaning |
|---|---|---|
| `regions` | session region | Regions to sync |
| `alarm_name_prefix` | `None` | Only alarms whose name starts with this |
| `max_alarms` | `1000` | Abort before fetching history if more alarms are in scope |
| `max_history_items` | `10` | State changes kept per alarm (`0` disables history) |
| `profile_name` | `None` | Named AWS profile instead of the default credential chain |
| `resource_name` | `cloudwatch_alarms` | dlt table name; use one per account if several share a dataset |

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
        "cloudwatch:DescribeAlarms",
        "cloudwatch:DescribeAlarmHistory"
      ],
      "Resource": "*"
    }
  ]
}
```

Set `LLM_API_KEY` as for any cognee run.

## How sync and forget-on-delete work

- **Incremental.** `DescribeAlarms` returns every metric and composite alarm with its state and
  the time its configuration and state last changed, in one paginated call per region. The
  connector keeps a fingerprint per alarm in dlt state and only calls `DescribeAlarmHistory`
  for alarms whose fingerprint changed. An unchanged account costs one listing per region.
- **Forget-on-delete.** An alarm synced before but missing from this run's listing (deleted, or
  out of scope after you change `regions` / `alarm_name_prefix`) is emitted with the `_deleted`
  hard-delete marker. dlt removes the row on `merge`, and cognee's orphan cleanup removes the
  document from the graph and vector stores.
- **30-day history.** CloudWatch keeps alarm history for 30 days, so state changes older than
  that never appear in a document. Route alarm state changes to EventBridge if you need a longer
  record.
- **Safe failure.** State is only saved when a run finishes. If the listing comes back empty
  while alarms were synced before (wrong region, missing permission), nothing is forgotten.
- **Stable documents.** Rendering is deterministic, so an unchanged alarm keeps its content
  hash and is not re-cognified.

## Privacy

- Metric data is never read. Only alarm definitions and their state history are ingested.
- Action ARNs (for example SNS topics) and alarm descriptions are ingested as-is. Don't put
  secrets in them.

## Testing

```bash
uv run pytest tests/
```

Tests use [moto](https://github.com/getmoto/moto) (`mock_aws`) with dummy credentials, so they
never touch a real account. moto does not implement `DescribeAlarmHistory`, so the history path
is covered with a small fake client. The tests cover rendering, the incremental fingerprint,
forget-on-delete, the scope and size guards, and a full run through cognee's
`ingest_dlt_source`.
