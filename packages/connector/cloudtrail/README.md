# cognee-community-connector-cloudtrail

An AWS CloudTrail data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync who-changed-what into memory and ask "who changed the `orders-sg` security group this week,
and did any IAM policy change fail?"

It exposes a `dlt` source you hand to `cognee.remember(...)`. Each write-type management event
becomes one markdown document (time, event name and source, the actor, source IP, the resources
touched, and the error when the call failed) that goes through cognee's normal cognify pipeline.

## Install

```bash
pip install cognee-community-connector-cloudtrail
# or, from this monorepo:
cd packages/connector/cloudtrail && uv sync --extra test
```

Requires `cognee>=1.6` (table-scoped dlt document cleanup).

## Usage

```python
import cognee
from cognee_community_connector_cloudtrail import cloudtrail_source

await cognee.remember(
    cloudtrail_source(regions=["ap-south-1", "us-east-1"], lookback_days=7),
    dataset_name="aws_audit",
    primary_key="id",
    write_disposition="merge",  # required: cognee's default "replace" breaks incremental sync
)

answer = await cognee.search(
    query_text="Who changed security groups this week, and did any change fail?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["aws_audit"],
)
```

See `examples/example.py` for a runnable version. Global services such as IAM record their
events in `us-east-1`, so include that region to see IAM changes.

| Argument | Default | Meaning |
|---|---|---|
| `regions` | session region | Regions to sync |
| `event_names` | all write events | Only these event names, e.g. `["AuthorizeSecurityGroupIngress"]` |
| `lookback_days` | `7` | How far back the first run reads (1 to 90) |
| `retention_days` | `90` | Synced events older than this are forgotten (1 to 90) |
| `max_events` | `5000` | Abort before emitting anything if a run finds more new events |
| `include_request_parameters` | `False` | Also render each call's request parameters |
| `profile_name` | `None` | Named AWS profile instead of the default credential chain |
| `resource_name` | `cloudtrail_events` | dlt table name; use one per account if several share a dataset |

## Setup

No trail is needed: the connector reads CloudTrail's free 90-day event history. Credentials come
from the standard boto3 chain: environment variables, `AWS_PROFILE` / shared config, or an
instance/task role. If you sign in with `aws login`, botocore also needs its CRT extra:
`pip install "botocore[crt]"`. The connector only needs this read-only policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["cloudtrail:LookupEvents"],
      "Resource": "*"
    }
  ]
}
```

Set `LLM_API_KEY` as for any cognee run.

## How sync and forgetting work

- **Write events only.** Lookups filter on `ReadOnly=false`, so `Describe*`, `Get*` and `List*`
  calls are skipped. When `event_names` is set, CloudTrail filters on each name (it accepts only
  one lookup attribute per call) and read-only events are dropped by the connector.
- **Incremental.** A cursor per region is kept in dlt state. Each run reads from the previous
  cursor minus 15 minutes, because CloudTrail can deliver events a few minutes late. Event ids
  already synced are skipped, so the overlap never duplicates a document.
- **Retention, not deletion.** CloudTrail never deletes an event; it ages out of the 90-day event
  history. Synced events older than `retention_days` are emitted with the `_deleted` hard-delete
  marker, dlt removes the row on `merge`, and cognee's orphan cleanup removes the document. Lower
  `retention_days` to forget sooner.
- **Rate limit.** `LookupEvents` allows two requests per second per account per region. The
  connector spaces its calls at least half a second apart and uses botocore's adaptive retries.
- **Safe failure.** State is only saved when a run finishes, so a failed run is retried in full.

## Privacy

- The access key id of each call is never rendered.
- Request parameters are off by default. They show what was changed, but some APIs include
  values you may not want in memory. Turn them on with `include_request_parameters=True`.
- Usernames, role ARNs and source IP addresses are ingested. Limit who can query the dataset
  accordingly.

## Testing

```bash
uv run pytest tests/
```

moto does not implement `LookupEvents`, so the tests use a small in-memory fake client, plus
botocore's `Stubber` to check that every request the connector sends matches the real
CloudTrail API model. They cover rendering, the read-only filter, the incremental cursor and its
overlap, retention-based forgetting, call pacing, the size guard, and a full run through
cognee's `ingest_dlt_source`.
