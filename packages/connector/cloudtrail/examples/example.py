"""CloudTrail connector demo — ask who changed what in your AWS account.

Syncs the last week of write-type management events in one region into
cognee, then asks a question about them. Re-run later: only new events are
ingested, and events older than the retention window are forgotten.

Setup
-----
1. Install:  pip install cognee-community-connector-cloudtrail
2. AWS credentials allowed to call cloudtrail:LookupEvents (see README).
   Any method boto3 understands works: ``aws configure``, ``AWS_PROFILE``,
   or environment variables.
3. Export your LLM key and region, then run:

       export LLM_API_KEY="sk-..."
       export AWS_REGION="ap-south-1"
       python examples/example.py
"""

import asyncio
import os

import cognee

from cognee_community_connector_cloudtrail import cloudtrail_source

DATASET_NAME = "aws_audit"


async def main() -> None:
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        print("Set AWS_REGION (and AWS credentials) to run this example.")
        return

    print(f"Syncing the last 7 days of CloudTrail write events from {region} into cognee ...")
    await cognee.remember(
        cloudtrail_source(regions=[region], lookback_days=7),
        dataset_name=DATASET_NAME,
        primary_key="id",
        write_disposition="merge",
    )

    answer = await cognee.search(
        query_text="Who made changes in this account this week, and did any of them fail?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("\nSearch result:\n", answer)


if __name__ == "__main__":
    asyncio.run(main())
