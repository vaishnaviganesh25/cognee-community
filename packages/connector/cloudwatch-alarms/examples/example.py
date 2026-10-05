"""CloudWatch Alarms connector demo — ask questions about your alarms.

Syncs the CloudWatch alarms in one region into cognee, then asks a question
about them. Re-run after an alarm changes state or is deleted: only the changed
alarm is re-ingested, and a deleted alarm is forgotten.

Setup
-----
1. Install:  pip install cognee-community-connector-cloudwatch-alarms
2. AWS credentials with read-only CloudWatch access (see README for the IAM
   policy). Any method boto3 understands works: ``aws configure``,
   ``AWS_PROFILE``, or environment variables.
3. Export your LLM key and region, then run:

       export LLM_API_KEY="sk-..."
       export AWS_REGION="ap-south-1"
       python examples/example.py
"""

import asyncio
import os

import cognee

from cognee_community_connector_cloudwatch_alarms import cloudwatch_alarms_source

DATASET_NAME = "aws_alarms"


async def main() -> None:
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        print("Set AWS_REGION (and AWS credentials) to run this example.")
        return

    print(f"Syncing CloudWatch alarms from {region} into cognee ...")
    await cognee.remember(
        cloudwatch_alarms_source(regions=[region]),
        dataset_name=DATASET_NAME,
        primary_key="id",
        write_disposition="merge",
    )

    answer = await cognee.search(
        query_text="Which alarms are in ALARM state, what do they watch, and when did they fire?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("\nSearch result:\n", answer)


if __name__ == "__main__":
    asyncio.run(main())
