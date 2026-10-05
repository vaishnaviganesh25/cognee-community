"""CloudFormation connector demo — ask questions about your AWS stacks.

Syncs the CloudFormation stacks in one region into cognee, then asks a question
about them. Re-run after updating or deleting a stack: only the changed stack
is re-described, and a deleted stack is forgotten.

Setup
-----
1. Install:  pip install cognee-community-connector-cloudformation
2. AWS credentials with read-only CloudFormation access (see README for the
   IAM policy). Any method boto3 understands works: ``aws configure``,
   ``AWS_PROFILE``, or environment variables.
3. Export your LLM key and region, then run:

       export LLM_API_KEY="sk-..."
       export AWS_REGION="ap-south-1"
       python examples/example.py
"""

import asyncio
import os

import cognee

from cognee_community_connector_cloudformation import cloudformation_source

DATASET_NAME = "aws_stacks"


async def main() -> None:
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        print("Set AWS_REGION (and AWS credentials) to run this example.")
        return

    print(f"Syncing CloudFormation stacks from {region} into cognee ...")
    await cognee.remember(
        cloudformation_source(regions=[region]),
        dataset_name=DATASET_NAME,
        primary_key="id",
        write_disposition="merge",
    )

    answer = await cognee.search(
        query_text="Which stacks exist, what do they deploy, and did any of them fail recently?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("\nSearch result:\n", answer)


if __name__ == "__main__":
    asyncio.run(main())
