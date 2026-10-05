"""DynamoDB connector demo — ask questions about items you choose to sync.

Syncs one DynamoDB table into cognee, then asks a question about it. Re-run
after editing or deleting an item: only the changed item is re-ingested, and a
deleted item is forgotten.

Setup
-----
1. Install:  pip install cognee-community-connector-dynamodb
2. AWS credentials allowed to call dynamodb:DescribeTable and dynamodb:Scan
   on the table (see README). Any method boto3 understands works:
   ``aws configure``, ``AWS_PROFILE``, or environment variables.
3. Export your LLM key, region, table and text attribute, then run:

       export LLM_API_KEY="sk-..."
       export AWS_REGION="ap-south-1"
       export DYNAMODB_TABLE="app-config"
       export DYNAMODB_TEXT_ATTRIBUTE="body"
       python examples/example.py
"""

import asyncio
import os

import cognee

from cognee_community_connector_dynamodb import DynamoDBTable, dynamodb_source

DATASET_NAME = "app_data"


async def main() -> None:
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    table = os.environ.get("DYNAMODB_TABLE")
    text_attribute = os.environ.get("DYNAMODB_TEXT_ATTRIBUTE")
    if not (region and table and text_attribute):
        print("Set AWS_REGION, DYNAMODB_TABLE and DYNAMODB_TEXT_ATTRIBUTE to run this example.")
        return

    print(f"Syncing items from DynamoDB table {table} ({region}) into cognee ...")
    await cognee.remember(
        dynamodb_source(
            tables=[DynamoDBTable(table, text_attribute=text_attribute)],
            region=region,
        ),
        dataset_name=DATASET_NAME,
        primary_key="id",
        write_disposition="merge",
    )

    answer = await cognee.search(
        query_text=f"Summarise what the items in the {table} table say.",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("\nSearch result:\n", answer)


if __name__ == "__main__":
    asyncio.run(main())
