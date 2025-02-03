#!/usr/bin/env python3
import asyncio
import logging
import re
import csv
import math
import json
import argparse
from datetime import datetime
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics.aio import TextAnalyticsClient
from azure.cosmos.aio import CosmosClient
from tqdm import tqdm

# ---------------------------
# Default Cloud Endpoints & Keys
# ---------------------------
DEFAULT_TEXT_ANALYTICS_ENDPOINT = "https://<your-text-analytics-resource>.cognitiveservices.azure.com/"
DEFAULT_TEXT_ANALYTICS_KEY = "<your-text-analytics-key>"
DEFAULT_COSMOS_DB_ENDPOINT = "https://<your-cosmos-db-account>.documents.azure.com:443/"
DEFAULT_COSMOS_DB_KEY = "<your-cosmos-db-key>"

# ---------------------------
# Cosmos DB Database and Container Names
# ---------------------------
DATABASE_NAME = "YourDatabaseName"
MESSAGES_CONTAINER_NAME = "Messages"             # Source messages container (raw data, unchanged)
LEDGER_CONTAINER_NAME = "ProcessingLedger"         # Processing ledger container (tracks processed messages)
OUTPUT_CONTAINER_NAME = "CleanedMessages"          # Destination container for redacted messages

# ---------------------------
# Default API and Tier Settings
# ---------------------------
DEFAULT_BATCH_SIZE = 5  # Maximum for PII detection API (per request)
TIER_CONCURRENCY_LIMITS = {
    "S": 20,
    "S0": 5,
    "F0": 5
}

# ---------------------------
# Logging Configuration
# ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MVP1_Redaction")

# ---------------------------
# Utility Functions
# ---------------------------
def redact_entity(entity_text: str, entity_category: str, confidence: float) -> str:
    """
    Return a redaction placeholder containing the entity category and confidence.
    E.g., "[REDACTED - Name (0.97)]"
    """
    return f"[REDACTED - {entity_category} ({confidence:.2f})]"

# ---------------------------
# Cosmos DB Helper Functions
# ---------------------------
async def update_cosmos_container(container, items):
    tasks = [container.upsert_item(item) for item in items]
    await asyncio.gather(*tasks)

async def log_processed_messages(ledger_container, processed_messages):
    """
    Log processed messages in the ledger container using a composite key of conversationId and messageId.
    """
    timestamp = datetime.utcnow().isoformat()
    tasks = []
    for msg in processed_messages:
        record = {
            "id": f"{msg['conversationId']}_{msg['id']}",
            "conversationId": msg['conversationId'],
            "messageId": msg['id'],
            "processedAt": timestamp
        }
        tasks.append(ledger_container.upsert_item(record))
    await asyncio.gather(*tasks)

def write_local_redacted(redacted_messages, output_file):
    """
    Write the redacted messages (as JSON) to the local file specified.
    """
    try:
        with open(output_file, mode='w', encoding='utf-8') as f:
            json.dump(redacted_messages, f, indent=2)
        logger.info(f"Redacted messages written to local file: {output_file}")
    except Exception as e:
        logger.error(f"Failed to write redacted messages locally: {e}")

def write_failed_metadata(failed_messages, failed_file):
    """
    Write metadata for failed messages to a CSV file.
    """
    try:
        with open(failed_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["conversationId", "messageId", "errorTimestamp"])
            now = datetime.utcnow().isoformat()
            for msg in failed_messages:
                writer.writerow([msg.get("conversationId"), msg.get("id"), now])
        logger.info(f"Failed message metadata written to CSV: {failed_file}")
    except Exception as e:
        logger.error(f"Failed to write failed messages metadata: {e}")

# ---------------------------
# Query and Filtering Functions
# ---------------------------
async def query_all_messages(messages_container):
    """
    Query all messages from the source container.
    """
    query = "SELECT * FROM c"
    messages_iterator = messages_container.query_items(query, enable_cross_partition_query=True)
    messages = [msg async for msg in messages_iterator]
    return messages

async def get_processed_message_ids(ledger_container):
    """
    Retrieve the set of processed message composite keys (conversationId_messageId) from the ledger.
    """
    processed_ids = set()
    query = "SELECT c.conversationId, c.messageId FROM c"
    async for record in ledger_container.query_items(query, enable_cross_partition_query=True):
        key = f"{record.get('conversationId')}_{record.get('messageId')}"
        processed_ids.add(key)
    return processed_ids

async def filter_unprocessed_messages(messages, ledger_container):
    """
    Given a list of messages, filter out those that have already been processed by consulting the ledger.
    """
    processed_ids = await get_processed_message_ids(ledger_container)
    unprocessed = []
    for msg in messages:
        key = f"{msg.get('conversationId')}_{msg.get('id')}"
        if key not in processed_ids:
            unprocessed.append(msg)
    return unprocessed

async def display_overall_progress(messages_container, ledger_container):
    """
    Display overall progress by comparing the total messages to the number of processed messages (from the ledger).
    """
    total_query = "SELECT VALUE COUNT(1) FROM c"
    async for count in messages_container.query_items(total_query, enable_cross_partition_query=True):
        total_items = count
    processed_ids = await get_processed_message_ids(ledger_container)
    processed_items = len(processed_ids)
    remaining = total_items - processed_items
    percentage = (processed_items / total_items * 100) if total_items > 0 else 0
    logger.info(f"Overall Progress: {processed_items}/{total_items} messages processed ({percentage:.2f}%). {remaining} remaining.")
    return total_items, processed_items, remaining

# ---------------------------
# Asynchronous Batch Processing Functions
# ---------------------------
async def process_batch(batch_messages, ta_client, semaphore):
    """
    Process one batch (up to DEFAULT_BATCH_SIZE messages) asynchronously.
    Returns a tuple: (successful_messages, failed_messages)
    """
    async with semaphore:
        documents = [msg["content"] for msg in batch_messages]
        try:
            response = await ta_client.recognize_pii_entities_batch(documents, language="en")
        except Exception as ex:
            logger.error(f"Batch API call failed: {ex}")
            return [], batch_messages  # Entire batch failed

    successful = []
    failed = []
    for msg, result in zip(batch_messages, response):
        if result.is_error:
            logger.error(f"Error processing message {msg.get('id')}: {result.error}")
            failed.append(msg)
        else:
            processed_text = msg["content"]
            for entity in result.entities:
                replacement = redact_entity(entity.text, entity.category, entity.confidence_score)
                processed_text = processed_text.replace(entity.text, replacement)
            msg["processed_content"] = processed_text
            msg["pii_entities"] = [
                {"text": entity.text, "category": entity.category, "confidence": entity.confidence_score}
                for entity in result.entities
            ]
            successful.append(msg)
    return successful, failed

async def process_all_batches(messages, ta_client, max_concurrent_batches, batch_size):
    """
    Split messages into batches and process them concurrently with a progress bar.
    Returns (all_successful_messages, all_failed_messages)
    """
    semaphore = asyncio.Semaphore(max_concurrent_batches)
    tasks = []
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i+batch_size]
        tasks.append(process_batch(batch, ta_client, semaphore))
    successful_all = []
    failed_all = []
    progress_bar = tqdm(total=len(tasks), desc="Processing Batches", unit="batch")
    for future in asyncio.as_completed(tasks):
        try:
            success, failed = await future
            successful_all.extend(success)
            failed_all.extend(failed)
        except Exception as e:
            logger.error(f"Unexpected error in batch processing: {e}")
        progress_bar.update(1)
    progress_bar.close()
    return successful_all, failed_all

# ---------------------------
# MVP1: Redaction-Only Processing with Cloud/Local Options
# ---------------------------
async def mvp1_redaction(args):
    tier = args.tier
    batch_size = args.batch_size
    max_concurrent_batches = TIER_CONCURRENCY_LIMITS.get(tier, 5)
    logger.info(f"Tier: '{tier}' with up to {max_concurrent_batches} concurrent batches and batch size {batch_size}.")

    # Determine whether to run in cloud mode.
    cloud_mode = args.cloud_mode

    # Initialize Cosmos and Text Analytics clients.
    cosmos_db_endpoint = args.cosmos_db_endpoint or DEFAULT_COSMOS_DB_ENDPOINT
    cosmos_db_key = args.cosmos_db_key or DEFAULT_COSMOS_DB_KEY
    ta_endpoint = args.text_analytics_endpoint or DEFAULT_TEXT_ANALYTICS_ENDPOINT
    ta_key = args.text_analytics_key or DEFAULT_TEXT_ANALYTICS_KEY

    cosmos_client = CosmosClient(cosmos_db_endpoint, cosmos_db_key)
    ta_client = TextAnalyticsClient(
        endpoint=ta_endpoint,
        credential=AzureKeyCredential(ta_key)
    )

    # Get Cosmos DB containers.
    database = cosmos_client.get_database_client(DATABASE_NAME)
    messages_container = database.get_container_client(MESSAGES_CONTAINER_NAME)
    ledger_container = database.get_container_client(LEDGER_CONTAINER_NAME)
    output_container = None
    if cloud_mode:
        output_container = database.get_container_client(OUTPUT_CONTAINER_NAME)

    # Display overall progress before starting.
    await display_overall_progress(messages_container, ledger_container)

    # Query all messages from the source.
    all_messages = await query_all_messages(messages_container)
    # Filter out already processed messages using the ledger.
    messages_to_process = await filter_unprocessed_messages(all_messages, ledger_container)
    if not messages_to_process:
        logger.info("No unprocessed messages found. Exiting.")
        await ta_client.close()
        await cosmos_client.close()
        return

    logger.info(f"{len(messages_to_process)} unprocessed messages found.")
    successful_msgs, failed_msgs = await process_all_batches(messages_to_process, ta_client, max_concurrent_batches, batch_size)
    logger.info(f"Batch processing complete: {len(successful_msgs)} messages processed successfully; {len(failed_msgs)} failed in this run.")

    # Output results:
    if cloud_mode:
        # In cloud mode, upsert redacted messages into the output container and update ledger.
        if successful_msgs:
            await update_cosmos_container(output_container, successful_msgs)
            await log_processed_messages(ledger_container, successful_msgs)
        logger.info("Redacted messages stored in cloud (Cosmos DB).")
    else:
        # In local mode, write redacted messages to a local JSON file.
        if successful_msgs:
            write_local_redacted(successful_msgs, args.redacted_output_file)
        logger.info("Redacted messages stored locally.")

    # Always write failed message metadata locally.
    if failed_msgs:
        write_failed_metadata(failed_msgs, args.failed_output_file)
        logger.error(f"{len(failed_msgs)} messages failed to process. Re-run the script to resume processing.")
    else:
        logger.info("No failed messages.")

    await ta_client.close()
    await cosmos_client.close()
    await display_overall_progress(messages_container, ledger_container)
    logger.info("Processing complete.")

# ---------------------------
# Main Entry Point with Argparse
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="MVP1 Redaction-Only Processing Script (Ledger-Only Tracking)")
    parser.add_argument("--tier", type=str, default="S0", choices=["S", "S0", "F0"],
                        help="Azure service tier (affects throughput throttling)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help="Batch size for PII detection API calls (max 5)")
    parser.add_argument("--cloud-mode", action="store_true",
                        help="If set, store redacted messages in Cosmos DB; otherwise, store locally")
    parser.add_argument("--redacted-output-file", type=str, default="redacted_messages.json",
                        help="Local file path to store redacted messages (if not in cloud mode)")
    parser.add_argument("--failed-output-file", type=str, default="failed_messages_ledger.csv",
                        help="Local CSV file to store metadata for failed messages")
    parser.add_argument("--text-analytics-endpoint", type=str, default="",
                        help="Override the default Azure Text Analytics endpoint")
    parser.add_argument("--text-analytics-key", type=str, default="",
                        help="Override the default Azure Text Analytics key")
    parser.add_argument("--cosmos-db-endpoint", type=str, default="",
                        help="Override the default Cosmos DB endpoint")
    parser.add_argument("--cosmos-db-key", type=str, default="",
                        help="Override the default Cosmos DB key")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    try:
        asyncio.run(mvp1_redaction(args))
    except Exception as e:
        logger.error(f"Run failed with error: {e}")
        print("Run failed. Please check the logs above and re-run the script to resume processing.")

if __name__ == "__main__":
    main()
