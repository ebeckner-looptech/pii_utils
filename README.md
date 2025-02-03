# Comprehensive PII Processing System (MVP1 & MVP2)

## Overview

This documentation combines two modular Python components designed for cloud-deployed assistant systems:

- **MVP1: Redaction-Only Processing System (`pii_redact.py`)**
- **MVP2: PII Obfuscation Utility (`pii_obfuscate.py`)**

Both components leverage Azure AI Language for PII detection and Azure Cosmos DB for secure storage and processing.

### MVP1: Redaction-Only Processing System

MVP1 is an asynchronous batch-based system that processes messages from a source Cosmos DB container, detects Personally Identifiable Information (PII) using Azure AI Language's PII detection service, and produces a redacted version of each message. It operates in two modes:

- **Cloud Mode**: Redacted messages are stored in a dedicated Cosmos DB container.
- **Local Mode**: Redacted messages are stored locally (in a JSON file), while metadata for any failed messages is logged in a CSV file.

### MVP2: PII Obfuscation Utility

MVP2 is a modular component that provides **reversible** tokenization (obfuscation) of PII. It replaces detected PII with unique tokens (e.g., `{name_1}`), allowing downstream systems to process messages with obfuscated sensitive data, while still enabling the original information to be restored when needed. Token mappings are stored securely in Azure Cosmos DB.

## Features

### MVP1: Redaction-Only Processing
- **Batch and Asynchronous Processing**: Processes messages in batches (up to 5 per request).
- **Progress Tracking**: Displays progress using `tqdm`.
- **Resumability**: Uses a processing ledger to track progress.
- **Command-Line Configuration**: Supports runtime configuration.
- **Error Handling**: Logs failed message metadata to a CSV file.

### MVP2: PII Obfuscation Utility
- **PII Detection and Obfuscation**: Replaces PII with structured tokens.
- **Reversible Tokenization**: Enables deobfuscation using Cosmos DB.
- **Cloud-Based Storage**: Stores token mappings for consistent anonymization.
- **Modular Design**: Easily integrates into cloud-based assistant systems.

## Prerequisites

- **Python:** 3.7 or newer
- **Dependencies:**
  - `azure-ai-textanalytics`
  - `azure-cosmos`
  - `tqdm`
  - `argparse`

Install dependencies using pip:

```bash
pip install azure-ai-textanalytics azure-cosmos tqdm argparse
```

## Configuration

### MVP1 Configuration

You can configure the following parameters via command-line arguments or by editing the default values in the script:

#### Service Tier (`--tier`):  
Determines the maximum number of concurrent batches (e.g., choices are S, S0, or F0; default is S0).

#### Batch Size (`--batch-size`):  
Number of messages per API call (maximum 5 for PII detection). Default is 5.

#### Cloud Mode (`--cloud-mode`):  
If specified, redacted messages are stored in the Cosmos DB output container. Otherwise, redacted messages are stored locally.

#### Local Output Files:

- **Redacted Output File (`--redacted-output-file`)**:  
  Path to the JSON file where redacted messages will be stored locally (default: `redacted_messages.json`).
- **Failed Output File (`--failed-output-file`)**:  
  Path to the CSV file where metadata for failed messages will be stored (default: `failed_messages_ledger.csv`).

#### Endpoint Overrides:
You may override the default endpoints and keys for Azure Text Analytics and Cosmos DB using:

- `--text-analytics-endpoint` and `--text-analytics-key`
- `--cosmos-db-endpoint` and `--cosmos-db-key`

#### Verbose Logging (`--verbose`):  
Enable detailed logging for debugging purposes.

### MVP2 Configuration

Both utilities require Azure credentials and settings:

- **Cosmos DB:** Endpoint, key, database, and container name
- **Azure Text Analytics:** Endpoint and key

You can provide these values through environment variables or pass them directly in the script.

## Usage Examples

### MVP1: Redact Messages in Cloud Mode

```bash
python pii_redact.py --cloud-mode --tier S0 --batch-size 5
```

### MVP1: Redact Messages Locally

```bash
python pii_redact.py --tier S0 --batch-size 5 --redacted-output-file "local_redacted.json" --failed-output-file "local_failed.csv"
```

### MVP2: Obfuscate and Deobfuscate Messages

```python
import asyncio
from pii_obfuscate import PIIObfuscationUtility

async def example_run():
    utility = PIIObfuscationUtility(
        cosmos_db_endpoint="https://<your-cosmos-db>.documents.azure.com:443/",
        cosmos_db_key="<your-key>",
        text_analytics_endpoint="https://<your-text-analytics>.cognitiveservices.azure.com/",
        text_analytics_key="<your-key>",
        database_name="YourDatabase",
        token_mapping_container_name="TokenMapping"
    )
    
    await utility.init_clients()
    
    sample_message = "John Doe lives at 123 Maple Street. His SSN is 123-45-6789."
    user_id = "user_001"
    conversation_id = "conv_001"
    
    obfuscated = await utility.obfuscate_message_content(sample_message, user_id, conversation_id)
    print("Obfuscated:", obfuscated)
    
    deobfuscated = await utility.deobfuscate_message_content(obfuscated, user_id, conversation_id)
    print("Deobfuscated:", deobfuscated)
    
    await utility.close()

if __name__ == "__main__":
    asyncio.run(example_run())
```

## Integration & Deployment

Both utilities can be integrated into a cloud-based assistant system as:

- **MVP1**: Preprocessing pipeline for redacting PII before storing messages.
- **MVP2**: Tokenization module for reversible obfuscation before feeding data into an LLM.

## Contributing

Contributions, bug reports, and feature suggestions are welcome! Follow organizational guidelines when submitting changes.

## License

This project is provided "as is" with no warranty. Use at your own risk.

## Contact

For support or inquiries, reach out to [Your Team] at [Your Contact Information].

