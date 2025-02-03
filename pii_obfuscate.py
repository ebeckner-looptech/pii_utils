import asyncio
import difflib
import re
from datetime import datetime
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics.aio import TextAnalyticsClient
from azure.cosmos.aio import CosmosClient

class PIIObfuscationUtility:
    """
    A utility class that encapsulates methods for obfuscating (tokenizing)
    and deobfuscating message content based on PII detection, using Azure AI Language
    and Cosmos DB for persistent token mapping.
    
    All containers (for token mapping, messages, etc.) are cloud‐based.
    """
    
    def __init__(self, cosmos_db_endpoint: str, cosmos_db_key: str,
                 text_analytics_endpoint: str, text_analytics_key: str,
                 database_name: str, token_mapping_container_name: str):
        self.cosmos_db_endpoint = cosmos_db_endpoint
        self.cosmos_db_key = cosmos_db_key
        self.text_analytics_endpoint = text_analytics_endpoint
        self.text_analytics_key = text_analytics_key
        self.database_name = database_name
        self.token_mapping_container_name = token_mapping_container_name
        self.cosmos_client = None
        self.ta_client = None
        self.database = None
        self.mapping_container = None

    async def init_clients(self):
        """Initialize the Cosmos DB and Text Analytics clients and obtain the mapping container."""
        self.cosmos_client = CosmosClient(self.cosmos_db_endpoint, self.cosmos_db_key)
        self.ta_client = TextAnalyticsClient(
            endpoint=self.text_analytics_endpoint,
            credential=AzureKeyCredential(self.text_analytics_key)
        )
        self.database = self.cosmos_client.get_database_client(self.database_name)
        self.mapping_container = self.database.get_container_client(self.token_mapping_container_name)

    def normalize_entity(self, entity_value: str) -> str:
        """
        Normalize an entity string by lowercasing and removing punctuation/whitespace.
        This helps with fuzzy matching.
        """
        norm = entity_value.lower().strip()
        norm = re.sub(r'[\W_]+', '', norm)
        return norm

    async def get_existing_token(self, user_id: str, conversation_id: str,
                                 entity_category: str, norm_value: str) -> str:
        """
        Query the token mapping container for an existing token that matches (fuzzily)
        the normalized value for a given user, conversation, and entity category.
        """
        query = """
            SELECT * FROM c 
            WHERE c.userId = @user_id 
              AND c.conversationId = @conversation_id 
              AND c.entityCategory = @entity_category
        """
        parameters = [
            {"name": "@user_id", "value": user_id},
            {"name": "@conversation_id", "value": conversation_id},
            {"name": "@entity_category", "value": entity_category}
        ]
        items = self.mapping_container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        )
        async for item in items:
            existing_norm = item.get("normalizedValue")
            similarity = difflib.SequenceMatcher(None, existing_norm, norm_value).ratio()
            if similarity > 0.8:
                return item["token"]
        return None

    async def create_token(self, user_id: str, conversation_id: str,
                           entity_category: str, norm_value: str,
                           original_value: str) -> str:
        """
        Create a new token for the given entity and store it in the token mapping container.
        The token is generated with contextual naming (e.g. {name_1}) and the original value is stored
        to permit deobfuscation later.
        """
        # Count existing tokens for this user/conversation/category.
        query = """
            SELECT VALUE COUNT(1) FROM c 
            WHERE c.userId = @user_id 
              AND c.conversationId = @conversation_id 
              AND c.entityCategory = @entity_category
        """
        parameters = [
            {"name": "@user_id", "value": user_id},
            {"name": "@conversation_id", "value": conversation_id},
            {"name": "@entity_category", "value": entity_category}
        ]
        count_iter = self.mapping_container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        )
        count = 0
        async for cnt in count_iter:
            count = cnt
        token = f"{{{entity_category.lower()}_{count + 1}}}"  # E.g., {name_1}
        token_record = {
            "id": f"{user_id}_{conversation_id}_{entity_category}_{count + 1}",
            "userId": user_id,
            "conversationId": conversation_id,
            "entityCategory": entity_category,
            "normalizedValue": norm_value,
            "originalValue": original_value,  # Save the original text for deobfuscation.
            "token": token,
            "createdAt": datetime.utcnow().isoformat()
        }
        await self.mapping_container.create_item(token_record)
        return token

    async def get_or_create_token(self, user_id: str, conversation_id: str,
                                  entity_category: str, entity_value: str) -> str:
        """
        Retrieve an existing token for the entity (using fuzzy matching) or create a new one if needed.
        """
        norm_value = self.normalize_entity(entity_value)
        existing_token = await self.get_existing_token(user_id, conversation_id, entity_category, norm_value)
        if existing_token:
            return existing_token
        return await self.create_token(user_id, conversation_id, entity_category, norm_value, entity_value)

    async def obfuscate_message_content(self, message: str, user_id: str,
                                        conversation_id: str, language: str = "en") -> str:
        """
        Process the provided message string by detecting PII using Azure AI Language
        and replacing each detected entity with a token. Returns the obfuscated message.
        
        Inputs:
          - message: Raw text that may contain PII.
          - user_id: Identifier for the user.
          - conversation_id: Identifier for the conversation.
          - language: (Optional) The language of the text (default "en").
          
        Output:
          - A string in which each detected PII entity is replaced by a token (e.g., "{name_1}").
        """
        response = await self.ta_client.recognize_pii_entities(message, language=language)
        if response.is_error:
            raise Exception(f"PII detection error: {response.error}")
        obfuscated = message
        for entity in response.entities:
            token = await self.get_or_create_token(user_id, conversation_id, entity.category, entity.text)
            obfuscated = obfuscated.replace(entity.text, token)
        return obfuscated

    async def deobfuscate_message_content(self, obfuscated_message: str, user_id: str,
                                          conversation_id: str) -> str:
        """
        Replace tokens in the obfuscated message with the original PII text using the token mapping.
        
        Inputs:
          - obfuscated_message: Message containing tokens (e.g., "{name_1}").
          - user_id: Identifier for the user.
          - conversation_id: Identifier for the conversation.
          
        Output:
          - A string with tokens replaced by the original text.
        """
        query = """
            SELECT * FROM c 
            WHERE c.userId = @user_id 
              AND c.conversationId = @conversation_id
        """
        parameters = [
            {"name": "@user_id", "value": user_id},
            {"name": "@conversation_id", "value": conversation_id}
        ]
        original_message = obfuscated_message
        async for record in self.mapping_container.query_items(query=query, parameters=parameters, enable_cross_partition_query=True):
            token = record["token"]
            original_value = record["originalValue"]
            original_message = original_message.replace(token, original_value)
        return original_message

    async def close(self):
        """Cleanly close the Azure clients."""
        await self.ta_client.close()
        await self.cosmos_client.close()


# ---------------------------
# Example Usage
# ---------------------------
async def example_run():
    # Initialize the utility with your endpoints, keys, and configuration.
    utility = PIIObfuscationUtility(
        cosmos_db_endpoint="https://<your-cosmos-db-account>.documents.azure.com:443/",
        cosmos_db_key="<your-cosmos-db-key>",
        text_analytics_endpoint="https://<your-text-analytics-resource>.cognitiveservices.azure.com/",
        text_analytics_key="<your-text-analytics-key>",
        database_name="YourDatabaseName",
        token_mapping_container_name="TokenMapping"
    )
    await utility.init_clients()
    
    # Example message and identifiers.
    sample_message = "John Doe lives at 123 Maple Street. His SSN is 123-45-6789."
    user_id = "user_001"
    conversation_id = "conv_001"
    
    # Obfuscate the message.
    obfuscated = await utility.obfuscate_message_content(sample_message, user_id, conversation_id)
    print("Obfuscated Message:")
    print(obfuscated)
    
    # Deobfuscate the message.
    deobfuscated = await utility.deobfuscate_message_content(obfuscated, user_id, conversation_id)
    print("\nDeobfuscated Message:")
    print(deobfuscated)
    
    await utility.close()

if __name__ == "__main__":
    asyncio.run(example_run())
