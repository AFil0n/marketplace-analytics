package ru.practicum.common.utils;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;

import java.io.IOException;

public class SchemaRegistryHelper {
    public static void registerSchema(SchemaRegistryClient client, String subject, String schemaString) {
        try {
            System.out.println("🔄 Registering schema for: " + subject);
            ParsedSchema schema = new JsonSchema(schemaString);
            int schemaId = client.register(subject, schema); // ← ЭТА СТРОКА ДОЛЖНА БЫТЬ!
            System.out.println("✅ ✅ Schema registered for " + subject + " with ID: " + schemaId);
        } catch (Exception e) {
            System.err.println("❌ Failed to register schema for " + subject + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
