package com.subaru.tele.vhs.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author gauraaga
 */
public class LambdaSQSHandler implements RequestHandler<SQSEvent, String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        context.getLogger().log("Context Logger "+ "LambdaSQSHandler");

        List<SQSEvent.SQSMessage> records = event.getRecords();

        for (SQSEvent.SQSMessage msg : records) {
            String body = msg.getBody();
            context.getLogger().log("📩 Received message: " + body);

            try {
                JsonNode jsonNode = objectMapper.readTree(body);
                if (!jsonNode.has("id")) {
                    context.getLogger().log("❌ Invalid message format — missing 'id' field");
                    throw new RuntimeException("Invalid message");
                }


                String idValue = jsonNode.get("id").asText();
                String nameValue = jsonNode.has("name") ? jsonNode.get("name").asText() : null;

                Map<String, AttributeValue> item = new HashMap<>();
                item.put("id", new AttributeValue(idValue));
                if (nameValue != null && !nameValue.isEmpty()) {
                    item.put("name", new AttributeValue(nameValue));
                }
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                context.getLogger().log("❌ Invalid JSON format: " + e.getMessage());
                throw new RuntimeException("Invalid JSON format", e);

            } catch (Exception e) {
                context.getLogger().log("⚠️ Retriable error occurred: " + e.getMessage());
                throw new RuntimeException(e);
            }


        return "";
    }
}

