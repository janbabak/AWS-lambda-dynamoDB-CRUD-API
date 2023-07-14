package com.janbabak.lambda.api;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.janbabak.lambda.model.Product;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;

// handler com.janbabak.lambda.api.ProductLambdaHandler::handleRequest

public class ProductLambdaHandler implements RequestStreamHandler {

    private String DYNAMO_TABLE = "Products";
    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        JSONParser parser = new JSONParser();
        JSONObject responseObject = new JSONObject();
        JSONObject responseBody = new JSONObject();

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(client);

        int id;
        Item resItem = null;
        try {
            JSONObject reqObject = (JSONObject) parser.parse(reader);

            // path parameters
            if (reqObject.get("pathParameters") != null) {
                JSONObject pps = (JSONObject) reqObject.get("pathParameters");
                if (pps.get("id") != null) {
                    id = Integer.parseInt((String) pps.get("id"));
                    resItem = dynamoDB.getTable(DYNAMO_TABLE).getItem("id", id);
                }
            }
            // query string parameters
            else if (reqObject.get("queryStringParameters") != null) {
                JSONObject qps = (JSONObject) reqObject.get("queryStringParameters");
                if (qps.get("id") != null) {
                    id = Integer.parseInt((String) qps.get("id"));
                    resItem = dynamoDB.getTable(DYNAMO_TABLE).getItem("id", id);
                }
            }

            if (resItem != null) {
                Product product = new Product(resItem.toJSON());
                responseBody.put("product", product);
                responseBody.put("statusCode", 200);
            } else {
                responseBody.put("message", "No items found");
                responseBody.put("statusCode", 404);
            }

            responseObject.put("body", responseBody.toString());
        } catch (ParseException e) {
            context.getLogger().log("ERROR: " + e.getMessage());
        }

        writer.write(responseObject.toJSONString());
        reader.close();
        writer.close();
    }
}
