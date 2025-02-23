import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";

const dynamoDBClient = new DynamoDBClient({ region: 'us-east-1' });

export const handler = async (event) => {
    try {
        for (const record of event.Records) {
            // Decode and parse the Kinesis data
            const payload = Buffer.from(record.kinesis.data, 'base64').toString('utf-8');
            const contactTraceRecord = JSON.parse(payload);
            console.log("JSON Payload:", contactTraceRecord);

            // Prepare the DynamoDB item with defaults for missing fields
            const dynamoItem = {};

            // Iterate over all keys in the record and map them to DynamoDB attributes
            for (const [key, value] of Object.entries(contactTraceRecord)) {
                if (value === null || value === undefined) {
                    // Set an empty string for null/undefined values
                    dynamoItem[key] = { S: '' };
                } else if (typeof value === 'object' && !Array.isArray(value)) {
                    // Convert objects to JSON strings
                    dynamoItem[key] = { S: JSON.stringify(value) };
                } else if (Array.isArray(value)) {
                    // Convert arrays to JSON strings
                    dynamoItem[key] = { S: JSON.stringify(value) };
                } else if (typeof value === 'number') {
                    // Convert numbers to DynamoDB Number type
                    dynamoItem[key] = { N: String(value) };
                } else {
                    // Handle strings and other types as DynamoDB String type
                    dynamoItem[key] = { S: String(value) };
                }
            }

            // Define the PutItemCommand parameters
            const params = {
                TableName: 'DI-211SSO-CTR_Table',
                Item: dynamoItem,
            };

            // Write the record to DynamoDB
            console.log("Writing to Dynamo: ", JSON.stringify(params));
            await dynamoDBClient.send(new PutItemCommand(params));
        }

        return {
            statusCode: 200,
            body: 'Records processed successfully.',
        };
    } catch (error) {
        console.error('Error processing Kinesis records:', error);
        return {
            statusCode: 500,
            body: 'Error processing records.',
        };
    }
};
