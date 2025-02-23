//this Lambda will generate a CTR from a Kenisis event and then place it in an S3 Bucket for storing 

import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

// Initialize the S3 client (ensure the region matches your bucket's region)
const s3Client = new S3Client({ region: 'us-east-1' });

// Set the S3 bucket name as provided
const bucketName = 'amazon-connect-f7e5ad160906';

export const handler = async (event) => {
    try {
        // Process each record from the Kinesis event
        for (const record of event.Records) {
            // Decode and parse the Kinesis data
            const payload = Buffer.from(record.kinesis.data, 'base64').toString('utf-8');
            const contactTraceRecord = JSON.parse(payload);
            console.log("JSON Payload:", contactTraceRecord);

            // Extract the ContactId from the record. If missing, log and skip.
            const contactId = contactTraceRecord.ContactId;
            if (!contactId) {
                console.error("Missing ContactId in record:", contactTraceRecord);
                continue;
            }

            // Get current date information for folder structure and timestamp
            const now = new Date();
            const year = now.getFullYear();
            const month = String(now.getMonth() + 1).padStart(2, '0'); // Month is zero-indexed
            const day = String(now.getDate()).padStart(2, '0');
            const timestamp = now.toISOString();

            // Build the object key using the provided folder structure and file naming convention
            const objectKey = `connect/nitovo/nitovo_ctrs/${year}/${month}/${day}/${contactId}_NitovoCTR_${timestamp}.json`;

            // Convert the record back to a pretty-printed JSON string for the file content
            const jsonString = JSON.stringify(contactTraceRecord, null, 2);

            // Define the parameters for the S3 PutObject command
            const params = {
                Bucket: bucketName,
                Key: objectKey,
                Body: jsonString,
                ContentType: 'application/json'
            };

            console.log("Writing JSON to S3 with params:", JSON.stringify(params));
            await s3Client.send(new PutObjectCommand(params));
        }

        return {
            statusCode: 200,
            body: 'Records processed and written to S3 successfully.'
        };
    } catch (error) {
        console.error('Error processing records:', error);
        return {
            statusCode: 500,
            body: 'Error processing records.'
        };
    }
};
