import json
import boto3
import uuid

s3_client = boto3.client('s3')

def lambda_handler(event, context):

    print("Received event: " + event)  

    message = event

    if isinstance(message, str) and message.startswith("Hello from "):

        client_name = message[len("Hello from "):].split(' ')[0]  
        response_message = f"Received a message from: {client_name}"
        
        print(response_message)

        try:
            filename = f"response-{uuid.uuid4()}.txt" 
            bucket_name = "myBucket"
            object_key = f"responses/{filename}"
            
            s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=response_message 
            )
            print(f"Saved response to S3: s3://{bucket_name}/{object_key}")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'success',
                    's3_location': f"s3://{bucket_name}/{object_key}"
                })
            }
        except Exception as e:
            print(f"Error saving to S3: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'status': 'error',
                    'message': 'Failed to save response to S3',
                    'error': str(e)
                })
            }
    else:
        print("Received an invalid message format.")
        return {
            'statusCode': 400,
            'body': json.dumps({
                'status': 'error',
                'message': 'Invalid message format'
            })
        }
