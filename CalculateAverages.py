import json
import boto3
import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):

    print("Received event: " + json.dumps(event))  

    try:
        if isinstance(event, str):
            numbers_str = event
            numbers = json.loads(numbers_str) 
        elif isinstance(event, list):
            numbers = event  
        else:
            raise ValueError("Invalid event format: expected a string or a list of numbers")
    except (ValueError, json.JSONDecodeError) as e:
        print(f"Error parsing the event: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps({
                'status': 'error',
                'message': str(e)
            })
        }

    if isinstance(numbers, list) and all(isinstance(num, (int, float)) for num in numbers):
        average = sum(numbers) / len(numbers)
        print(f"Calculated average: {average}")

        output_string = f"Average of {numbers} is {average}"
        print(output_string)

        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"averages-{timestamp}.txt"  

        bucket_name = "myBucket"
        object_key = f"averages/{filename}"

        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=output_string  
            )
            print(f"Saved output to S3: s3://{bucket_name}/{object_key}")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'success',
                    'output': output_string,
                    's3_location': f"s3://{bucket_name}/{object_key}"
                })
            }
        except Exception as e:
            print(f"Error saving to S3: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'status': 'error',
                    'message': 'Failed to save output to S3',
                    'error': str(e)
                })
            }
    else:
        print("Received invalid numbers format.")
        return {
            'statusCode': 400,
            'body': json.dumps({
                'status': 'error',
                'message': 'Invalid numbers format'
            })
        }
