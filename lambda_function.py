import boto3
import pandas as pd

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = 'arn:aws:sns:us-east-1:891377251081:doordash-topic-na'

def lambda_handler(event, context):
    # Get the S3 bucket and object key from the Lambda event trigger
    print('Recieved Event -> ',event)
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
    
        # Use boto3 to get the Json file from S3 and write back to S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        json_data = response["Body"].read().decode('utf-8')
        df = pd.read_json(json_data)
        
        df_final = df[df['status']=='delivered']
        final_json_data = df_final.to_json()
        target_bucket = 'doordash-target-zn-na'
        l = key.split("-")
        target_key = f"{l[0]}-{l[1]}-{l[2]}-output.json"
        s3_client.put_object(Bucket=target_bucket, Key=target_key, Body=final_json_data)
        
        message = "Input S3 File {} has been processed succesfuly !!".format("s3://"+bucket+"/"+key)
        respone = sns_client.publish(Subject="SUCCESS - DoorDash Daily Data Processing",TargetArn=sns_arn, Message=message, MessageStructure='text')
    except Exception as err:
        print(err)
        message = "Input S3 File {} processing is Failed !!".format("s3://"+bucket+"/"+key)
        respone = sns_client.publish(Subject="FAILED - DoorDash Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')