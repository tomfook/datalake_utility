import json
import boto3
import datetime

s3 = boto3.resource("s3")
bucket = s3.Bucket("MY_BUCKET_NAME")
def output_obj(dataname, filename): return bucket.Object(f"dynamodb/history/{dataname}/{filename}")

def lambda_handler(event, context):
    my_arn = context.invoked_function_arn
    
    for record in event["Records"]:
        event_id = record["eventID"]
        event_name = record["eventName"]
        timestamp = record["dynamodb"]["ApproximateCreationDateTime"]
        event_source_arn = record["eventSourceARN"]
        
        table_name = event_source_arn.split("table")[-1].split("/")[1]

        dt = datetime.datetime.fromtimestamp(timestamp)
        dt_jst = dt + datetime.timedelta(hours = 9)
        ymdhms = dt_jst.strftime("%Y%m%d%H%M%S")
        
        out_key = "{}_{}_{}.json".format(ymdhms, event_name, event_id)
        output_obj(table_name, out_key).put(Body = json.dumps(record), Tagging="put_by="+my_arn)

    return True
