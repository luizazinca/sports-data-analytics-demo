import json
import os
import boto3
import pandas as pd
from io import StringIO, BytesIO

s3 = boto3.client("s3")

S3_BUCKET = os.environ.get("S3_BUCKET")
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze")
SILVER_PREFIX = os.environ.get("SILVER_PREFIX", "silver")


def lambda_handler(event, context):
    """
    Triggered by S3 object creation in bronze/.
    Reads CSV from bronze/, does light cleaning, writes CSV to silver/.
    """
    try:
        # S3 trigger format
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        print(f"Received event for bucket={bucket}, key={key}")

        if not key.startswith(f"{BRONZE_PREFIX}/"):
            print("Not a bronze key. Skipping.")
            return {"statusCode": 200, "body": "Skipped non-bronze object"}

        # Download CSV
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(obj["Body"])

        # Basic cleaning for demo
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df = df.drop_duplicates()

        # Build silver key (preserve dataset subfolder)
        silver_key = key.replace(f"{BRONZE_PREFIX}/", f"{SILVER_PREFIX}/", 1)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=bucket,
            Key=silver_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )

        print(f"Wrote cleaned file to s3://{bucket}/{silver_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Bronze to Silver success",
                "input_key": key,
                "output_key": silver_key,
                "rows": int(len(df))
            })
        }

    except Exception as e:
        print(f"Error in bronze_to_silver: {e}")
        raise
