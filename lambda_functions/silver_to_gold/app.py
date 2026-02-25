import json
import os
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timezone

s3 = boto3.client("s3")

S3_BUCKET = os.environ.get("S3_BUCKET")
SILVER_PREFIX = os.environ.get("SILVER_PREFIX", "silver")
GOLD_PREDICTIONS_PREFIX = os.environ.get("GOLD_PREDICTIONS_PREFIX", "gold/predictions")
GOLD_METRICS_PREFIX = os.environ.get("GOLD_METRICS_PREFIX", "gold/metrics")
GOLD_LATEST_PREFIX = os.environ.get("GOLD_LATEST_PREFIX", "gold/latest")


def lambda_handler(event, context):
    """
    Triggered by S3 object creation in silver/.
    For demo: reads silver CSV, creates a simple 'score' / summary output,
    writes results to gold/.
    """
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        print(f"Received event for bucket={bucket}, key={key}")

        if not key.startswith(f"{SILVER_PREFIX}/"):
            print("Not a silver key. Skipping.")
            return {"statusCode": 200, "body": "Skipped non-silver object"}

        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(obj["Body"])

        # Demo "preprocessing + inference"
        result_df = df.copy()

        # Example demo score: count non-null values per row (works on any CSV)
        result_df["demo_score"] = result_df.notna().sum(axis=1)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        base_name = key.split("/")[-1].replace(".csv", "")

        # Predictions output
        pred_key = f"{GOLD_PREDICTIONS_PREFIX}/{base_name}_predictions_{timestamp}.csv"
        pred_buffer = StringIO()
        result_df.to_csv(pred_buffer, index=False)
        s3.put_object(
            Bucket=bucket,
            Key=pred_key,
            Body=pred_buffer.getvalue(),
            ContentType="text/csv"
        )

        # Metrics summary output
        metrics_df = pd.DataFrame([{
            "source_key": key,
            "rows": int(len(df)),
            "columns": int(df.shape[1]),
            "generated_at_utc": timestamp,
            "demo_score_mean": float(result_df["demo_score"].mean()) if len(result_df) > 0 else 0.0
        }])

        metrics_key = f"{GOLD_METRICS_PREFIX}/{base_name}_metrics_{timestamp}.csv"
        metrics_buffer = StringIO()
        metrics_df.to_csv(metrics_buffer, index=False)
        s3.put_object(
            Bucket=bucket,
            Key=metrics_key,
            Body=metrics_buffer.getvalue(),
            ContentType="text/csv"
        )

        # "Latest" snapshot for Streamlit demo
        latest_key = f"{GOLD_LATEST_PREFIX}/{base_name}_latest.csv"
        latest_buffer = StringIO()
        result_df.to_csv(latest_buffer, index=False)
        s3.put_object(
            Bucket=bucket,
            Key=latest_key,
            Body=latest_buffer.getvalue(),
            ContentType="text/csv"
        )

        print(f"Wrote gold outputs: {pred_key}, {metrics_key}, {latest_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Silver to Gold success",
                "input_key": key,
                "prediction_key": pred_key,
                "metrics_key": metrics_key,
                "latest_key": latest_key
            })
        }

    except Exception as e:
        print(f"Error in silver_to_gold: {e}")
        raise
