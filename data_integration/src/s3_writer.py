from __future__ import annotations

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def upload_jsonl(content: str, bucket: str, key: str, region: str) -> str:
    """Upload JSONL string to S3 and return the s3:// URI.

    Uses boto3's default credential chain:
      1. AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars
      2. ~/.aws/credentials
      3. IAM instance role
    """
    try:
        client = boto3.client("s3", region_name=region)
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to upload s3://{bucket}/{key}: {exc}") from exc

    return f"s3://{bucket}/{key}"
