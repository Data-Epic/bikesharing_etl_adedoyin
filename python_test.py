import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",  # For host access
    aws_access_key_id="iMu7c6mHKR0ITQBMJuHj",
    aws_secret_access_key="gfQRYTRhR0XWaKy1b7Cis8dSvktFk76YKhTscQHG",
    region_name="us-east-1",
)

try:
    buckets = s3.list_buckets()
    print("Buckets:", buckets)
except Exception as e:
    print("Error:", e)
