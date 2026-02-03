import boto3

# -------- CONFIG --------
LOCAL_FILE = "/XXXXXXXXXXXXX.csv"
S3_BUCKET = "XXXXXXXXX"
S3_KEY = "XXXXXXX.csv"
# ------------------------

s3 = boto3.client("s3")

s3.upload_file(
    Filename=LOCAL_FILE,
    Bucket=S3_BUCKET,
    Key=S3_KEY
)

print(f"Uploaded {LOCAL_FILE} to s3://{S3_BUCKET}/{S3_KEY}")
