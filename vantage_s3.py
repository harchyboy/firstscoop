import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError

class VantageDataLake:
    def __init__(self):
        """
        Initializes the connection to the Vantage Data Lake on AWS S3.
        Expects AWS credentials to be set in environment variables (loaded via .env).
        """
        # Create the S3 Client using credentials from your .env file
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            # Default to London (eu-west-2) if not specified
            region_name=os.getenv('AWS_DEFAULT_REGION', 'eu-west-2')
        )
        
        # CRITICAL: This must match the bucket name you created in AWS
        # If you named it "vantage-data-lake-prod-john", change it here.
        self.bucket_name = "vantage-data-lake-prod"

    def download_file(self, s3_key, local_path):
        """
        Downloads a specific file from S3 to your local machine.
        Args:
            s3_key: The path inside the bucket (e.g. "raw/epc/certificates.csv")
            local_path: Where to save it on your laptop (e.g. "./data/certificates.csv")
        """
        try:
            # Create the local folder if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            print(f"⬇️ Connecting to S3... Downloading {s3_key}...")
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            print(f"✅ Success! Saved to {local_path}")
            return True
            
        except ClientError as e:
            # Handle specific AWS errors (like File Not Found)
            if e.response['Error']['Code'] == "404":
                print(f"❌ Error: File not found in bucket: {s3_key}")
            elif e.response['Error']['Code'] == "403":
                print("❌ Error: Permission Denied. Check your AWS Keys.")
            else:
                print(f"❌ S3 Error: {e}")
            return False

# --- TEST BLOCK ---
# This allows you to run 'python vantage_s3.py' to verify your connection works.
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv() # Load keys for testing
    
    print("📡 Testing S3 Connection...")
    lake = VantageDataLake()
    
    # Try to list files in the 'raw' folder to prove access
    try:
        response = lake.s3_client.list_objects_v2(Bucket=lake.bucket_name, Prefix="raw/", MaxKeys=5)
        if 'Contents' in response:
            print("✅ Connection Verified! Found files:")
            for obj in response['Contents']:
                print(f" - {obj['Key']}")
        else:
            print("✅ Connection Verified! (Bucket is empty or folder not found)")
    except Exception as e:
        print(f"❌ Connection Failed: {e}")