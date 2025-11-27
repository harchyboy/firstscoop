import os
from dotenv import load_dotenv

print("🔍 DIAGNOSTIC MODE: Checking Vantage Configuration...")
print(f"📂 Current Folder: {os.getcwd()}")

# 1. Check if .env exists
files = os.listdir('.')
env_files = [f for f in files if '.env' in f]

if not env_files:
    print("❌ CRITICAL: No .env file found.")
    print("   -> Did you save it inside a subfolder?")
else:
    print(f"✅ Found potential config files: {env_files}")
    for f in env_files:
        if f == '.env.txt':
            print("⚠️ WARNING: Your file is named '.env.txt'. It should be just '.env'")
            print("   -> Action: Rename it by removing the .txt extension.")

# 2. Try to load it
load_dotenv()
access_key = os.getenv('AWS_ACCESS_KEY_ID')

if access_key:
    print(f"✅ Keys Loaded! Access Key starts with: {access_key[:4]}...")
else:
    print("❌ CRITICAL: .env file exists but keys could not be read.")
    print("   -> Make sure you saved the file.")

# 3. Test Boto3 direct (Bypass Vantage Class)
import boto3
try:
    print("☁️ Testing AWS Connection directly...")
    client = boto3.client('s3', region_name='eu-west-2')
    # If keys are missing, this next line is usually where it fails or returns empty
    # Note: boto3.client() doesn't fail until you make a call if keys are missing
    client.list_buckets() 
    print("✅ AWS Connection Successful!")
except Exception as e:
    print(f"❌ AWS Connection Failed: {e}")
```

### **Step 2: Run the Diagnostic**

Run this command in your terminal:
```bash
python debug_setup.py