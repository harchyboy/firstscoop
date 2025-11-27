from vantage_s3 import VantageDataLake
from dotenv import load_dotenv

load_dotenv()

print("📡 Scanning Vantage Data Lake...\n")
lake = VantageDataLake()

try:
    # List ALL objects in the bucket
    paginator = lake.s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=lake.bucket_name)
    
    files_by_folder = {}
    total_size = 0
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                size = obj['Size']
                total_size += size
                
                # Group by top-level folder
                parts = key.split('/')
                if len(parts) > 1:
                    folder = parts[0] + '/' + parts[1] if len(parts) > 2 else parts[0]
                else:
                    folder = 'root'
                
                if folder not in files_by_folder:
                    files_by_folder[folder] = []
                files_by_folder[folder].append({
                    'key': key,
                    'size': size,
                    'size_mb': round(size / 1024 / 1024, 2)
                })
    
    # Print organized results
    print("=" * 80)
    print("VANTAGE DATA LAKE INVENTORY")
    print("=" * 80)
    
    for folder in sorted(files_by_folder.keys()):
        files = files_by_folder[folder]
        folder_size = sum(f['size'] for f in files)
        folder_size_mb = round(folder_size / 1024 / 1024, 2)
        
        print(f"\n📁 {folder}/")
        print(f"   Files: {len(files)} | Size: {folder_size_mb} MB")
        print("   " + "-" * 70)
        
        for f in files[:20]:  # Show first 20 files per folder
            size_str = f"{f['size_mb']} MB" if f['size_mb'] > 0.01 else f"{f['size']} bytes"
            print(f"   • {f['key']} ({size_str})")
        
        if len(files) > 20:
            print(f"   ... and {len(files) - 20} more files")
    
    print("\n" + "=" * 80)
    print(f"TOTAL: {sum(len(f) for f in files_by_folder.values())} files | {round(total_size / 1024 / 1024, 2)} MB")
    print("=" * 80)
    
except Exception as e:
    print(f"❌ Error listing bucket contents: {e}")

