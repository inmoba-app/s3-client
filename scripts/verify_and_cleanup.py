import argparse
import io
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pyarrow.parquet as pq
from botocore.config import Config

MAX_WORKERS = 100

def parse_args():
    parser = argparse.ArgumentParser(description="Verify migration and delete old layout.")
    parser.add_argument("--bucket", default="inmoba-sunarp-vispartida")
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--profile", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Verify but do not delete")
    return parser.parse_args()

def get_parquet_partidas(s3, bucket):
    print("Downloading curated/partidas.parquet...")
    obj = s3.get_object(Bucket=bucket, Key="curated/partidas.parquet")
    buf = io.BytesIO(obj["Body"].read())
    table = pq.read_table(buf, columns=["partida_registral"])
    partidas = set(table.column("partida_registral").to_pylist())
    print(f"  Found {len(partidas)} records in Parquet.")
    return partidas

def get_copied_pdfs(s3, bucket):
    print("Listing documents/ prefix...")
    partidas = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="documents/partida_"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # documents/partida_12345.pdf
            filename = key.split("/")[-1]
            if filename.startswith("partida_") and filename.endswith(".pdf"):
                p = filename[8:-4]
                partidas.add(p)
    print(f"  Found {len(partidas)} PDFs in documents/.")
    return partidas

def get_old_prefixes(s3, bucket):
    print("Listing root prefixes...")
    prefixes = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Delimiter="/"):
        for prefix_obj in page.get("CommonPrefixes", []):
            p = prefix_obj["Prefix"].rstrip("/")
            if p not in ("runs", "curated", "documents"):
                prefixes.add(p)
    print(f"  Found {len(prefixes)} old partida folders.")
    return prefixes

def delete_old_partida_folder(s3, bucket, partida):
    # List all objects in the old folder and delete them
    keys_to_delete = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{partida}/"):
        for obj in page.get("Contents", []):
            keys_to_delete.append({"Key": obj["Key"]})
    
    if not keys_to_delete:
        return 0

    # Delete in batches of 1000
    deleted_count = 0
    for i in range(0, len(keys_to_delete), 1000):
        batch = keys_to_delete[i:i+1000]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
        deleted_count += len(batch)
    return deleted_count

def main():
    args = parse_args()
    boto_config = Config(max_pool_connections=MAX_WORKERS)
    
    if args.profile:
        session = boto3.Session(profile_name=args.profile)
        s3 = session.client("s3", region_name=args.region, config=boto_config)
    else:
        s3 = boto3.client("s3", region_name=args.region, config=boto_config)
        
    print(f"Verification starting for bucket: {args.bucket}")
    
    parquet_partidas = get_parquet_partidas(s3, args.bucket)
    copied_pdfs = get_copied_pdfs(s3, args.bucket)
    old_prefixes = get_old_prefixes(s3, args.bucket)
    
    print("\n--- Verification Results ---")
    missing_in_parquet = old_prefixes - parquet_partidas
    missing_in_pdfs = old_prefixes - copied_pdfs
    
    if missing_in_parquet:
        print(f"❌ FAIL: {len(missing_in_parquet)} old partidas are NOT in the Parquet file!")
        print(f"Sample missing: {list(missing_in_parquet)[:5]}")
        sys.exit(1)
    else:
        print("✅ SUCCESS: 100% of old partidas are present in the Parquet file.")
        
    if missing_in_pdfs:
        print(f"❌ FAIL: {len(missing_in_pdfs)} old PDFs are NOT in the documents/ prefix!")
        print(f"Sample missing: {list(missing_in_pdfs)[:5]}")
        sys.exit(1)
    else:
        print("✅ SUCCESS: 100% of old PDFs were successfully copied to documents/.")
        
    print("----------------------------\n")
    
    if args.dry_run:
        print("[DRY RUN] Verification passed. To actually delete old folders, run without --dry-run")
        sys.exit(0)
        
    print(f"Verification passed! Commencing deletion of {len(old_prefixes)} old folders...")
    
    total_deleted_objects = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(delete_old_partida_folder, s3, args.bucket, p): p for p in old_prefixes}
        for i, future in enumerate(as_completed(futures)):
            if i > 0 and i % 1000 == 0:
                print(f"  Deleted {i}/{len(old_prefixes)} folders...")
            try:
                deleted_count = future.result()
                total_deleted_objects += deleted_count
            except Exception as e:
                p = futures[future]
                print(f"  WARN: Failed to delete folder {p}: {e}")
                
    print(f"\nCleanup complete. Deleted {total_deleted_objects} old objects.")

if __name__ == "__main__":
    main()
