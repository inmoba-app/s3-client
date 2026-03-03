from __future__ import annotations

import argparse
import importlib
import io
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import boto3
from botocore.config import Config

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from inmoba_s3.schema import PARTIDA_SCHEMA, normalize_record


CURATED_KEY = "curated/partidas.parquet"
MAX_WORKERS = 100


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Migrate old S3 layout ({partida}/metadata.json + {partida}/partida_{partida}.pdf) "
            "to curated/partidas.parquet + documents/partida_{partida}.pdf."
        )
    )
    parser.add_argument("--bucket", default="inmoba-sunarp-vispartida")
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--profile", default=None, help="AWS profile name")
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate without writing to S3"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Process first N partidas"
    )
    parser.add_argument("--skip-pdfs", action="store_true", help="Skip PDF copy step")
    parser.add_argument(
        "--output-dir",
        default="./migration_output",
        help="Dir for dry-run output",
    )
    return parser.parse_args(argv)


def list_partida_prefixes(s3_client, bucket: str) -> list[str]:
    prefixes: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Delimiter="/"):
        for prefix_obj in page.get("CommonPrefixes", []):
            prefix = prefix_obj["Prefix"]
            if prefix.startswith("runs/"):
                continue
            prefixes.append(prefix.rstrip("/"))
    return prefixes


def _fetch_metadata(s3_client, bucket: str, partida: str) -> tuple[str, dict[str, Any] | None, Exception | None]:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=f"{partida}/metadata.json")
        raw = json.loads(response["Body"].read().decode("utf-8"))
        record = normalize_record(raw)
        record["partida_registral"] = partida
        return partida, record, None
    except Exception as exc:
        return partida, None, exc


def migrate_metadata(
    s3_client, bucket: str, partidas: list[str], args
) -> tuple[list[dict[str, object]], list[str]]:
    records: list[dict[str, object]] = []
    failures: list[str] = []
    
    print(f"  Fetching metadata with {MAX_WORKERS} concurrent workers...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_metadata, s3_client, bucket, p): p for p in partidas
        }
        for i, future in enumerate(as_completed(futures)):
            if i > 0 and i % 1000 == 0:
                print(f"  Processed {i}/{len(partidas)} metadata files...")
            partida, record, exc = future.result()
            if exc:
                print(f"  WARN: Failed metadata for {partida}: {exc}", file=sys.stderr)
                failures.append(partida)
            elif record:
                records.append(record)
                
    return records, failures


def _copy_pdf(s3_client, bucket: str, partida: str) -> tuple[str, bool, Exception | None]:
    old_key = f"{partida}/partida_{partida}.pdf"
    new_key = f"documents/partida_{partida}.pdf"
    try:
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": old_key},
            Key=new_key,
        )
        return partida, True, None
    except Exception as exc:
        return partida, False, exc


def migrate_pdfs(
    s3_client, bucket: str, partidas: list[str], args
) -> tuple[int, list[str]]:
    copied = 0
    failures: list[str] = []
    
    print(f"  Copying PDFs with {MAX_WORKERS} concurrent workers...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_copy_pdf, s3_client, bucket, p): p for p in partidas
        }
        for i, future in enumerate(as_completed(futures)):
            if i > 0 and i % 1000 == 0:
                print(f"  Processed {i}/{len(partidas)} PDFs...")
            partida, success, exc = future.result()
            if exc:
                print(f"  WARN: Failed PDF for {partida}: {exc}", file=sys.stderr)
                failures.append(partida)
            elif success:
                copied += 1
                
    return copied, failures


def _load_pyarrow() -> tuple[Any, Any]:
    pa = importlib.import_module("pyarrow")
    pq = importlib.import_module("pyarrow.parquet")
    return pa, pq


def build_parquet_table(records: list[dict[str, object]]) -> Any:
    pa, _ = _load_pyarrow()
    return pa.Table.from_pylist(records, schema=PARTIDA_SCHEMA)


def write_parquet(records: list[dict[str, object]], output_path: str) -> None:
    _, pq = _load_pyarrow()
    table = build_parquet_table(records)
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    pq.write_table(table, output_path, compression="zstd")


def upload_parquet(
    s3_client, bucket: str, records: list[dict[str, object]], curated_key: str
) -> None:
    _, pq = _load_pyarrow()
    table = build_parquet_table(records)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    s3_client.put_object(
        Bucket=bucket,
        Key=curated_key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )


def main(args=None) -> None:
    if args is None:
        args = parse_args()

    # Increase max pool connections so threads aren't bottlenecked by boto3 HTTP connection pool
    boto_config = Config(max_pool_connections=MAX_WORKERS)

    if args.profile:
        session = boto3.Session(profile_name=args.profile)
        s3 = session.client("s3", region_name=args.region, config=boto_config)
    else:
        s3 = boto3.client("s3", region_name=args.region, config=boto_config)

    print(f"Migration starting: bucket={args.bucket}, dry_run={args.dry_run}")

    print("Listing partida prefixes...")
    partidas = list_partida_prefixes(s3, args.bucket)
    print(f"Found {len(partidas)} partidas")

    if args.limit is not None:
        partidas = partidas[: args.limit]
        print(f"Limiting to {len(partidas)} partidas")

    print("Processing metadata.json files...")
    records, meta_failures = migrate_metadata(s3, args.bucket, partidas, args)
    print(f"Metadata: {len(records)} ok, {len(meta_failures)} failed")

    table = build_parquet_table(records)
    print(f"Parquet table: {table.num_rows} rows")

    if args.dry_run:
        output_path = os.path.join(args.output_dir, "partidas.parquet")
        write_parquet(records, output_path)
        print(f"Dry run: wrote {table.num_rows} rows to {output_path}")
        print("Sample (first 3 rows):")
        print(json.dumps(table.slice(0, 3).to_pylist(), ensure_ascii=True, indent=2))
        pdf_copied = 0
        pdf_failures: list[str] = []
        if args.skip_pdfs:
            print("Skipping PDF copy step (--skip-pdfs).")
        else:
            print("Dry run enabled: skipping PDF copy step.")
    else:
        upload_parquet(s3, args.bucket, records, CURATED_KEY)
        print(f"Uploaded {CURATED_KEY} ({table.num_rows} rows)")

        if args.skip_pdfs:
            print("Skipping PDF copy step (--skip-pdfs).")
            pdf_copied = 0
            pdf_failures = []
        else:
            print("Copying PDF documents...")
            pdf_copied, pdf_failures = migrate_pdfs(s3, args.bucket, partidas, args)
            print(f"PDFs: {pdf_copied} copied, {len(pdf_failures)} failed")

    total_failures = len(meta_failures) + len(pdf_failures)
    print(f"\nMigration complete. Total failures: {total_failures}")


if __name__ == "__main__":
    main()
