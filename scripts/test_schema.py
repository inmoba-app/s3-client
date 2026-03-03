import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import migrate
import boto3
from inmoba_s3.schema import PARTIDA_SCHEMA, normalize_record
import pyarrow as pa

session = boto3.Session(profile_name='developer-leonardo-candio')
s3 = session.client('s3', region_name='us-east-2')
partidas = migrate.list_partida_prefixes(s3, 'inmoba-sunarp-vispartida')[:50]

records, _ = migrate.migrate_metadata(s3, 'inmoba-sunarp-vispartida', partidas, type('Args', (), {'dry_run': True})())

for i, r in enumerate(records):
    try:
        pa.Table.from_pylist([r], schema=PARTIDA_SCHEMA)
    except Exception as e:
        print(f"FAILED on record {i}: {e}")
        sys.exit(1)

print("SUCCESS: 50 records loaded successfully with new schema.")
