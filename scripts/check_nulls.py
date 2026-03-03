import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import migrate
import boto3
import json

session = boto3.Session(profile_name='developer-leonardo-candio')
s3 = session.client('s3', region_name='us-east-2')
partidas = migrate.list_partida_prefixes(s3, 'inmoba-sunarp-vispartida')[:5]

class Args:
    dry_run = True

args = Args()
records, _ = migrate.migrate_metadata(s3, 'inmoba-sunarp-vispartida', partidas, args)

for i, r in enumerate(records):
    print(f"\n--- Record {i} ({r['partida_registral']}) ---")
    for k, v in r.items():
        if k == 'raw_response':
            print(f"{k}: <json string of length {len(v)}>")
        elif k in ['asientos', 'fichas', 'folios']:
            print(f"{k}: list of {len(v)} items")
        else:
            print(f"{k}: {repr(v)}")
