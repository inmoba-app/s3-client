import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import migrate
import boto3

session = boto3.Session(profile_name='developer-leonardo-candio')
s3 = session.client('s3', region_name='us-east-2')
partidas = migrate.list_partida_prefixes(s3, 'inmoba-sunarp-vispartida')[1:2]

records, _ = migrate.migrate_metadata(s3, 'inmoba-sunarp-vispartida', partidas, type('Args', (), {'dry_run': True})())

r = records[0]
print("Top level keys:")
for k in r.keys():
    print(f"- {k}")

if r['asientos']:
    print("\nAsientos keys:")
    for k in r['asientos'][0].keys():
        print(f"- {k} (type: {type(r['asientos'][0][k]).__name__})")

if r['folios']:
    print("\nFolios keys:")
    for k in r['folios'][0].keys():
        print(f"- {k} (type: {type(r['folios'][0][k]).__name__})")
