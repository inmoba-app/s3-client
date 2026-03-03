import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import migrate
import boto3

session = boto3.Session(profile_name='developer-leonardo-candio')
s3 = session.client('s3', region_name='us-east-2')

resp = s3.get_object(Bucket='inmoba-sunarp-vispartida', Key="00708360/metadata.json")
import json
data = json.loads(resp['Body'].read().decode('utf-8'))
if data.get('fichas') and len(data['fichas']) > 0:
    for f in data['fichas']:
        if f.get('listPag'):
            print(f"listPag in ficha: {f['listPag']} (type: {type(f['listPag'][0]).__name__})")
            break
if data.get('asientos') and len(data['asientos']) > 0:
    for a in data['asientos']:
        if a.get('listPag'):
            print(f"listPag in asiento: {a['listPag']} (type: {type(a['listPag'][0]).__name__})")
            break
