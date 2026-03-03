import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import migrate
import boto3

session = boto3.Session(profile_name='developer-leonardo-candio')
s3 = session.client('s3', region_name='us-east-2')

# Page through until we find fichas
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket='inmoba-sunarp-vispartida', Delimiter='/'):
    for prefix in page.get('CommonPrefixes', []):
        p = prefix.get('Prefix')
        if p.startswith('runs/'):
            continue
        partida = p.rstrip('/')
        try:
            resp = s3.get_object(Bucket='inmoba-sunarp-vispartida', Key=f"{partida}/metadata.json")
            import json
            data = json.loads(resp['Body'].read().decode('utf-8'))
            if data.get('fichas') and len(data['fichas']) > 0:
                print(f"Found fichas in {partida}")
                for k in data['fichas'][0].keys():
                    print(f"- {k} (type: {type(data['fichas'][0][k]).__name__})")
                sys.exit(0)
        except Exception as e:
            pass
