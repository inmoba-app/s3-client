import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from inmoba_s3.schema import PARTIDA_SCHEMA
import pyarrow as pa
import migrate
import boto3

session = boto3.Session(profile_name='developer-leonardo-candio')
s3 = session.client('s3', region_name='us-east-2')
partidas = migrate.list_partida_prefixes(s3, 'inmoba-sunarp-vispartida')[:5]

class Args:
    dry_run = True

args = Args()
records, _ = migrate.migrate_metadata(s3, 'inmoba-sunarp-vispartida', partidas, args)

for i, r in enumerate(records):
    # Quick fix for busqueda_id
    if 'busqueda_id' in r and r['busqueda_id'] is not None:
        r['busqueda_id'] = str(r['busqueda_id'])
        
    try:
        table = pa.Table.from_pylist([r], schema=PARTIDA_SCHEMA)
        print(f"Record {i} OK")
    except Exception as e:
        print(f"Record {i} FAILED")
        for col_name in PARTIDA_SCHEMA.names:
            col_type = PARTIDA_SCHEMA.field(col_name).type
            try:
                pa.array([r[col_name]], type=col_type)
            except Exception as ce:
                print(f"  -> Column '{col_name}' failed: {ce}")
                if isinstance(r[col_name], list) and len(r[col_name]) > 0:
                    first_item = r[col_name][0]
                    if isinstance(first_item, dict):
                        for sub_k, sub_v in first_item.items():
                            print(f"     {sub_k}: {type(sub_v).__name__} = {repr(sub_v)}")
                else:
                    print(f"     Value = {repr(r[col_name])} (type: {type(r[col_name])})")
        break
