CREATE DATABASE IF NOT EXISTS inmoba_sunarp;

DROP TABLE IF EXISTS inmoba_sunarp.partidas;

CREATE EXTERNAL TABLE inmoba_sunarp.partidas (
  partida_registral STRING,
  busqueda_id STRING,
  oficina_registral STRING,
  area_registral STRING,
  total_pages INT,
  asientos ARRAY<STRUCT<
    codActo:STRING,
    codRubro:STRING,
    aaTitu:STRING,
    numTitu:STRING,
    descActo:STRING,
    nsAsiento:STRING,
    nsAsiePlaca:STRING,
    numPlaca:STRING,
    fgExonPub:STRING,
    fgExonJud:STRING,
    numPag:INT,
    idImgAsiento:INT,
    letraRubro:STRING,
    nombreRubro:STRING,
    fechaInscripcion:STRING,
    numRef:STRING,
    numPagRef:STRING,
    listPag:ARRAY<STRUCT<pagina:STRING,nroPagRef:STRING>>,
    nroPagTotalAiento:STRING,
    refNumPart:STRING,
    oficRegId:STRING,
    regPubId:STRING,
    areaRegId:STRING,
    numPartida:STRING,
    esSARP:BOOLEAN
  >>,
  fichas ARRAY<STRUCT<
    numFicha:STRING,
    fichaBis:STRING,
    idImgFicha:INT,
    numPag:INT,
    listPag:ARRAY<STRUCT<pagina:STRING,nroPagRef:STRING>>
  >>,
  folios ARRAY<STRUCT<
    nuFoja:STRING,
    nuTomo:STRING,
    idImgFolio:INT,
    nsCade:INT,
    nroPagRef:STRING
  >>,
  raw_response STRING,
  scraped_at STRING,
  is_sarp BOOLEAN,
  sarp_source STRING
)
STORED AS PARQUET
LOCATION 's3://inmoba-sunarp-vispartida/curated/'
TBLPROPERTIES ('parquet.compression' = 'ZSTD');
