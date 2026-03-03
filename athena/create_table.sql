-- Athena DDL for inmoba-sunarp-vispartida Data Lake
-- Generated for: inmoba_sunarp.partidas
-- Parquet location: s3://inmoba-sunarp-vispartida/curated/
-- Compression: ZSTD

CREATE DATABASE IF NOT EXISTS inmoba_sunarp;

CREATE EXTERNAL TABLE IF NOT EXISTS inmoba_sunarp.partidas (
    partida_registral    STRING,
    busqueda_id          STRING,
    oficina_registral    STRING,
    area_registral       STRING,
    total_pages          INT,
    asientos             ARRAY<STRUCT<
        numero_asiento:   STRING,
        acto:             STRING,
        monto:            STRING,
        moneda:           STRING,
        fecha:            STRING,
        descripcion:      STRING,
        tipo_acto:        STRING,
        estado:           STRING,
        tomo:             STRING,
        ficha:            STRING,
        folio:            STRING,
        partida:          STRING,
        pagina:           STRING,
        ciento:           STRING,
        oficina_origen:   STRING,
        domicilio:        STRING,
        titular:          STRING,
        concubino:        STRING,
        gravamen:         STRING,
        importe_letra:    STRING,
        plazo:            STRING,
        tasa:             STRING,
        correlativo:      STRING,
        raw_text:         STRING,
        orden:            STRING
    >>,
    fichas               ARRAY<STRUCT<
        numero_ficha:     STRING,
        tipo:             STRING,
        fecha:            STRING,
        descripcion:      STRING,
        estado:           STRING
    >>,
    folios               ARRAY<STRUCT<
        numero_folio:     STRING,
        tipo:             STRING,
        fecha:            STRING,
        descripcion:      STRING,
        estado:           STRING
    >>,
    raw_response         STRING,
    scraped_at           STRING,
    is_sarp              BOOLEAN,
    sarp_source          STRING
)
STORED AS PARQUET
LOCATION 's3://inmoba-sunarp-vispartida/curated/'
TBLPROPERTIES (
    'parquet.compression' = 'ZSTD'
);

-- ============================================================
-- EXAMPLE QUERIES
-- ============================================================

-- Count all partidas
-- SELECT COUNT(*) FROM inmoba_sunarp.partidas;

-- Find partida by ID
-- SELECT * FROM inmoba_sunarp.partidas
-- WHERE partida_registral = '00708079';

-- Expand asientos with CROSS JOIN UNNEST
-- SELECT
--     p.partida_registral,
--     p.oficina_registral,
--     a.numero_asiento,
--     a.acto,
--     a.fecha,
--     a.monto,
--     a.moneda,
--     a.titular
-- FROM inmoba_sunarp.partidas p
-- CROSS JOIN UNNEST(p.asientos) AS t(a)
-- WHERE p.partida_registral = '00708079';

-- Find partidas with SARP entries
-- SELECT partida_registral, sarp_source
-- FROM inmoba_sunarp.partidas
-- WHERE is_sarp = true
-- LIMIT 100;

-- Count asientos per oficina
-- SELECT
--     p.oficina_registral,
--     COUNT(a) AS total_asientos
-- FROM inmoba_sunarp.partidas p
-- CROSS JOIN UNNEST(p.asientos) AS t(a)
-- GROUP BY p.oficina_registral
-- ORDER BY total_asientos DESC;
