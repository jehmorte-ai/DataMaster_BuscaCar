-- GOLD: visão 360 (FIPE + SUSEP + SEGUROS)
CREATE OR REPLACE TABLE `gold.veiculos_360` AS
WITH
-- 1) FIPE (pega o ÚLTIMO mês disponível; troque para um mês específico se quiser)
fipe_dedup AS (
  SELECT
    SAFE_CAST(marca AS STRING)  AS marca,
    SAFE_CAST(modelo AS STRING) AS modelo,
    SAFE_CAST(ano_modelo AS INT64)           AS ano_modelo,
    SAFE_CAST(tipo_combustivel AS STRING)    AS combustivel,
    SAFE_CAST(valor_float AS FLOAT64)        AS valor_fipe,
    SAFE_CAST(data_referencia AS DATE)       AS data_ref,
    -- chave normalizada (mesma regra da SUSEP)
    LOWER(REGEXP_REPLACE(
      TRANSLATE(CONCAT(marca,' ',modelo),
        'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'),
      r'[^a-z0-9]+', '_'))                   AS chave_join
  FROM `optical-victor-463515-v8.silver.fipe`
),
fipe_last_month AS (
  SELECT MAX(data_ref) AS max_ref FROM fipe_dedup
),
fipe_base AS (
  SELECT f.*
  FROM fipe_dedup f
  JOIN fipe_last_month m ON f.data_ref = m.max_ref
  WHERE f.valor_fipe IS NOT NULL
),

-- 2) SUSEP (já normalizada na sua Silver)
susep_base AS (
  SELECT
    SAFE_CAST(marca AS STRING)               AS susep_marca,
    SAFE_CAST(modelo AS STRING)              AS susep_modelo,
    SAFE_CAST(chave_join AS STRING)          AS chave_join,
    SAFE_CAST(indice_roubo_pct AS FLOAT64)   AS indice_roubo_pct,
    SAFE_CAST(veiculos_expostos AS FLOAT64)  AS veiculos_expostos,
    SAFE_CAST(sinistros AS FLOAT64)          AS sinistros,
    SAFE_CAST(sinistro_rate_pct AS FLOAT64)  AS sinistro_rate_pct,
    TIMESTAMP(data_extracao)                 AS susep_data_extracao
  FROM `optical-victor-463515-v8.silver.susep`
),

-- 3) SEGUROS (Silver). 
--    Se ainda não tiver a Silver, troque a origem para a Bronze e mantenha a construção da chave_join abaixo.
seg_base AS (
  SELECT
    SAFE_CAST('' AS STRING)          AS seg_marca,
    SAFE_CAST(modelo_slug AS STRING)         AS seg_modelo,
    LOWER(REGEXP_REPLACE(
      TRANSLATE(CONCAT('',' ',modelo_slug),
        'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'),
      r'[^a-z0-9]+', '_'))              AS chave_join,
    SAFE_CAST(seguradora_padrao AS STRING)     AS seguradora,
    SAFE_CAST(valor_seguro AS FLOAT64) AS premio_mensal,      -- R$ por mês
    SAFE_CAST(franquia_valor AS FLOAT64)      AS franquia,           -- R$
    TIMESTAMP(data_ingestao)            AS seg_data_extracao
  FROM `optical-victor-463515-v8.silver.seguros`
),

-- 4) agrega seguros por chave (ex.: média de prêmio, contagem de seguradoras)
seg_agg AS (
  SELECT
    chave_join,
    COUNT(DISTINCT seguradora)                      AS qtd_seguradoras,
    AVG(premio_mensal)                              AS premio_mensal_medio,
    STDDEV_SAMP(premio_mensal)                      AS premio_mensal_dp,
    AVG(franquia)                                   AS franquia_media,
    MAX(seg_data_extracao)                          AS seg_ult_extracao
  FROM seg_base
  GROUP BY 1
),

-- 5) junta tudo
joined AS (
  SELECT
    f.chave_join,
    f.data_ref                        AS fipe_data_ref,
    f.marca                           AS marca,
    f.modelo                          AS modelo,
    f.ano_modelo,
    f.combustivel,
    f.valor_fipe,

    -- SUSEP
    s.indice_roubo_pct,
    s.sinistro_rate_pct,
    s.veiculos_expostos,
    s.sinistros,
    s.susep_data_extracao,

    -- Seguros (agregado)
    a.qtd_seguradoras,
    a.premio_mensal_medio,
    a.premio_mensal_dp,
    a.franquia_media,
    a.seg_ult_extracao,

    -- derivadas
    SAFE_DIVIDE(a.premio_mensal_medio * 12.0, NULLIF(f.valor_fipe, 0)) * 100.0
      AS premio_anual_vs_valor_pct,

    CASE
      WHEN s.indice_roubo_pct IS NULL AND s.sinistro_rate_pct IS NULL THEN 'desconhecido'
      WHEN COALESCE(s.indice_roubo_pct, s.sinistro_rate_pct) < 1  THEN 'muito baixo'
      WHEN COALESCE(s.indice_roubo_pct, s.sinistro_rate_pct) < 2  THEN 'baixo'
      WHEN COALESCE(s.indice_roubo_pct, s.sinistro_rate_pct) < 4  THEN 'médio'
      WHEN COALESCE(s.indice_roubo_pct, s.sinistro_rate_pct) < 8  THEN 'alto'
      ELSE 'muito alto'
    END AS risco_bucket
  FROM fipe_base f
  LEFT JOIN susep_base s USING (chave_join)
  LEFT JOIN seg_agg     a USING (chave_join)
)

SELECT * FROM joined;
