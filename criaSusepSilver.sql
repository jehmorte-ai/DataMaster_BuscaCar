-- cria/atualiza a Silver da SUSEP
CREATE OR REPLACE TABLE `silver.susep` 
PARTITION BY DATE(data_extracao) AS

WITH src AS (
  SELECT
    -- tenta usar colunas já normalizadas; se não existirem, extrai da primeira coluna "0"
    UPPER(TRIM(COALESCE(CAST(marca AS STRING),
                        -- pega a primeira palavra como marca, se vier "MARCA MODELO"
                        SPLIT(CAST('0' AS STRING), ' ')[OFFSET(0)])))                   AS marca_raw,
    TRIM(COALESCE(CAST(modelo_susep AS STRING),
                  -- remove a primeira palavra (marca) e deixa o resto como modelo
                  REGEXP_REPLACE(CAST('0' AS STRING), r'^\s*\S+\s*', '')))             AS modelo_raw,

    -- números: índice %, expostos, sinistros (aceita “1.234,56”, “0,00 %”, etc.)
    CAST(REPLACE(REPLACE(REGEXP_REPLACE(
           COALESCE(CAST(indice_roubo_percentual AS STRING), CAST('1' AS STRING), ''), r'[^0-9,.\-]', ''), '.', ''), ',', '.') AS FLOAT64) AS indice_roubo_pct,
    CAST(REPLACE(REPLACE(REGEXP_REPLACE(
           COALESCE(CAST(veiculos_expostos AS STRING), CAST('2' AS STRING), ''), r'[^0-9,.\-]', ''), '.', ''), ',', '.') AS FLOAT64) AS veiculos_expostos,
    CAST(REPLACE(REPLACE(REGEXP_REPLACE(
           COALESCE(CAST(numero_sinistros AS STRING), CAST('3' AS STRING), ''), r'[^0-9,.\-]', ''), '.', ''), ',', '.') AS FLOAT64) AS sinistros,

    -- data de extração (já vem na Bronze)
    TIMESTAMP(COALESCE(CAST(data_extracao AS STRING), CAST(CURRENT_TIMESTAMP() AS STRING))) AS data_extracao
  FROM `optical-victor-463515-v8.bronze.raw_susep`
  -- filtra possíveis linhas de cabeçalho/lixo
  WHERE
    SAFE_CAST('0' AS STRING) IS NULL
    OR (UPPER(CAST('0' AS STRING)) NOT LIKE 'FABRICANTE%')
),
norm AS (
  SELECT
    UPPER(TRIM(marca_raw)) AS marca,
    TRIM(modelo_raw)       AS modelo,
    indice_roubo_pct,
    veiculos_expostos,
    sinistros,
    SAFE_DIVIDE(sinistros, NULLIF(veiculos_expostos, 0)) * 100 AS sinistro_rate_pct,
    data_extracao,

    -- slug para JOIN (remove acentos e caracteres não [a-z0-9], troca por "_")
    LOWER(REGEXP_REPLACE(
      TRANSLATE(CONCAT(src.marca_raw, ' ', src.modelo_raw),
        'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'),
      r'[^a-z0-9]+', '_')) AS chave_join
  FROM src
  -- descarta linhas totalmente vazias
  WHERE COALESCE(src.marca_raw, '') <> '' AND COALESCE(src.modelo_raw, '') <> ''
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY chave_join ORDER BY data_extracao DESC) AS rn
  FROM norm
)
SELECT
  marca,
  modelo,
  chave_join,
  indice_roubo_pct,
  veiculos_expostos,
  sinistros,
  sinistro_rate_pct,
  data_extracao
FROM dedup
WHERE rn = 1;