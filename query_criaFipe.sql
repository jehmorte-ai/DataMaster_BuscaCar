#standardSQL
CREATE OR REPLACE TABLE `fipe_silver.fipe` AS
SELECT
  SAFE_CAST(marca AS STRING) AS marca,
  SAFE_CAST(modelo AS STRING) AS modelo,
  LOWER(REGEXP_REPLACE(modelo, r'[^a-zA-Z0-9]', '_')) AS modelo_slug,
  SAFE_CAST(ano AS INT64) AS ano_modelo,
  SAFE_CAST(combustivel AS STRING) AS tipo_combustivel,

  -- Conversão de "R$ 45.000,00" para FLOAT64
  SAFE_CAST(
    REPLACE(
      REPLACE(
        REGEXP_REPLACE(valor, r'[^\d,]', ''), 
        '.', ''
      ), 
      ',', '.'
    ) AS FLOAT64
  ) AS valor_float,

  -- Extração do mês e ano da string "Junho de 2024"
  REGEXP_EXTRACT(data_consulta, r'(\w+)') AS mes_extenso,
  CAST(REGEXP_EXTRACT(data_consulta, r'(\d{4})') AS INT64) AS ano_referencia,

  -- Conversão do nome do mês para número
  CASE
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'janeiro' THEN 1
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'fevereiro' THEN 2
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'março' THEN 3
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'abril' THEN 4
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'maio' THEN 5
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'junho' THEN 6
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'julho' THEN 7
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'agosto' THEN 8
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'setembro' THEN 9
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'outubro' THEN 10
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'novembro' THEN 11
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'dezembro' THEN 12
  ELSE NULL
END AS mes_referencia,

  -- Composição da data
  DATE(CONCAT(
  REGEXP_EXTRACT(data_consulta, r'(\d{4})'), '-',
  LPAD(
    CAST(
      CASE
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'janeiro' THEN 1
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'fevereiro' THEN 2
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'março' THEN 3
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'abril' THEN 4
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'maio' THEN 5
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'junho' THEN 6
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'julho' THEN 7
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'agosto' THEN 8
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'setembro' THEN 9
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'outubro' THEN 10
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'novembro' THEN 11
  WHEN REGEXP_EXTRACT(LOWER(TRIM(data_consulta)), r'(\w+)') = 'dezembro' THEN 12
  ELSE NULL
END AS STRING
    ), 2, '0'
  ),
  '-01'
)) AS data_referencia,

  -- Rastreabilidade
  origem_arquivo,
  DATETIME(TIMESTAMP(data_ingestao)) AS data_ingestao

FROM `fipe_bronze.raw_fipe`;