-- 1) Cria o schema "ref" (se ainda não existir)
CREATE SCHEMA IF NOT EXISTS `optical-victor-463515-v8.ref`;

-- 2) Padroniza a tabela de conformidade em um formato canônico
CREATE OR REPLACE TABLE `optical-victor-463515-v8.ref.mapeamento_susep_fipe` AS
WITH slug AS (
  SELECT 1 AS k,
         'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ' AS A,
         'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC' AS B
),
src AS (
  SELECT
    UPPER(TRIM(CAST(fipe_marca   AS STRING))) AS marca_fipe,
    TRIM(CAST(fipe_modelo  AS STRING))        AS modelo_fipe,
    UPPER(TRIM(CAST(susep_marca  AS STRING))) AS marca_susep,
    TRIM(CAST(susep_modelo AS STRING))        AS modelo_susep,
  FROM `optical-victor-463515-v8.bronze.raw_conformidade`
),
norm AS (
  SELECT
    marca_fipe, modelo_fipe, marca_susep, modelo_susep,
    -- chaves (mesma regra usada na FIPE/SUSEP)
    REGEXP_REPLACE(LOWER(TRANSLATE(CONCAT(marca_fipe,' ',modelo_fipe),(SELECT A FROM slug),(SELECT B FROM slug))), r'[^a-z0-9]+','_') AS chave_fipe_join,
    REGEXP_REPLACE(LOWER(TRANSLATE(marca_fipe,(SELECT A FROM slug),(SELECT B FROM slug))), r'[^a-z0-9]+','_')                                                               AS chave_fipe_marca,
    REGEXP_REPLACE(LOWER(TRANSLATE(CONCAT(marca_susep,' ',modelo_susep),(SELECT A FROM slug),(SELECT B FROM slug))), r'[^a-z0-9]+','_') AS chave_susep_join,
    REGEXP_REPLACE(LOWER(TRANSLATE(marca_susep,(SELECT A FROM slug),(SELECT B FROM slug))), r'[^a-z0-9]+','_')                                                               AS chave_susep_marca
  FROM src
)
SELECT DISTINCT * FROM norm;