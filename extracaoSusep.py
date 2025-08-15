# -*- coding: utf-8 -*-
"""
Created on Fri Aug 15 11:36:09 2025

@author: Jessica
"""

import os, sys
import requests, pandas as pd, re, os, urllib3
from bs4 import BeautifulSoup
from unidecode import unidecode
from datetime import datetime
from google.cloud import storage
from datetime import datetime
from google.cloud import storage, bigquery
from datetime import datetime
import pandas as pd
from io import StringIO

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
URL = "https://www2.susep.gov.br/menuestatistica/rankroubo/resp_menu1.asp"

sys.path.append(os.path.join(os.path.dirname(__file__), "utils"))  # se precisar


from obs_logging import log_job  # agora é .py

with log_job("extract_susep"):

    # pasta de saída
    PASTA_OUT = r"C:\Users\Jessica\Desktop\DataMaster\Cargas_Susep"
    os.makedirs(PASTA_OUT, exist_ok=True)
    ARQ_OUT = os.path.join(PASTA_OUT, f"susep_normalizado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

def norm_header(s):
    s = str(s).strip()
    s = unidecode(s).lower()
    s = re.sub(r'\s+', '_', s)
    s = s.replace('(*)','').replace('%','pct')
    return s

def to_float(x):
    if pd.isna(x): return None
    s = str(x).strip().replace('.', '').replace(',', '.')
    s = re.sub(r'[^0-9\.\-]', '', s)
    try: return float(s) if s not in ('','-') else None
    except: return None

COMPOSTAS = {
    'ALFA ROMEO','MERCEDES BENZ','MERCEDES-BENZ','ROLLS ROYCE','LAND ROVER',
    'AGRALE CAMINHOES','AGRALE TRATORES','CHERY EQ'
}

def split_marca_modelo(raw):
    if pd.isna(raw): return None, None
    s = unidecode(str(raw)).upper().strip()
    s = re.sub(r'\s+',' ', s)
    if ' - ' in s:
        m, md = s.split(' - ',1)
        return m.strip(), md.strip()
    for comp in sorted(COMPOSTAS, key=len, reverse=True):
        c = unidecode(comp).upper()
        if s.startswith(c+' '):
            return comp.replace('-',' ').upper(), s[len(c):].strip()
    parts = s.split(' ',1)
    m = parts[0]
    md = parts[1].strip() if len(parts)>1 else ''
    return m, md

def main():
    resp = requests.post(URL, verify=False, timeout=30)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if table is None:
        raise ValueError("Nenhuma tabela encontrada na página da SUSEP.")

    df = pd.read_html(str(table), decimal=",", thousands=".")[0]
    # promove primeira linha a header
    header = df.iloc[0].tolist()
    df = df.iloc[1:].reset_index(drop=True)
    df.columns = [norm_header(h) for h in header]
    df.columns = [c.replace('/','_').replace('__','_') for c in df.columns]

    col_modelo  = next((c for c in df.columns if c.startswith('modelo')), None)
    col_indice  = next((c for c in df.columns if 'indice' in c and 'pct' in c), None)
    col_exposto = next((c for c in df.columns if 'expostos' in c), None)
    col_sinist  = next((c for c in df.columns if 'sinistro' in c), None)
    if not col_modelo or not col_indice:
        raise ValueError(f"Colunas esperadas não encontradas. Colunas: {df.columns.tolist()}")

    df['indice_roubo_percentual'] = df[col_indice].apply(to_float)
    if col_exposto: df['veiculos_expostos'] = df[col_exposto].apply(to_float)
    if col_sinist:  df['numero_sinistros'] = df[col_sinist].apply(to_float)

    marca_modelo = df[col_modelo].apply(split_marca_modelo)
    df['marca'] = marca_modelo.apply(lambda x: x[0])
    df['modelo_susep'] = marca_modelo.apply(lambda x: x[1])
    df = df[df['marca'].notna() & (df['marca'].astype(str).str.len()>0)].copy()

    df['marca'] = df['marca'].str.replace(r'\s+',' ', regex=True).str.strip()
    df['modelo_susep'] = df['modelo_susep'].str.replace(r'\s+',' ', regex=True).str.strip()
    df['data_extracao'] = datetime.now()

    cols_ordem = ['marca','modelo_susep','indice_roubo_percentual','veiculos_expostos','numero_sinistros','data_extracao']
    cols_exist = [c for c in cols_ordem if c in df.columns]
    df[cols_exist].to_csv(ARQ_OUT, index=False, encoding='utf-8-sig')
    print(f" SUSEP normalizado salvo em: {ARQ_OUT}")

if __name__ == "__main__":
    main()
    
#etapa que carrega os arquivos para o bukcet

with log_job("ingest_susep_upload_gcs"):
    
    # CONFIGURAÇÕES
    caminho_chave = r"C:\Users\Jessica\Desktop\DataMaster\fipe-sa.json"  
    bucket_name = "bronze_susep" #alterar nome da pasta de acordo com a carga
    pasta_origem = r"C:\Users\Jessica\Desktop\DataMaster\Cargas_Susep" #alterar o nome de acordo da carga
    prefixo_gcs = "raw/"  # pasta no bucket
    
    # Autentica no GCP
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = caminho_chave
    client = storage.Client()
    
    bucket = client.bucket(bucket_name)
    
    # Pega todos os arquivos .csv da pasta
    arquivos = [f for f in os.listdir(pasta_origem) if f.endswith(".csv")]
    
    for arquivo in arquivos:
        caminho_local = os.path.join(pasta_origem, arquivo)
        destino_gcs = prefixo_gcs + arquivo
    
        blob = bucket.blob(destino_gcs)
        blob.upload_from_filename(caminho_local)
    
        print(f" Arquivo enviado: {arquivo} → gs://{bucket_name}/{destino_gcs}")
        
with log_job("bronze_susep_build"):       
        
    #Criar a bronze
    # Configurações
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Jessica\Desktop\DataMaster\fipe-sa.json"
    
    BUCKET_NAME = "bronze_susep" #alterar o nome do bucket
    PASTA_BUCKET = "raw/"
    DATASET_ID = "bronze"
    TABELA_ID = "raw_susep"
    PROJETO_ID = "optical-victor-463515-v8"
    
    # Inicializa clientes
    storage_client = storage.Client()
    bq_client = bigquery.Client()
    
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = list(storage_client.list_blobs(bucket, prefix=PASTA_BUCKET))
    
    # Processa cada arquivo CSV
    todos_df = []
    
    for blob in blobs:
        if blob.name.endswith(".csv"):
            print(f" Lendo {blob.name}...")
    
            conteudo = blob.download_as_text()
            df = pd.read_csv(StringIO(conteudo))
    
            df["origem_arquivo"] = blob.name.split("/")[-1]
            df["data_ingestao"] = datetime.utcnow()
    
            todos_df.append(df)
    
    # Empilha tudo
    df_final = pd.concat(todos_df, ignore_index=True)
    
    # Configurações de schema (BigQuery detecta automaticamente)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # sobrescreve a tabela
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET if False else bigquery.SourceFormat.CSV,
    )
    
    # Cria job de upload
    table_ref = f"{PROJETO_ID}.{DATASET_ID}.{TABELA_ID}"
    job = bq_client.load_table_from_dataframe(df_final, table_ref, job_config=job_config)
    job.result()
    
    print(f" Dados carregados com sucesso para {table_ref}")
    
    pass
