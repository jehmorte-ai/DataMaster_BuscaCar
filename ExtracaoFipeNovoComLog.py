# -*- coding: utf-8 -*-
"""
Created on Fri Aug 15 12:21:58 2025

@author: Jessica
"""

import requests
import pandas as pd
import time
from datetime import datetime
import os
from google.cloud import storage, bigquery
from datetime import datetime
import pandas as pd
from io import StringIO

# Gerar data e hora
agora = datetime.now().strftime("%Y%m%d_%H%M%S")

BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"
HEADERS = {
    'Content-Type': 'application/json',
    'Referer': 'https://veiculos.fipe.org.br/',
}

from obs_logging import log_job  

with log_job("extract_fipe"):

    def safe_post(endpoint, payload=None):
        try:
            r = requests.post(f"{BASE_URL}/{endpoint}", headers=HEADERS, json=payload)
            if r.status_code == 200 and r.text.strip():
                return r.json()
            else:
                print(f"[ERRO] Status: {r.status_code} | Endpoint: {endpoint}")
                print(f"Resposta: {r.text}")
                return None
        except Exception as e:
            print(f"[EXCEÇÃO] Falha ao acessar {endpoint}: {e}")
            return None
        
    #Escolher o mês de referencia da FIPE 0 = mês atual, 1 mês anterior
    def get_referencias():
        data = safe_post("ConsultarTabelaDeReferencia")
        return data[0]['Codigo'] if data else None
    
    def get_marcas(tipo, tabela):
        payload = {"codigoTabelaReferencia": tabela, "codigoTipoVeiculo": tipo}
        return safe_post("ConsultarMarcas", payload) or []
    
    def get_modelos(tipo, tabela, marca):
        payload = {
            "codigoTabelaReferencia": tabela,
            "codigoTipoVeiculo": tipo,
            "codigoMarca": marca
        }
        data = safe_post("ConsultarModelos", payload)
        return data.get("Modelos", []) if data else []
    
    def get_anos(tipo, tabela, marca, modelo):
        payload = {
            "codigoTabelaReferencia": tabela,
            "codigoTipoVeiculo": tipo,
            "codigoMarca": marca,
            "codigoModelo": modelo
        }
        return safe_post("ConsultarAnoModelo", payload) or []
    
    def get_preco(tipo, tabela, marca, modelo, ano_modelo, combustivel):
        payload = {
            "codigoTabelaReferencia": tabela,
            "codigoMarca": marca,
            "codigoModelo": modelo,
            "codigoTipoVeiculo": tipo,
            "anoModelo": int(ano_modelo),
            "codigoTipoCombustivel": int(combustivel),
            "tipoConsulta": "tradicional"
        }
        return safe_post("ConsultarValorComTodosParametros", payload)
    
    # Início do processo
    TIPO_VEICULO = 1  # 1 = Carro
    tabela_id = get_referencias()
    
    if not tabela_id:
        print("Não foi possível obter o código da tabela de referência.")
        exit()
    
    marcas = get_marcas(TIPO_VEICULO, tabela_id)
    dados = []
    
    for marca in marcas[:90]:
        marca_nome = marca['Label']
        marca_valor = marca['Value']
        modelos = get_modelos(TIPO_VEICULO, tabela_id, marca_valor)
        
        for modelo in modelos[:20]:
            modelo_nome = modelo['Label']
            modelo_valor = modelo['Value']
            anos = get_anos(TIPO_VEICULO, tabela_id, marca_valor, modelo_valor)
    
            for ano in anos[:1]:
                try:
                    ano_valor, combustivel = ano['Value'].split("-")
                    preco = get_preco(TIPO_VEICULO, tabela_id, marca_valor, modelo_valor, ano_valor, combustivel)
    
                    if preco:
                        dados.append({
                            "marca": marca_nome,
                            "modelo": modelo_nome,
                            "ano": preco.get("AnoModelo"),
                            "combustivel": preco.get("Combustivel"),
                            "valor": preco.get("Valor"),
                            "data_consulta": preco.get("MesReferencia")
                        })
    
                except Exception as e:
                    print(f"[EXCEÇÃO] Erro ao processar {marca_nome} - {modelo_nome}: {e}")
    
                time.sleep(0.5)
    
    # Salvar resultado
    df_fipe = pd.DataFrame(dados)
    print(df_fipe.head())
    
    caminho_pasta = r"C:\Users\Jessica\Desktop\DataMaster\Cargas_fipe"
    os.makedirs(caminho_pasta, exist_ok=True)
    
    nome_arquivo = f"fipe_veiculos_{agora}.csv"
    caminho_completo = os.path.join(caminho_pasta, nome_arquivo)
    
    df_fipe.to_csv(caminho_completo, index=False, encoding='utf-8-sig')
    print(f" Arquivo salvo em: {caminho_completo}")

    with log_job("ingest_fipe_upload_gcs"):
     
        #etapa que carrega os arquivos para o bukcet
        
        from google.cloud import storage
        from datetime import datetime
        import os
        
        # CONFIGURAÇÕES
        caminho_chave = r"C:\Users\Jessica\Desktop\DataMaster\fipe-sa.json"  
        bucket_name = "bronze_susep" #alterar nome da pasta de acordo com a carga
        pasta_origem = r"C:\Users\Jessica\Desktop\DataMaster\Cargas_fipe" #alterar o nome de acordo da carga
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
            
with log_job("bronze_fipe_build"):           
        #Criar a bronze
    
    # Configurações
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Jessica\Desktop\DataMaster\fipe-sa.json"
    
    BUCKET_NAME = "fipe-bronze" #alterar o nome do bucket
    PASTA_BUCKET = "raw/"
    DATASET_ID = "bronze"
    TABELA_ID = "raw_fipe"
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