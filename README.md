BuscaCar - Plataforma de Análise e Comparação de Veículos

## Sumário
- [Objetivo](#objetivo)
- [Arquitetura Conceitual](#arquitetura-conceitual)
- [Arquitetura Técnica & Sequência](#arquitetura-técnica--sequência)
- [Cronograma (Gantt)](#cronograma-gantt)
- [Diagramas do Projeto](#diagramas-projeto)
- [Execução (como rodar)](#execução-como-rodar)
- [Observabilidade](#observabilidade)
- [Segurança & Mascaramento](#segurança--mascaramento)
- [Reprodutibilidade](#reprodutibilidade)
- [Melhorias Futuras](#melhorias-futuras)
- [Licença](#licença)
  
## Objetivo

O BuscaCar é uma solução de Engenharia de Dados voltada para análise e comparação de preços de veículos, utilizando a Tabela FIPE, cruzando com dados fictícios de seguros e índices de roubo de veículos (SUSEP).
O objetivo é fornecer insights de valor, visão de risco de roubo e previsibilidade de preços e custos associados à posse de um veículo.
Público-alvo: lojistas do setor automotivo, compradores exigentes, entusiastas de carros e analistas de mercado.

Objetivos do Projeto

Coleta automatizada de dados da Tabela FIPE.
Base comparativa com histórico de valores de veículos.
Análises de custo usando dados de seguros fictícios.
Coleta automatizada de dados da SUSEP.
Visualizações acessíveis via dashboard.
Pipeline de dados robusto, escalável e observável.

## Arquitetura Conceitual

1. **Coleta de Dados**
   - Web scraping da Tabela FIPE.
   - Ingestão de dados fictícios de seguros (CSV manual).
   - Web scraping da Tabela SUSEP.
   
2. **Camadas de Processamento**
   - **Bronze:** Dados brutos da FIPE, SUSEP e seguros.
   - **Silver:** Dados tratados, formatados e limpos.
   - **Gold:** Dados prontos para análise e consumo de BI.

3. **Armazenamento**
   - Google Cloud Storage (staging).
   - BigQuery como data warehouse.
     
4. **Visualização**
   - Power BI.

5. **Observabilidade e Segurança** 
   Observabilidade: monitoramento de pipelines com UpTimeRobot e logs no BigQuery.
   Segurança: controle de acesso via IAM, mascaramento de dados sensíveis com GCP DLP.
   
## Arquitetura de solução

flowchart LR
    A[Coleta de Dados] --> B[Camada Bronze]
    B --> C[Camada Prata]
    C --> D[Camada Gold]
    D --> E[Power BI]
    subgraph Infraestrutura GCP
        B
        C
        D
        F[Google Cloud Storage]
        G[BigQuery]
    end

##Tecnologias Utilizadas

| Componente      | Tecnologia                         |
| --------------- | ---------------------------------- |
| Armazenamento   | Google Cloud Storage + BigQuery    |
| Coleta de Dados | Python (BeautifulSoup, Requests)   |
| Orquestração    | Terraform (infraestrutura buckets) |
| Visualização    | Power BI                           |
| Segurança       | GCP DLP + IAM                      |
| Observabilidade | UpTimeRobot + logs no BigQuery     |

## Diagramas do Projeto

### Arquitetura Técnica
```mermaid
flowchart LR
    %% Arquitetura Técnica BuscaCar

    subgraph Fontes["Fontes de Dados"]
      FIPE["FIPE (web)"]
      SUSEP["SUSEP (web)"]
      SEGUROS["Seguros (CSV fictício)"]
    end

    subgraph ETL["ETL / Ingestão"]
      FIPE_SCRIPT["ExtracaoFipeNovoComLog.py"]
      SUSEP_SCRIPT["extracaoSusep.py"]
      CONF_SCRIPT["Conformidade.py"]
    end

    subgraph GCP["GCP"]
      GCS["Google Cloud Storage (Bronze files)"]
      BQ_BRONZE["BigQuery (Bronze)"]
      BQ_PRATA["BigQuery (Prata)"]
      BQ_GOLD["BigQuery (Gold)"]
    end

    subgraph OBS["Observabilidade & Segurança"]
      LOG["obs_logging.py -> BigQuery: obs.run_log"]
      UPTIME["UpTimeRobot (health-check)"]
      IAM["IAM (perfis mínimos)"]
      DLP["GCP DLP (mascaramento)"]
    end

    subgraph BI["Consumo"]
      PBI["Power BI (dashboards)"]
      USER["Usuários finais"]
    end

    %% Fluxo
    FIPE -->|scraping| FIPE_SCRIPT
    SUSEP -->|scraping| SUSEP_SCRIPT
    SEGUROS -->|CSV| FIPE_SCRIPT
    SEGUROS -->|CSV| SUSEP_SCRIPT

    FIPE_SCRIPT -->|upload| GCS
    SUSEP_SCRIPT -->|upload| GCS
    FIPE_SCRIPT -->|log| LOG
    SUSEP_SCRIPT -->|log| LOG

    GCS --> BQ_BRONZE
    BQ_BRONZE -->|padronização/limpeza| BQ_PRATA
    BQ_PRATA -->|conformidade nomes| CONF_SCRIPT
    CONF_SCRIPT -->|output tratado| BQ_PRATA
    CONF_SCRIPT -->|log| LOG

    BQ_PRATA -->|marts/métricas| BQ_GOLD
    BQ_GOLD --> PBI --> USER

    UPTIME -. consulta .-> LOG
    IAM -. controla acesso .- GCS
    IAM -. controla acesso .- BQ_GOLD
    DLP -. mascara campos sensíveis .- BQ_PRATA

## Execução do Projeto

# Clone de Buckets GCP com Terraform

Este pacote recria os buckets do projeto original usando Terraform.

## Pré-requisitos

- Terraform instalado
- Autenticação com `gcloud auth application-default login`
- Permissões para criar buckets no projeto de destino

## Como usar

1. Edite o `project_id` diretamente no terminal:

```bash
terraform init
terraform apply -var="project_id=meu-projeto-clone"
```

2. Confirme com `yes` quando solicitado.

## Observações

- Os buckets serão criados com as mesmas configurações de nome, região e classe de armazenamento.
- Certifique-se de que os nomes dos buckets não estejam em uso globalmente (nomes de buckets são únicos no mundo).

Apos o ambiente instalado rode os codigos py



1 - ExtracaoFipeNovoComLog.py
2 - extracaoSusep.py

## Observabilidade

Monitoramento de jobs via UpTimeRobot (verifica logs de execução no BigQuery).
Tabela de logs (obs.run_log) contendo status, horário de início/fim e mensagens de erro.
Alertas configurados para falhas na ingestão.

#Segurança

IAM: acesso restrito a usuários autorizados.
DLP: mascaramento de campos sensíveis (placa, CPF, etc.).
Criptografia: dados criptografados em repouso e em trânsito.

#Reprodutibilidade

Este repositório contém:
Scripts de ingestão.
Código de transformação.
Configurações Terraform.
Instruções no README.
Passos para reprodução:
Criar projeto no GCP.
Configurar autenticação.
Criar buckets e datasets com Terraform.
Executar scripts de ingestão.
Publicar dashboard no Power BI.
