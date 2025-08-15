#BuscaCar - Plataforma de Análise e Comparação de Veículos

##Objetivo

Este projeto tem como objetivo criar uma plataforma de engenharia de dados voltada à análise de preços de veículos com base na Tabela FIPE, cruzando essas informações com dados fictícios de seguros e indice de roubo de veiculos (coletados da Susep). A solução visa fornecer aos usuários insights de valor, uma visão de roubo do veiculo e previsibilidade de preços e custos associados à posse de um veículo.
> **Público-alvo:** Lojistas do setor automotivo, compradores exigentes, entusiastas de carros e analistas de mercado.

##Objetivos do Projeto

- Realizar a extração automatizada dos dados da Tabela FIPE.
- Criar uma base comparativa com valores históricos de veículos.
- Simular análises de custos usando dados de seguros fictícios.
- Realizar a extração automatizada dos dados da Susep.
- Fornecer visualizações acessíveis via dashboard.
- Estruturar um pipeline de dados robusto, escalável e observável.

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
   - Utilizado UpTimeRobot

##Tecnologias Utilizadas

| Componente       | Tecnologia                    |
|------------------|-------------------------------|
| Armazenamento    | Google Cloud Storage + BigQuery |
| Coleta de Dados  | Web Scraping com Python (BeautifulSoup) |
| Visualização     | Power BI  					   |
| Segurança        | GCP DLP + Regras IAM          |
| Observabilidade  | UpTimeRobot		           |

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
