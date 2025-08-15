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
