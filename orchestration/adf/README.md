# Azure Data Factory

Esta pasta vai concentrar os artefatos de orquestracao:

- pipelines
- datasets
- linked services
- triggers
- parametros globais

Fluxo esperado na V1:

1. pipeline de ingestao REST
2. gravacao bronze no ADLS Gen2
3. chamada de notebook/job no Databricks
4. atualizacao de status e telemetria

Estrutura sugerida:

```text
orchestration/adf/
|- README.md
|- datasets/
|- linkedServices/
|- pipelines/
`- triggers/
```

Arquivos ja adicionados:

- `linkedServices/ls_key_vault.json`
- `linkedServices/ls_azure_databricks_workspace.json`
- `pipelines/pl_bees_orchestration.json`
