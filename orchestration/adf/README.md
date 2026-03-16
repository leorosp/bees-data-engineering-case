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
