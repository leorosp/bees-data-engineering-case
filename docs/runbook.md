# Runbook

## Ordem de execucao recomendada

1. Provisionar a infraestrutura com `Bicep`
2. Publicar os notebooks no Azure Databricks
3. Ajustar os placeholders dos linked services do ADF
4. Publicar o pipeline `pl_bees_orchestration`
5. Executar o pipeline com `basePath`, `landingDate` e `runId`
6. Validar as tabelas `bronze`, `silver`, `gold` e `ops`

## Parametros minimos do pipeline

- `basePath`: caminho raiz no ADLS Gen2
- `landingDate`: data logica da ingestao
- `runId`: identificador unico da execucao

## Evidencias de sucesso

- arquivos raw em `bronze/landing_date=...`
- Delta table em `silver/breweries`
- Delta table em `gold/breweries_by_type_location`
- quality logs em `ops/quality_results`
- execution logs em `ops/execution_events`

## Pendencias para producao

- role assignments dos managed identities
- diagnosticos completos do ADF para Log Analytics
- alertas de monitoramento conectados ao Action Group
- automacao do deploy via GitHub Actions
