# Runbook

## Ordem de execucao recomendada

1. Provisionar a infraestrutura com `Bicep`
2. Executar primeiro os notebooks manualmente no Azure Databricks
3. Validar as camadas `bronze`, `silver`, `gold` e `ops`
4. Ajustar os placeholders dos linked services do ADF
5. Publicar o pipeline `pl_bees_orchestration`
6. Executar a orquestracao completa pelo ADF

## Parametros minimos do pipeline

- `basePath`: caminho raiz do filesystem `lake` no ADLS Gen2
- `landingDate`: data logica da ingestao
- `runId`: identificador unico da execucao

Formato esperado de `basePath`:

```text
abfss://lake@<storage-account>.dfs.core.windows.net
```

## Evidencias de sucesso

- arquivos raw em `bronze/landing_date=...`
- Delta table em `silver/breweries`
- Delta table em `gold/breweries_by_type_location`
- quality logs em `ops/quality_results`
- execution logs em `ops/execution_events`

## Sequencia de teste recomendada

1. Fazer o deploy do `Bicep` e anotar o nome final da `Storage Account`.
2. Montar o `basePath` com o formato `abfss://lake@<storage-account>.dfs.core.windows.net`.
3. Abrir o Azure Databricks, criar um cluster simples de teste e clonar este repositorio em um `Git folder`.
4. Rodar os notebooks na ordem `bronze`, `silver`, `gold` e `quality` usando o mesmo `basePath`.
5. So depois configurar o `ADF` para chamar esses mesmos notebooks de forma orquestrada.

## Pendencias para producao

- role assignments dos managed identities
- diagnosticos completos do ADF para Log Analytics
- alertas de monitoramento conectados ao Action Group
- automacao do deploy via GitHub Actions
