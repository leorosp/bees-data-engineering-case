# Backlog de implementacao

## Fase 0 - Foundation

- [ ] Definir nome final do projeto, naming convention e tags Azure
- [ ] Criar Resource Group de `dev`
- [ ] Criar Bicep base para infraestrutura
- [ ] Configurar repositorio no GitHub com branch protection e templates

**Saida esperada**
- repositorio organizado
- padroes de nomenclatura definidos
- infraestrutura pronta para evoluir

## Fase 1 - Infraestrutura Azure

- [x] Provisionar ADLS Gen2
- [x] Provisionar Azure Data Factory
- [x] Provisionar Azure Databricks
- [x] Provisionar Azure Key Vault
- [x] Provisionar Log Analytics Workspace
- [x] Provisionar Azure Monitor Action Group

**Saida esperada**
- stack Azure minima criada e acessivel

## Fase 2 - Bronze ingestion

- [x] Modelar pipeline ADF para consumir a Open Brewery DB API
- [x] Configurar paginacao e politicas de retry
- [x] Persistir json bruto em `bronze`
- [x] Criar naming pattern por data de ingestao
- [x] Registrar logs de execucao

**Saida esperada**
- ingestao funcionando ponta a ponta

## Fase 3 - Silver transformation

- [x] Criar notebook/job Databricks para leitura do bronze
- [x] Padronizar schema e tipos
- [x] Tratar duplicidades
- [x] Criar tabela Delta silver
- [x] Particionar por `country` e `state_province`

**Saida esperada**
- camada silver confiavel e reprocessavel

## Fase 4 - Gold analytics

- [x] Criar agregacoes por `brewery_type`
- [x] Criar agregacoes por localizacao
- [x] Criar agregacao combinada por tipo e localizacao
- [x] Publicar tabela gold final

**Saida esperada**
- camada gold pronta para consumo

## Fase 5 - Data quality

- [x] Definir contratos minimos de qualidade
- [x] Implementar checks de null, schema drift, duplicidade e contagem minima
- [x] Persistir resultados em `ops.quality_logs`
- [ ] Falhar pipeline quando regras criticas forem violadas

**Saida esperada**
- qualidade de dados automatizada

## Fase 6 - Observabilidade

- [ ] Enviar diagnosticos do ADF para Log Analytics
- [x] Enviar logs do Databricks para monitoramento
- [ ] Criar alertas para falha de pipeline e falha de qualidade
- [ ] Configurar notificacao por e-mail/webhook

**Saida esperada**
- monitoramento e alertas operacionais ativos

## Fase 7 - Consumo

- [ ] Publicar dataset para Power BI
- [ ] Criar dashboard executivo basico
- [ ] Criar pagina tecnica com metricas do pipeline

**Saida esperada**
- camada de consumo pronta para demonstracao

## Fase 8 - CI/CD e hardening

- [x] Criar pipeline GitHub Actions para lint e testes
- [ ] Automatizar deploy do Bicep
- [ ] Versionar notebooks e configuracoes
- [ ] Documentar runbook de operacao
- [ ] Revisar custo e otimizar jobs

**Saida esperada**
- projeto pronto para entrega tecnica
