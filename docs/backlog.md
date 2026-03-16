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

- [ ] Provisionar ADLS Gen2
- [ ] Provisionar Azure Data Factory
- [ ] Provisionar Azure Databricks
- [ ] Provisionar Azure Key Vault
- [ ] Provisionar Log Analytics Workspace
- [ ] Provisionar Azure Monitor Action Group

**Saida esperada**
- stack Azure minima criada e acessivel

## Fase 2 - Bronze ingestion

- [ ] Modelar pipeline ADF para consumir a Open Brewery DB API
- [ ] Configurar paginacao e politicas de retry
- [ ] Persistir json bruto em `bronze`
- [ ] Criar naming pattern por data de ingestao
- [ ] Registrar logs de execucao

**Saida esperada**
- ingestao funcionando ponta a ponta

## Fase 3 - Silver transformation

- [ ] Criar notebook/job Databricks para leitura do bronze
- [ ] Padronizar schema e tipos
- [ ] Tratar duplicidades
- [ ] Criar tabela Delta silver
- [ ] Particionar por `country` e `state_province`

**Saida esperada**
- camada silver confiavel e reprocessavel

## Fase 4 - Gold analytics

- [ ] Criar agregacoes por `brewery_type`
- [ ] Criar agregacoes por localizacao
- [ ] Criar agregacao combinada por tipo e localizacao
- [ ] Publicar tabela gold final

**Saida esperada**
- camada gold pronta para consumo

## Fase 5 - Data quality

- [ ] Definir contratos minimos de qualidade
- [ ] Implementar checks de null, schema drift, duplicidade e contagem minima
- [ ] Persistir resultados em `ops.quality_logs`
- [ ] Falhar pipeline quando regras criticas forem violadas

**Saida esperada**
- qualidade de dados automatizada

## Fase 6 - Observabilidade

- [ ] Enviar diagnosticos do ADF para Log Analytics
- [ ] Enviar logs do Databricks para monitoramento
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

- [ ] Criar pipeline GitHub Actions para lint e testes
- [ ] Automatizar deploy do Bicep
- [ ] Versionar notebooks e configuracoes
- [ ] Documentar runbook de operacao
- [ ] Revisar custo e otimizar jobs

**Saida esperada**
- projeto pronto para entrega tecnica
