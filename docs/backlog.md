# Backlog de implementacao

## Fase 0 - Foundation

- [x] Definir nome final do projeto
- [x] Organizar repositorio no GitHub
- [x] Definir arquitetura medallion
- [x] Criar documentacao inicial

## Fase 1 - Pipeline Colab/PySpark

- [x] Criar ingestao `bronze`
- [x] Criar transformacao `silver`
- [x] Criar agregacoes `gold`
- [x] Criar camada `ops`

## Fase 2 - Data quality

- [x] Definir contratos minimos
- [x] Implementar checks de campos obrigatorios
- [x] Implementar checks de duplicidade
- [x] Persistir resultados em `ops`
- [x] Falhar execucao automaticamente quando regra critica for violada

## Fase 3 - Evidencia do case

- [x] Validar execucao de referencia
- [x] Validar o quality gate com dataset problemático
- [x] Documentar evidencias no repositorio
- [x] Criar quickstart para avaliacao

## Fase 4 - Consumo

- [x] Criar dashboard executivo e operacional em Streamlit
- [ ] Capturar screenshots e publicar no README
- [ ] Criar versao resumida para apresentacao

## Fase 5 - Evolucao para GCP

- [ ] Adaptar persistencia para `Google Cloud Storage`
- [ ] Executar o pipeline em `Dataproc Serverless`
- [ ] Publicar camada analitica em `BigQuery`
- [ ] Criar dashboard em `Looker Studio`
- [ ] Configurar monitoramento em `Cloud Monitoring`

## Fase 6 - Hardening

- [x] Criar pipeline GitHub Actions para testes automatizados
- [x] Expandir testes para o fluxo PySpark completo
- [ ] Automatizar validacao visual do dashboard
- [ ] Revisar custo e simplificar operacao
