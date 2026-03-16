# Backlog de implementacao

## Fase 0 - Foundation

- [x] Definir nome final do projeto
- [x] Organizar repositorio no GitHub
- [x] Definir arquitetura medallion
- [x] Criar documentacao inicial

**Saida esperada**
- repositorio organizado
- narrativa tecnica clara
- base pronta para evoluir

## Fase 1 - Pipeline Colab/PySpark

- [x] Criar ingestao `bronze`
- [x] Criar transformacao `silver`
- [x] Criar agregacoes `gold`
- [x] Criar camada `ops`

**Saida esperada**
- pipeline funcional ponta a ponta em `PySpark`

## Fase 2 - Data quality

- [x] Definir contratos minimos
- [x] Implementar checks de campos obrigatorios
- [x] Implementar checks de duplicidade
- [x] Persistir resultados em `ops`
- [ ] Falhar execucao automaticamente quando regra critica for violada

**Saida esperada**
- qualidade de dados automatizada

## Fase 3 - Evidencia do case

- [x] Validar cenario feliz
- [x] Validar cenario com falha controlada
- [x] Documentar evidencias no repositorio

**Saida esperada**
- projeto defensavel tecnicamente

## Fase 4 - Consumo

- [x] Criar dashboard executivo e operacional em Streamlit
- [ ] Capturar screenshots e publicar no README
- [ ] Criar versao resumida para apresentacao

**Saida esperada**
- camada de demonstracao pronta

## Fase 5 - Evolucao para GCP

- [ ] Adaptar persistencia para `Google Cloud Storage`
- [ ] Executar o pipeline em `Dataproc Serverless`
- [ ] Publicar camada analitica em `BigQuery`
- [ ] Criar dashboard em `Looker Studio`
- [ ] Configurar monitoramento em `Cloud Monitoring`

**Saida esperada**
- versao cloud-native em GCP

## Fase 6 - Hardening

- [x] Criar pipeline GitHub Actions para testes basicos
- [ ] Expandir testes para o fluxo PySpark completo
- [ ] Automatizar validacao do dashboard
- [ ] Revisar custo e simplificar operacao

**Saida esperada**
- projeto pronto para entrega mais madura
