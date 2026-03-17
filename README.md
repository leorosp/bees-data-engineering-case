# Case de Engenharia de Dados BEES

Implementacao do case da Open Brewery DB com caminho principal em `PySpark`, validado localmente e em `Google Colab`, usando arquitetura medallion e um dashboard em `Streamlit` para a camada de demonstracao.

## O que este repositorio entrega

- ingestao para `bronze`
- transformacao tipada e deduplicacao em `silver`
- agregacao analitica em `gold`
- checks de qualidade e logs operacionais em `ops`
- orquestracao com `Luigi`, incluindo retries e tratamento de erro
- dashboard executivo e operacional
- evidencia de execucao valida e exercicio do gate de qualidade
- CI com testes e validacao rapida do fluxo principal

## Arquitetura em Alto Nivel

- `bronze`: preserva o payload bruto da API
- `silver`: normaliza colunas, aplica tipos e deduplica `brewery_id`
- `silver`: persiste em `parquet` particionado por `country` e `state_province`
- `gold`: agrega quantidade de breweries por `brewery_type`, `country` e `state_province`
- `ops`: persiste resultados de qualidade e eventos de execucao

## Como Avaliar em 3 Minutos

Rode os comandos abaixo a partir da raiz do repositorio:

```bash
pip install -e ".[dev,local,dashboard]"
python scripts/run_local_pyspark_demo.py
python -m streamlit run dashboard/app.py
```

Depois:

- abra `http://localhost:8501`
- use `Dataset demo` para a apresentacao mais rapida
- use `Saida local` quando `local_output/` tiver sido gerado

Documentacao complementar:

- [Guia rapido do avaliador](./docs/evaluator-quickstart.md)
- [Guia rapido local](./docs/local-quickstart.md)
- [Monitoramento e alertas](./docs/monitoring-alerting.md)
- [Dashboard](./dashboard/README.md)

## Orquestracao do Pipeline

O projeto agora inclui uma trilha explicita com `Luigi`, cobrindo:

- dependencias entre `bronze`, `silver`, `gold` e `ops`
- scheduling local com `--local-scheduler` e trilha natural para `luigid`
- retries configuraveis por etapa
- falha opcional quando um check critico de qualidade quebra

Execucao de exemplo:

```bash
python -m luigi --module orchestration.luigi_pipeline PipelineOrchestration \
  --local-scheduler \
  --source-file examples/sample_breweries.json \
  --output-dir luigi_output \
  --landing-date 2026-03-16 \
  --run-id luigi-run-001
```

## Evidencias de Validacao

### Execucao de Referencia

Execucao de referencia do fluxo principal:

As chaves e caminhos abaixo refletem a saida real do script e, por isso, permanecem tecnicos:

```json
{
  "bronze_output_path": "local_output/bronze/landing_date=2026-03-16",
  "silver_output_path": "local_output/silver/breweries",
  "gold_output_path": "local_output/gold/breweries_by_type_location",
  "quality_results_path": "local_output/ops/quality_results",
  "execution_events_path": "local_output/ops/execution_events",
  "source_record_count": 4,
  "silver_record_count": 4,
  "gold_record_count": 3,
  "quality_gate_status": "pass"
}
```

Artefatos esperados:

- `local_output/bronze/landing_date=.../`
- `local_output/silver/breweries/`
- `local_output/gold/breweries_by_type_location/`
- `local_output/ops/quality_results/`
- `local_output/ops/execution_events/`

### Exercicio do Gate de Qualidade

O repositorio inclui um dataset ruim em [examples/sample_breweries_bad.json](./examples/sample_breweries_bad.json).

Esse dataset existe para demonstrar o comportamento do pipeline quando regras criticas de qualidade sao violadas:

```bash
python scripts/run_local_pyspark_demo.py \
  --source-file examples/sample_breweries_bad.json \
  --output-dir local_output_bad \
  --run-id bad-case-001 \
  --landing-date 2026-03-16 \
  --fail-on-critical-quality
```

Resultado esperado:

- o comando termina com erro por design
- `required_fields = fail`
- `duplicate_primary_keys = fail`
- os artefatos em `local_output_bad/` continuam disponiveis para inspecao

## Previa Visual

O dashboard em `Streamlit` consolida:

- distribuicao de breweries por tipo
- concentracao geografica por estado
- status da ultima execucao
- resultado dos checks de qualidade

## O Que o Avaliador Deve Verificar

- `bronze` preserva o payload bruto com metadados de ingestao
- `silver` deduplica `brewery_id` e entrega schema estavel
- `gold` responde a pergunta do case com agregacao por tipo e localizacao
- `ops` mostra checks de qualidade e status da execucao
- o dashboard resume negocio e saude tecnica no mesmo lugar

## Monitoramento e Alertas

O requisito de observabilidade do case esta coberto em dois niveis:

- no MVP, o pipeline persiste sinais operacionais em `ops/quality_results` e `ops/execution_events`
- na documentacao, o projeto descreve como transformar esses sinais em alertas para falha de pipeline, falha critica de qualidade, atraso de execucao e queda anormal de volume

Resumo do desenho:

- falha de pipeline apos retries: alerta de alta prioridade
- check critico com `status = fail`: falha do job e alerta imediato
- ausencia de execucao no SLA: alerta de frescor
- queda anormal entre `records_in` e `records_out`: alerta de anomalia operacional

Detalhamento completo:

- [Monitoramento e alertas](./docs/monitoring-alerting.md)

## Testes e CI

O repositorio hoje possui:

- testes unitarios de configuracao e qualidade
- testes de qualidade critica
- testes de integracao para `silver`, `gold` e pipeline local ponta a ponta
- validacao rapida em `GitHub Actions` executando o fluxo principal em `PySpark`

## Estrutura Principal

```text
.
|- docs/
|- dashboard/
|- examples/
|- scripts/
|- src/bees_case/
`- tests/
```

## Leitura Recomendada

- [Arquitetura](./docs/architecture.md)
- [Escolha dos servicos](./docs/services.md)
- [Backlog](./docs/backlog.md)
- [Runbook](./docs/runbook.md)
- [Guia Colab/GCP](./docs/gcp-colab-guide.md)

## Escopo Atual

- `implementado e validado`: `PySpark + Streamlit`
- `ambiente principal`: local ou `Google Colab`
- `documentado como evolucao`: `GCP`

## Referencias

- [Open Brewery DB](https://www.openbrewerydb.org/)
- [Google Colab](https://colab.research.google.com/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Streamlit Documentation](https://docs.streamlit.io/)
