# Monitoramento e Alertas

Este documento fecha o requisito do case que pede a descricao de como monitorar e alertar o pipeline.

## O que ja existe no MVP

O projeto ja gera dois artefatos operacionais que funcionam como base da observabilidade:

- `ops/quality_results`: resultado de cada check de qualidade, com camada, status, metrica e mensagem.
- `ops/execution_events`: status da execucao, volume de entrada, volume de saida, detalhes e timestamp.

Esses artefatos sao produzidos pelo pipeline em `PySpark` e tambem aparecem no dashboard em `Streamlit`.

## Eventos que devem ser monitorados

### 1. Falha de pipeline

Sinal:
- etapa do `Luigi` falha mesmo apos retries
- `status = failed` em `ops/execution_events`

Acao:
- abrir alerta de alta prioridade
- enviar notificacao para email ou canal de time
- bloquear a promocao do dado para consumo

### 2. Falha critica de qualidade

Sinal:
- qualquer check critico em `ops/quality_results` com `status = fail`
- pipeline executado com `--fail-on-critical-quality`

Acao:
- falhar o job imediatamente
- disparar alerta de alta prioridade
- anexar ao alerta o `run_id`, o check que falhou e a metrica observada

### 3. Queda anormal de volume

Sinal:
- diferenca relevante entre `records_in` e `records_out`
- queda inesperada entre `bronze`, `silver` e `gold`

Acao:
- alerta de media prioridade
- revisar se a queda veio de deduplicacao legitima, filtro esperado ou problema na origem

Exemplo de regra:
- alertar quando a reducao de volume ultrapassar `30%` fora de um dataset conhecido como teste

### 4. Ausencia de execucao ou atraso

Sinal:
- nenhuma execucao no horario esperado
- ultima execucao com timestamp acima do SLA definido

Acao:
- alerta de media prioridade
- checar scheduler, credenciais e disponibilidade da API

### 5. Crescimento de nulos ou duplicidades

Sinal:
- aumento de `missing_required_fields`
- aumento de `duplicate_primary_keys`

Acao:
- alerta de media prioridade
- manter tendencia historica por `run_id` para diferenciar ruido de regressao

## Severidade sugerida

| Cenario | Severidade | Acao |
| --- | --- | --- |
| Pipeline falhou apos retries | Alta | Notificacao imediata e investigacao |
| Check critico falhou | Alta | Falhar pipeline e alertar |
| Queda anormal de volume | Media | Investigar e comparar com execucoes anteriores |
| Execucao atrasada ou ausente | Media | Verificar scheduler e dependencia externa |
| Variacao moderada de nulos ou duplicidade | Baixa/Media | Acompanhar tendencia e abrir incidente se persistir |

## Como eu implementaria em producao

### Caminho GCP

1. Persistir `ops/quality_results` e `ops/execution_events` em `Google Cloud Storage` ou `BigQuery`.
2. Enviar logs estruturados do `Luigi` e do `PySpark` para `Cloud Logging`.
3. Criar metricas baseadas em logs e tabelas operacionais.
4. Configurar alertas no `Cloud Monitoring`.
5. Publicar notificacoes em email, webhook, Slack ou PagerDuty.

### Alertas concretos no GCP

- `pipeline_failure_alert`
  - condicao: job terminou com erro ou sem evento de sucesso dentro da janela esperada
- `critical_quality_alert`
  - condicao: existe linha com `status = fail` para check critico
- `volume_drop_alert`
  - condicao: `records_out / records_in < 0.70`
- `freshness_alert`
  - condicao: ultimo `event_timestamp_utc` acima do SLA

## O que o avaliador pode considerar implementado hoje

- qualidade medida e persistida na camada `ops`
- execucao registrada com eventos operacionais
- quality gate critico com falha automatica opcional
- retries no orquestrador `Luigi`
- dashboard exibindo estado da ultima execucao e checks de qualidade

## O que fica como ultima milha de producao

- roteamento real para canais de notificacao
- integracao com `Cloud Monitoring`
- historico de execucoes para alertas por anomalia
- politicas formais de SLA e severidade
