# Escolha dos servicos e ambientes

## Caminho principal da V1

| Componente | Papel no projeto | Motivo |
| --- | --- | --- |
| Google Colab | Ambiente principal de execucao e validacao | Baixo atrito para demonstrar o case |
| PySpark | Transformacoes, validacoes e agregacoes | Aderencia tecnica ao desafio |
| Luigi | Orquestracao das etapas bronze-silver-gold-ops | Atende scheduling, retries e error handling de forma leve |
| Arquivos locais / artefatos parquet-json | Persistencia de `bronze`, `silver`, `gold` e `ops` | Simples, reprodutivel e suficiente para o MVP |
| Streamlit | Dashboard executivo e operacional | Enriquecimento visual do projeto |
| `ops/quality_results` e `ops/execution_events` | Base do monitoramento do pipeline | Permitem acompanhar qualidade, falha e volume por execucao |
| GitHub Actions | CI/CD | Integra bem com o repositorio |

## Evolucao recomendada para GCP

| Servico | Papel no projeto | Quando usar |
| --- | --- | --- |
| Google Cloud Storage | Data lake bronze/silver/gold | Quando quiser persistencia em cloud |
| Dataproc Serverless | Execucao de PySpark em escala | Quando o pipeline sair do modo Colab |
| BigQuery | Serving e consulta analitica | Quando quiser consumo SQL/BI |
| Looker Studio | Dashboard executivo em cloud | Quando quiser uma camada BI nativa do Google |
| Cloud Monitoring | Observabilidade e alertas | Quando a execucao estiver em GCP |
| Secret Manager | Gestao de segredos | Quando existir credencial de API ou servico |

## Monitoramento e alertas no desenho atual

Mesmo no MVP local/Colab, o pipeline ja produz sinais suficientes para observabilidade:

- falha/sucesso da execucao em `ops/execution_events`
- checks de qualidade em `ops/quality_results`
- retries no `Luigi`
- quality gate critico com falha automatica opcional

O desenho completo de alertas e a trilha para `Cloud Monitoring` estao detalhados em [monitoring-alerting.md](./monitoring-alerting.md).

## O que nao e obrigatorio para o case

| Item | Motivo |
| --- | --- |
| Airflow | Mais pesado que o necessario para a V1, dado que `Luigi` ja atende a orquestracao |
| Docker | Opcional no enunciado |
| BigQuery na V1 | O MVP ja fica forte com Colab + PySpark + dashboard |

## Trilha atual do repositorio

| Status | Item |
| --- | --- |
| Implementado | Colab + PySpark |
| Implementado | Orquestracao com Luigi |
| Implementado | Dashboard em Streamlit |
| Implementado | Evidencia de qualidade em cenarios feliz e falho |
| Implementado | Desenho de monitoramento e alertas |
| Documentado | Evolucao para GCP |
