# Escolha dos servicos e ambientes

## Caminho principal da V1

| Componente | Papel no projeto | Motivo |
| --- | --- | --- |
| Google Colab | Ambiente principal de execucao e validacao | Baixo atrito para demonstrar o case |
| PySpark | Transformacoes, validacoes e agregacoes | Aderencia tecnica ao desafio |
| Arquivos locais / artefatos parquet-json | Persistencia de `bronze`, `silver`, `gold` e `ops` | Simples, reprodutivel e suficiente para o MVP |
| Streamlit | Dashboard executivo e operacional | Enriquecimento visual do projeto |
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

## O que nao e obrigatorio para o case

| Item | Motivo |
| --- | --- |
| Airflow | Aumenta o escopo sem necessidade |
| Docker | Opcional no enunciado |
| BigQuery na V1 | O MVP ja fica forte com Colab + PySpark + dashboard |

## Trilha atual do repositorio

| Status | Item |
| --- | --- |
| Implementado | Colab + PySpark |
| Implementado | Dashboard em Streamlit |
| Implementado | Evidencia de qualidade em cenarios feliz e falho |
| Documentado | Evolucao para GCP |
