# Escolha exata dos servicos Azure

## Servicos aprovados para a V1

| Servico | Papel no projeto | Motivo |
| --- | --- | --- |
| Azure Data Factory | Ingestao da API, agendamento e acionamento dos jobs | REST connector nativo, paginacao e boa aderencia ao caso |
| Azure Data Lake Storage Gen2 | Data lake bronze/silver/gold | Storage certo para medallion no Azure |
| Azure Databricks | Transformacoes, validacoes e agregacoes | Melhor fit para PySpark + Delta |
| Azure Key Vault | Segredos e chaves | Remove credenciais do codigo |
| Azure Monitor | Telemetria e regras de alerta | Centraliza monitoramento |
| Log Analytics Workspace | Persistencia e consulta de logs | Facilita diagnostico operacional |
| Action Groups | Notificacoes por e-mail/webhook | Fecha o requisito de alerting |
| Power BI | Camada de consumo | Melhor fit executivo no ecossistema Microsoft |
| GitHub Actions | CI/CD | Integra bem com o GitHub do projeto |

## Servicos descartados para a V1

| Servico | Motivo para nao usar agora |
| --- | --- |
| Apache Airflow | ADF ja cobre bem a orquestracao para este caso |
| Azure Functions | So entra se a API exigir logica de ingestao mais customizada do que o ADF suporta |
| Azure Synapse | Aumenta o escopo sem ganho claro para este case |
| Metabase | Power BI encaixa melhor no ecossistema escolhido |
| Streamlit em producao | Pode entrar depois como demo tecnica, mas nao e prioridade na V1 |

## Mapeamento das referencias para Azure

| Ideia de referencia | Equivalente escolhido |
| --- | --- |
| S3 / MinIO | ADLS Gen2 |
| Glue / Spark standalone | Azure Databricks |
| Lambda para ingestao | Azure Data Factory REST pipeline |
| CloudWatch | Azure Monitor + Log Analytics |
| SES / e-mail de alertas | Action Groups |
| Metabase / Streamlit | Power BI |

## Documentacao oficial usada

- [Azure Data Factory REST connector](https://learn.microsoft.com/en-us/azure/data-factory/connector-rest)
- [Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/upgrade-to-data-lake-storage-gen2)
- [Azure Databricks schema evolution](https://learn.microsoft.com/en-us/azure/databricks/data-engineering/schema-evolution)
- [Azure Monitor alerts overview](https://learn.microsoft.com/en-us/azure/azure-monitor/alerts/alerts-overview)
- [Azure Monitor action groups](https://learn.microsoft.com/en-us/azure/azure-monitor/alerts/action-groups)
