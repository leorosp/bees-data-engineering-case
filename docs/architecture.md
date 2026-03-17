# Arquitetura

## Visao geral

O desenho abaixo prioriza simplicidade, aderencia ao case e execucao pratica em `Google Colab + PySpark`, com uma trilha natural de evolucao para `GCP`.

```mermaid
flowchart LR
    A["Open Brewery DB API"] --> B["Orquestracao com Luigi"]
    Z["Dataset de exemplo para testes controlados"] --> B
    B --> C["PySpark bronze<br/>json bruto por data de ingestao"]
    C --> D["PySpark silver<br/>parquet particionado por localizacao"]
    D --> E["PySpark gold<br/>agregacoes por tipo e localizacao"]
    C --> O["Camada ops<br/>qualidade e eventos de execucao"]
    D --> O
    E --> O
    E --> F["Streamlit Dashboard"]
    O --> F
    O --> M["Desenho de monitoramento e alertas"]

    E --> G["Camada opcional de serving em GCP"]
    G --> H["Google Cloud Storage / BigQuery / Looker Studio"]
    M --> I["Cloud Logging / Cloud Monitoring"]
```

## Fluxo por camada

### Bronze

- `PySpark` consome a Open Brewery DB API ou dados de exemplo controlados.
- Os payloads sao gravados em `json`.
- Particionamento inicial por `ingestion_date=YYYY-MM-DD`.
- Objetivo: preservar o dado bruto para replay e auditoria.

### Silver

- `PySpark` le os arquivos bronze.
- Normaliza schema, remove duplicidades, padroniza tipos e trata nulos.
- Grava em `parquet` no caminho local/Colab.
- Particionamento implementado por `country` e `state_province`.

### Gold

- `PySpark` gera tabelas agregadas.
- Foco principal do case:
  - quantidade de breweries por `brewery_type`
  - quantidade de breweries por localizacao
  - quantidade de breweries por `brewery_type + country + state_province`

### Ops

- Centraliza sinais operacionais do pipeline.
- Persiste checks de qualidade em `ops/quality_results`.
- Persiste eventos de execucao em `ops/execution_events`.
- Alimenta o dashboard e a estrategia de monitoramento e alertas.

## Decisoes principais

- `Google Colab` foi escolhido como caminho principal porque permite validar o case rapidamente sem overhead de infraestrutura.
- `PySpark` foi mantido como tecnologia central para aderir ao perfil do desafio.
- `Luigi` foi adotado como orquestrador leve para explicitar dependencias, retries e error handling.
- `Streamlit` entra como camada de consumo e demonstracao do valor do pipeline.
- `GCP` foi definido como trilha natural de cloud por combinar bem com `Colab`.

## Principios adotados no desenho

- modularizacao para separar ingestao, transformacao, qualidade, observabilidade e consumo
- reproducibilidade com dataset de exemplo, quickstart curto e smoke test
- foco em demonstrabilidade, com dashboard e cenarios feliz e falho
- caminho de evolucao claro para cloud sem bloquear o MVP local

## Regras de implementacao

- O repositorio deve ser original: inspiracao sim, copia literal nao.
- O MVP precisa funcionar em `Google Colab` sem depender de uma cloud especifica.
- Qualquer cloud futura deve ser uma evolucao da solucao, nao um bloqueio para o uso do projeto.
- Cada camada deve ser reprocessavel de forma independente.
