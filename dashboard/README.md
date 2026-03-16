# Streamlit Dashboard

Este dashboard enriquece o projeto com duas visoes:

- executiva: distribuicao de breweries por tipo e por estado
- operacional: checks de qualidade e ultima execucao

## Como rodar

1. Gere os artefatos locais:

```bash
python scripts/run_local_pyspark_demo.py
```

2. Instale as dependencias do dashboard:

```bash
pip install -e ".[local,dashboard]"
```

3. Rode o app:

```bash
streamlit run dashboard/app.py
```

## Pastas de entrada esperadas

Por padrao, o app le:

- `local_output/gold/breweries_by_type_location`
- `local_output/ops/quality_results`
- `local_output/ops/execution_events`

Voce tambem pode apontar o dashboard para `local_output_bad` para mostrar um cenario com falhas de qualidade.
