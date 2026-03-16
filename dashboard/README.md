# Streamlit Dashboard

Este dashboard enriquece o projeto com duas visoes:

- executiva: distribuicao de breweries por tipo e por estado
- operacional: checks de qualidade e ultima execucao

Ele faz parte do caminho principal `Colab + PySpark` do projeto.

## Como rodar

1. Entre na pasta do projeto:

```powershell
cd "C:\Users\leona\Documents\GitHub\bees-data-engineering-case"
```

2. Instale as dependencias:

```bash
pip install -e ".[local,dashboard]"
```

3. Gere os artefatos locais:

```bash
python scripts/run_local_pyspark_demo.py
```

4. Rode o app:

```bash
python -m streamlit run dashboard/app.py
```

5. Abra no navegador:

`http://localhost:8501`

## Pastas de entrada esperadas

Por padrao, o app le:

- `local_output/gold/breweries_by_type_location`
- `local_output/ops/quality_results`
- `local_output/ops/execution_events`

Voce tambem pode apontar o dashboard para `local_output_bad` para mostrar um cenario com falhas de qualidade.

## Como apresentar os dois cenarios

- `local_output`: execucao saudavel com checks em `pass`
- `local_output_bad`: execucao com falhas controladas de qualidade

Troque o campo `Artifacts folder` na barra lateral do dashboard para alternar entre os dois.

## Erro mais comum

Se aparecer algo como:

- `pyproject.toml not found`
- `can't open file scripts/run_local_pyspark_demo.py`
- `dashboard/app.py does not exist`

o terminal foi aberto fora da pasta do repositorio. Volte para:

```powershell
cd "C:\Users\leona\Documents\GitHub\bees-data-engineering-case"
```
