# Streamlit Dashboard

O dashboard e a camada de apresentacao do projeto. Ele organiza a experiencia em tres blocos:

- `Overview`: KPIs do `gold`, status do pipeline e ultimo run
- `Analytics`: distribuicao por tipo, concentracao geografica e tabela filtravel
- `Operational Details`: checks de qualidade, resumo da execucao e instrucoes locais

## Como rodar

Execute os comandos abaixo a partir da raiz do repositorio:

```bash
pip install -e ".[dev,local,dashboard]"
python scripts/run_local_pyspark_demo.py
python -m streamlit run dashboard/app.py
```

Abra `http://localhost:8501`.

Se `local_output/` ainda nao existir, o app abre automaticamente com o `Demo Dataset`.

## Controles do painel

Os filtros e controles ficam na sidebar recolhida:

- `Data Source`: escolhe entre `Demo Dataset` e os artefatos locais disponiveis
- `Refresh Dashboard`: recarrega os dados
- `Generate Local Output`: tenta gerar `local_output` a partir do proprio app
- `Brewery Types` e `States`: aplicam filtros na visualizacao

## Fontes de dados

Quando os artefatos locais existem, o app le:

- `local_output/gold/breweries_by_type_location`
- `local_output/ops/quality_results`
- `local_output/ops/execution_events`

## Observacao sobre PySpark local

Se `Generate Local Output` falhar, o dashboard continua funcionando normalmente com o dataset demo.
