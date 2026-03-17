# Dashboard em Streamlit

O dashboard e a camada de apresentacao do projeto. Ele organiza a experiencia em tres blocos:

- `Visao geral`: KPIs do `gold`, status do pipeline e ultima execucao
- `Analises`: distribuicao por tipo, concentracao geografica e tabela filtravel
- `Detalhes operacionais`: checks de qualidade, resumo da execucao e instrucoes locais

## Como rodar

Execute os comandos abaixo a partir da raiz do repositorio:

```bash
pip install -e ".[dev,local,dashboard]"
python scripts/run_local_pyspark_demo.py
python -m streamlit run dashboard/app.py
```

Abra `http://localhost:8501`.

Se `local_output/` ainda nao existir, o app abre automaticamente com o `Dataset demo`.

## Controles do painel

Os filtros e controles ficam na sidebar recolhida:

- `Fonte de dados`: escolhe entre `Dataset demo` e os artefatos locais disponiveis
- `Atualizar painel`: recarrega os dados
- `Gerar saida local`: tenta gerar `local_output` a partir do proprio app
- `Tipos de cervejaria` e `Estados`: aplicam filtros na visualizacao

## Fontes de dados

Quando os artefatos locais existem, o app le:

- `local_output/gold/breweries_by_type_location`
- `local_output/ops/quality_results`
- `local_output/ops/execution_events`

## Observacao sobre PySpark local

Se `Gerar saida local` falhar, o dashboard continua funcionando normalmente com o dataset demo.
