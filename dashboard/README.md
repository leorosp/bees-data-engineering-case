# Streamlit Dashboard

O dashboard e a camada de demonstracao do projeto. Ele consolida:

- resumo executivo do `gold`
- saude tecnica da execucao em `ops`
- acoes operacionais para atualizar o painel ou tentar gerar os artefatos locais

## Como rodar

Execute os comandos abaixo a partir da raiz do repositorio:

```bash
pip install -e ".[dev,local,dashboard]"
python scripts/run_local_pyspark_demo.py
python -m streamlit run dashboard/app.py
```

Abra `http://localhost:8501`.

Se `local_output/` ainda nao existir, o app abre automaticamente em `Demo do projeto`.

## Como usar

- `Demo do projeto`: modo padrao para apresentacao rapida
- `Artefatos locais`: habilita os dados gerados pelo fluxo `PySpark`
- `Atualizar painel`: recarrega os dados
- `Gerar local_output`: tenta rodar o pipeline local a partir do proprio app

Os filtros e controles ficam concentrados na sidebar recolhida.

## Guia operacional no proprio painel

O app possui uma secao `Guia operacional do projeto` com:

- status do ambiente
- modo atual
- resumo de como executar o projeto
- proximos passos recomendados

## Fontes de dados esperadas

Por padrao, o app le:

- `local_output/gold/breweries_by_type_location`
- `local_output/ops/quality_results`
- `local_output/ops/execution_events`

Para demonstrar a falha controlada, gere primeiro `local_output_bad/` e depois selecione esse conjunto na sidebar.

## Erro mais comum

Se o app nao achar `dashboard/app.py`, `pyproject.toml` ou `scripts/run_local_pyspark_demo.py`, o comando foi executado fora da raiz do repositorio.

## Observacao sobre PySpark local

Se `Gerar local_output` falhar com mensagem sobre `Java` ou `JAVA_HOME`, o dashboard continua funcionando normalmente no modo demo.
