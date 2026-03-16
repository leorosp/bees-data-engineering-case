# Streamlit Dashboard

Este dashboard enriquece o projeto com duas visoes:

- executiva: distribuicao de breweries por tipo e por estado
- operacional: checks de qualidade e ultima execucao
- acoes rapidas: atualizar o painel e tentar gerar os artefatos locais

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

Se o projeto ainda nao tiver `local_output/`, o app abre automaticamente com um dataset demo embutido no repositorio.

## Como usar a interface

- `Demo do projeto`: modo padrao e mais simples para apresentacao
- `Artefatos locais`: aparece quando o pipeline local ja gerou `local_output`
- `Atualizar painel`: recarrega os dados exibidos
- `Gerar local_output`: tenta executar `scripts/run_local_pyspark_demo.py` a partir do proprio app

O layout atual concentra os controles e filtros na sidebar recolhida, deixando a area principal mais limpa e direta.

## Pastas de entrada esperadas

Por padrao, o app le:

- `local_output/gold/breweries_by_type_location`
- `local_output/ops/quality_results`
- `local_output/ops/execution_events`

Voce tambem pode apontar o dashboard para `local_output_bad` para mostrar um cenario com falhas de qualidade.

## Como apresentar os dois cenarios

- `local_output`: execucao saudavel com checks em `pass`
- `local_output_bad`: execucao com falhas controladas de qualidade

Troque a fonte para `Artefatos locais` e selecione o conjunto desejado na sidebar.

## Erro mais comum

Se aparecer algo como:

- `pyproject.toml not found`
- `can't open file scripts/run_local_pyspark_demo.py`
- `dashboard/app.py does not exist`

o terminal foi aberto fora da pasta do repositorio. Volte para:

```powershell
cd "C:\Users\leona\Documents\GitHub\bees-data-engineering-case"
```

## Observacao sobre PySpark local

Se o botao `Gerar dados locais` falhar com mensagem sobre `Java` ou `JAVA_HOME`, isso significa que o `PySpark` local ainda nao esta pronto nesta maquina.

Nessa situacao, o dashboard continua funcionando normalmente no `Demo do projeto`.
