# Databricks notebooks

Organizacao sugerida:

- `bronze/01_ingest_open_brewery_db.py`: ingestao paginada da API e persistencia raw
- `silver/02_build_silver.py`: limpeza, tipagem, deduplicacao e particionamento
- `gold/03_build_gold.py`: agregacoes finais para a camada analitica
- `gold/04_quality_and_ops.py`: validacoes, quality logs e execution logs

Boas praticas esperadas:

- notebooks pequenos e orientados a camada
- regras de qualidade desacopladas da logica principal
- parametros para ambiente e caminho de storage
