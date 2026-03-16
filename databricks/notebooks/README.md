# Databricks notebooks

Organizacao sugerida:

- `bronze/`: validacao minima do raw
- `silver/`: limpeza, tipagem, deduplicacao e particionamento
- `gold/`: agregacoes finais e tabelas de consumo

Boas praticas esperadas:

- notebooks pequenos e orientados a camada
- regras de qualidade desacopladas da logica principal
- parametros para ambiente e caminho de storage
