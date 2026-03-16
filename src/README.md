# Source layout

O codigo Python compartilhado do projeto deve ficar aqui.

- `bees_case/config.py`: parametros do pipeline e paths medallion
- `bees_case/bronze.py`: ingestao HTTP e preparacao de rows raw
- `bees_case/contracts.py`: contratos de colunas e campos obrigatorios
- `bees_case/observability.py`: eventos estruturados de execucao
- `bees_case/quality.py`: checks reutilizaveis de qualidade
