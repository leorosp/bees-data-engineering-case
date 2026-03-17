# Tests

Camadas atuais de teste:

- `unit/`: configuracao, helpers de qualidade e funcoes puras
- `data_quality/`: quality gate critico
- `integration/`: `silver`, `gold` e pipeline local ponta a ponta

Cobertura atual:

- construcao de paths
- validacao de campos obrigatorios
- deteccao de chaves duplicadas
- enforcement do quality gate critico
- deduplicacao no `silver`
- agregacao no `gold`
- escrita de artefatos `bronze/silver/gold/ops`
- falha controlada no fluxo completo quando uma regra critica e violada
