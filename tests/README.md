# Tests

Current testing layers:

- `unit/`: configuration, quality helpers, and pure functions
- `data_quality/`: critical quality gate behavior
- `integration/`: `silver`, `gold`, `silver` partitioning, and the end-to-end local pipeline

Current coverage:

- path construction
- required field validation
- duplicate key detection
- critical quality gate enforcement
- `silver` deduplication
- `silver` partitioning by `country` and `state_province`
- `gold` aggregation
- writing `bronze/silver/gold/ops` artifacts
- basic `Luigi` orchestration structure
- controlled end-to-end failure when a critical rule is violated
