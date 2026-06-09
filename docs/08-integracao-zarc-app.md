# Integração com `zarc-app`

> **Status**: stub. A integração no `zarc-app` (R/Shiny, Rhino) **não** faz
> parte da entrega atual. Este documento será preenchido na próxima entrega
> quando o app for adaptado para ler dos parquets locais em vez de chamar as
> APIs ao vivo.

## Contrato disponível hoje

`zarc-etl/data/final/` contém parquets prontos. O `zarc-app` pode lê-los com:

```r
library(arrow)
library(sf)
library(duckdb)

# Estações (com geometria WKB)
estacoes <- arrow::read_parquet("path/to/data/final/estacoes.parquet")
estacoes_sf <- sf::st_as_sf(estacoes, sf::st_as_sfc(estacoes$geometry, crs = 4326))

# Histórico diário (substitui fetch_climate_data())
con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
diario <- DBI::dbGetQuery(con, "
  SELECT cd_estacao, data, temp_med, temp_max, umid_med, umid_min, itu_med, itu_max
  FROM read_parquet('path/to/data/final/inmet_historico_diario.parquet')
  WHERE cd_estacao = 'A001' AND data BETWEEN '2024-01-01' AND '2024-12-31'
")
DBI::dbDisconnect(con, shutdown = TRUE)

# Malhas (geometrias)
malhas <- arrow::read_parquet("path/to/data/final/ibge_malhas_estados.parquet")
malhas_sf <- sf::st_as_sf(malhas, sf::st_as_sfc(malhas$geometry, crs = 4326))

# Produção de leite
leite <- arrow::read_parquet("path/to/data/final/milk_production.parquet")
```

## Próximos passos (entrega futura)

1. Substituir `app/logic/inmet.R::fetch_climate_data` por leitura do
   `inmet_historico_diario.parquet` via DuckDB (predicate pushdown).
2. Substituir `app/logic/ibge.R::fetch_geojson` por leitura dos
   `ibge_malhas_*.parquet`.
3. Substituir `fetch_milk_production` por leitura do `milk_production.parquet`.
4. Manter as funções de view que não pertencem ao ETL: `filter_stations`,
   `buffer_polygon`, `state.R`, `stats.R`.
5. Adicionar variável `zarc_etl_data_dir` no `config.yml` para localizar os
   parquets.
6. Atualizar `Dockerfile` do `zarc-app` para montar o volume compartilhado
   com `zarc-etl/data/final/` (ou copiar parquets na build).
