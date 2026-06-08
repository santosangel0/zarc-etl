# Contratos de dados (`data/final/`)

Esses parquets são a interface estável entre `zarc-etl` e qualquer consumidor
(hoje: `zarc-app`). Mudanças de schema requerem versionamento explícito.

Todos os arquivos: **ZSTD level 3**, **`row_group_size=100000`**.

## `estacoes.parquet`

| Coluna | Tipo | Notas |
|---|---|---|
| `cd_estacao` | `Utf8` | Ex: `A001` |
| `dc_nome` | `Utf8` | Nome da estação |
| `sg_estado` | `Utf8` | UF (sigla) |
| `vl_latitude` | `Float64` | EPSG:4326 |
| `vl_longitude` | `Float64` | EPSG:4326 |
| `geometry` | `Binary` (WKB) | Point EPSG:4326 |
| (outras) | `Utf8` | Demais campos do JSON da API INMET, normalizados snake_case |

**Ordem física**: nenhuma específica.

## `inmet_historico.parquet`

Schema **raw hourly** (port fiel da `pipeline_inmet_parquet.qmd`). Todas as
colunas são `Utf8` — DuckDB / Polars castam na hora da query. Aproximadamente:

| Coluna | Tipo |
|---|---|
| `cd_estacao` | `Utf8` |
| `data` | `Utf8` (`YYYY-MM-DD`) |
| `hora_utc` | `Utf8` (`HHMM UTC`) |
| `precipitacao_total_horario_mm` | `Utf8` |
| `pressao_atmosferica_ao_nivel_da_estacao_horaria_mb` | `Utf8` |
| `temperatura_do_ar_bulbo_seco_horaria_c` | `Utf8` |
| `umidade_relativa_do_ar_horaria` | `Utf8` |
| ... (≈30 colunas, dependendo do ano) | `Utf8` |

**Ordem física**: `(cd_estacao, data)` — predicate pushdown otimizado para
queries por estação/período.

**Tamanho típico**: ~2-3 GB para 2000-2025.

## `inmet_historico_diario.parquet`

Agregação diária com QC + ITU. Schema espelha `fetch_climate_data()` do
`zarc-app`.

| Coluna | Tipo | Notas |
|---|---|---|
| `cd_estacao` | `Utf8` | |
| `data` | `Date` | Já casted |
| `temp_med` | `Float64` | mean de temperatura horária, QC [-10, 50] |
| `temp_max` | `Float64` | max de temperatura horária |
| `umid_med` | `Float64` | mean de UR horária, QC [0, 100] |
| `umid_min` | `Float64` | min de UR horária |
| `itu_med` | `Float64` | Buffington 1977 |
| `itu_max` | `Float64` | Buffington 1977 com `(temp_max, umid_min)` |

**Ordem física**: `(cd_estacao, data)`.

## `ibge_localidades_{regioes,estados,mesorregioes,microrregioes,municipios}.parquet`

| Coluna | Tipo | Notas |
|---|---|---|
| `id` | `Int64` | Código IBGE |
| `nome` | `Utf8` | |
| `parent_id` | `Int64` (nullable) | id da entidade pai (regiões não têm) |

## `ibge_malhas_{nivel}.parquet`

| Coluna | Tipo | Notas |
|---|---|---|
| `code` | `Utf8` | Código IBGE da geometria (`codarea` no GeoJSON) |
| `nivel` | `Utf8` | `regioes` / `estados` / `mesorregioes` / `municipios` |
| `geometry` | `Binary` (WKB) | EPSG:4326, polygon ou multipolygon |

## `milk_production.parquet`

| Coluna | Tipo | Notas |
|---|---|---|
| `code` | `Int64` | IBGE da localidade |
| `nome` | `Utf8` | |
| `year` | `Int64` | |
| `milk_production_liters` | `Float64` | **litros** (mil litros × 1000) |
| `geo_level` | `Utf8` | `N1`/`N2`/`N3`/`N6`/`N8`/`N9` |

**Ordem física**: `(geo_level, year, code)`.

## `nasa_power_{lat}_{lon}_{start}_{end}.parquet`

| Coluna | Tipo |
|---|---|
| `date` | `Date` |
| `lat` | `Float64` |
| `lon` | `Float64` |
| `t2m` | `Float64` (nullable) |
| `rh2m` | `Float64` (nullable) |
| `prectotcorr` | `Float64` (nullable) |

**Ordem física**: `date` ascendente.
