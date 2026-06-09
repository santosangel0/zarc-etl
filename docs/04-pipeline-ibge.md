# Pipeline IBGE

Três produtos parquet a partir de três APIs IBGE distintas. Tudo em
`src/{collect,transform}/ibge.py`.

## 1. Localidades (v1)

```bash
docker compose run --rm etl ibge-localidades
```

Endpoints `https://servicodados.ibge.gov.br/api/v1/localidades/...` para
regiões, estados, mesorregiões, microrregiões e municípios. Para cada nível,
gera um parquet `ibge_localidades_{nivel}.parquet` com `id`, `nome` e
`parent_id` (quando aplicável).

| Nível | Endpoint | Parent |
|---|---|---|
| `regioes` | (estático em `REGIONS`) | — |
| `estados` | `/estados?orderBy=nome` | `regiao.id` |
| `mesorregioes` | `/estados/{uf}/mesorregioes` (loop) | `UF.id` |
| `microrregioes` | `/estados/{uf}/microrregioes` (loop) | `mesorregiao.UF.id` |
| `municipios` | `/estados/{uf}/municipios?orderBy=nome` (loop) | `microrregiao.id` |

Mirrors `app/logic/ibge.R:17-145` + `:440-463`.

## 2. Malhas (v3) — geometria como WKB

```bash
docker compose run --rm etl ibge-malhas --level estados
docker compose run --rm etl ibge-malhas --level municipios --code 31  # MG
```

Endpoint `https://servicodados.ibge.gov.br/api/v3/malhas/{level}/{code}` com
`?formato=application/vnd.geo+json`. GeoJSON é convertido para WKB binário
via `shapely`:

```python
from shapely.geometry import shape
from shapely import wkb as shapely_wkb
g = shape(feature["geometry"])
wkb_bytes = shapely_wkb.dumps(g)
```

Saída: `ibge_malhas_{nivel}.parquet` com colunas `code`, `nivel`, `geometry`
(Binary).

**Leitura no R**:

```r
library(arrow)
library(sf)
df <- arrow::read_parquet("data/final/ibge_malhas_estados.parquet")
sf_obj <- sf::st_as_sf(df, sf::st_as_sfc(df$geometry, crs = 4326))
```

Mirrors `app/logic/ibge.R:147-282` (`fetch_geojson`, `fetch_subdivisions`).

## 3. SIDRA (Agregados v3) — produção de leite

```bash
docker compose run --rm etl ibge-milk --geo-level N3 --codes 31 --years 2000-2023
```

Endpoint:

```
https://servicodados.ibge.gov.br/api/v3/agregados/74/periodos/{years}/variaveis/106
  ?localidades={geo_level}[{codes}]&classificacao=80[2682]
```

Tabela 74 (PPM), variável 106 (quantidade), classificação 80/2682 (Leite).
Saída em **mil litros** — `transform/ibge.py::parse_milk_production`
multiplica por 1000 para retornar em **litros**, igual a
`app/logic/ibge.R:373-376`.

### Limpeza de valores especiais (`clean_sidra_value`)

| Valor IBGE | Significado | Conversão |
|---|---|---|
| `"-"` | Zero absoluto | `0.0` |
| `".."` / `"..."` | Não disponível | `null` |
| `"X"` | Resultado retido | `null` |
| `""` | Vazio | `null` |
| `"123"` | Numérico | `123.0` |

Idêntico a `app/logic/ibge.R:421-433`.

### Limite de 100k valores (`validate_api_request`)

A API rejeita queries onde `categorias × períodos × localidades > 100.000`.
Validador retorna `True` se OK, ou string com mensagem de erro
(idêntico a `app/logic/ibge.R:397-419`). O CLI `ibge-milk` chama o validador
antes de fazer HTTP e aborta com exit code 2 se exceder.

**Saída**: `milk_production.parquet` com `code, nome, year,
milk_production_liters, geo_level`, ordenado por `(geo_level, year, code)`.

## Geo-levels suportados

| Código | Descrição |
|---|---|
| `N1` | Brasil |
| `N2` | Grande região |
| `N3` | Estado |
| `N6` | Município |
| `N8` | Mesorregião |
| `N9` | Microrregião |
