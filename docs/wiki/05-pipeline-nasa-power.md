# Pipeline NASA POWER

Coleta de dados meteorológicos diários para um ponto a partir do reanálise
NASA POWER. Útil para preencher lacunas das estações INMET ou comparar séries.

```bash
docker compose run --rm etl nasa-power \
  --lat -21.7 --lon -43.4 \
  --start 2024-01-01 --end 2024-12-31
```

## Endpoint

```
GET https://power.larc.nasa.gov/api/temporal/daily/point
  ?parameters=T2M,RH2M,PRECTOTCORR
  &community=AG
  &format=JSON
  &latitude=<lat>
  &longitude=<lon>
  &start=YYYYMMDD
  &end=YYYYMMDD
```

Sem token / sem rate limit explícito. `community=AG` (agriculture) calibra os
parâmetros para uso agronômico.

## Parâmetros coletados

| Código NASA | Descrição | Unidade |
|---|---|---|
| `T2M` | Temperatura média do ar a 2m | °C |
| `RH2M` | Umidade relativa a 2m | % |
| `PRECTOTCORR` | Precipitação total corrigida | mm/dia |

## Cache

Resposta JSON é cacheada em
`data/raw/nasa_power/{lat}_{lon}_{start}_{end}.json`. Re-execuções com os
mesmos parâmetros não fazem HTTP.

## Sentinel `-999`

Valores ausentes na fonte vêm como `-999.0`. A função `parse_point` em
`src/transform/nasa_power.py` converte qualquer valor ≤ -998.5 em `null`.

## Saída

Parquet `nasa_power_{lat}_{lon}_{start}_{end}.parquet` com colunas:

| Coluna | Tipo | Descrição |
|---|---|---|
| `date` | `Date` | Data |
| `lat` | `Float64` | Latitude do ponto (do payload) |
| `lon` | `Float64` | Longitude do ponto (do payload) |
| `t2m` | `Float64` | Temperatura média (°C), null se ausente |
| `rh2m` | `Float64` | Umidade relativa (%), null se ausente |
| `prectotcorr` | `Float64` | Precipitação (mm/dia), null se ausente |

## Exemplo: sítio experimental Embrapa Gado de Leite (Juiz de Fora, MG)

```bash
docker compose run --rm etl nasa-power \
  --lat -21.7 --lon -43.4 \
  --start 2000-01-01 --end 2024-12-31
```

Gera ~9.000 linhas (25 anos × 365 dias) em `~200 KB` (ZSTD).
