# Runbook

## Setup inicial (host novo)

```bash
git clone <repo-url> zarc-etl
cd zarc-etl
cp .env.example .env
# Editar .env: setar INMET_TOKEN se for usar inmet-live
docker compose build
```

## Atualização anual completa (rodada de Janeiro)

```bash
docker compose run --rm etl all --history-years 2000-$(date +%Y) \
  --milk-years 2000-$(($(date +%Y) - 1))
```

Tempo esperado em conexão decente: 30-60 min para histórico completo,
1-2 min para o resto.

## Atualizar só o último ano do INMET (mais comum)

Se já existe `inmet_historico.parquet` antigo e você quer apenas adicionar
2025:

```bash
# 1. Baixa o ZIP novo + acumula em DuckDB + reexporta
docker compose run --rm etl inmet-history --years 2025-2025

# 2. Recomputa o derivado diário
docker compose run --rm etl inmet-history-daily
```

> **Atenção**: `inmet-history` re-exporta TUDO que está no DuckDB. Se você
> rodou `--years 2025-2025` em cima de um DuckDB recém-truncado, o parquet
> final terá só 2025. Para incremental real, rodar com a faixa completa
> `--years 2000-2025`.

## Adicionar novo estado/município ao IBGE milk

```bash
# Ex: incluir Goiás (52)
docker compose run --rm etl ibge-milk --geo-level N3 --codes 31,52 --years 2000-2024
```

`milk_production.parquet` é regravado com o novo escopo.

## Invalidar caches

| O que invalidar | Comando |
|---|---|
| ZIPs INMET (forçar re-download) | `rm -rf data/raw/inmet/` |
| CSVs descompactados | `rm -rf data/interim/inmet/` |
| DuckDB temporário (caso fique órfão) | `rm -f data/interim/inmet_temp.duckdb*` |
| Cache NASA POWER | `rm -rf data/raw/nasa_power/` |
| Tudo (recomeçar do zero) | `rm -rf data/` |

## Debug / observabilidade

```bash
# Ver schema e contagens de um parquet
docker compose run --rm --entrypoint python etl -c \
  "import polars as pl; df = pl.read_parquet('/data/final/inmet_historico_diario.parquet'); print(df.schema); print(df.height)"

# Top estações por volume de dados
docker compose run --rm --entrypoint duckdb etl \
  "SELECT cd_estacao, COUNT(*) FROM read_parquet('/data/final/inmet_historico.parquet') GROUP BY 1 ORDER BY 2 DESC LIMIT 10"

# Rodar com log DEBUG
LOG_LEVEL=DEBUG docker compose run --rm etl inmet-stations
```

## Quando a API do INMET cai

O retry do `requests` (3 tentativas, backoff exponencial em
`429,500,502,503,504`) cobre quedas curtas. Para indisponibilidade
prolongada:

- `inmet-stations` falha com erro HTTP — re-rodar mais tarde.
- `inmet-history` continua tentando ZIP por ZIP. ZIPs já baixados são pulados;
  re-rodada retoma do ponto onde parou.
- `inmet-live` retorna lista vazia em HTTP error (igual ao R), não falha.

## Promoção para o `zarc-app`

Hoje os parquets ficam em `zarc-etl/data/final/`. Para o Shiny lê-los:

```r
# Em zarc-app (futuro):
library(arrow)
df <- arrow::read_parquet("/path/to/zarc-etl/data/final/inmet_historico_diario.parquet")
```

Detalhes em [08-integracao-zarc-app.md](08-integracao-zarc-app.md) (stub, a
ser preenchido na próxima entrega).
