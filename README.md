# zarc-etl

Pipeline batch (Python + Polars) que coleta dados de **INMET**, **IBGE** e
**NASA POWER** e materializa-os em parquets locais consumidos pelo
[`zarc-app`](https://github.com/santosangel0/zarc-app) (R/Shiny).

## Setup

Pré-requisito: Docker + Compose v2.

```bash
cp .env.example .env
# Editar .env: setar INMET_TOKEN se for usar `inmet-live`
docker compose build
```

## Uso

```bash
# Estações INMET (rápido)
docker compose run --rm etl inmet-stations

# Histórico INMET completo (vários GB, demora)
docker compose run --rm etl inmet-history --years 2000-2025
docker compose run --rm etl inmet-history-daily   # agregação diária + QC + ITU

# IBGE
docker compose run --rm etl ibge-localidades
docker compose run --rm etl ibge-malhas --level estados
docker compose run --rm etl ibge-milk --geo-level N3 --codes 31 --years 2000-2023

# NASA POWER (sem token)
docker compose run --rm etl nasa-power --lat -21.7 --lon -43.4 \
  --start 2024-01-01 --end 2024-12-31

# Tudo
docker compose run --rm etl all
```

Saída em `data/final/*.parquet` (ZSTD, sort físico para predicate pushdown).

## Documentação

Documentação completa na wiki em
`/home/wizard/google_drive/docs/Embrapa/wiki/zarc-etl/`. Cópia versionada em
[`docs/wiki/`](docs/wiki/index.md):

- [01 Arquitetura](docs/wiki/01-arquitetura.md)
- [02 Docker](docs/wiki/02-docker.md)
- [03 Pipeline INMET](docs/wiki/03-pipeline-inmet.md)
- [04 Pipeline IBGE](docs/wiki/04-pipeline-ibge.md)
- [05 Pipeline NASA POWER](docs/wiki/05-pipeline-nasa-power.md)
- [06 Contratos de dados](docs/wiki/06-contratos-de-dados.md)
- [07 Runbook](docs/wiki/07-runbook.md)
- [08 Integração com zarc-app](docs/wiki/08-integracao-zarc-app.md)

## Dev local (sem Docker)

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
ruff check src tests main.py
python main.py inmet-stations
```
