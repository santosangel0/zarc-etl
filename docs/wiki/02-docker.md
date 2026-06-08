# Docker

Toda a execução padrão do `zarc-etl` é via Docker. O host só precisa do Docker
Engine + Compose v2.

## Build

```bash
cd /home/user/zarc-etl
docker compose build
```

A imagem é single-stage `python:3.12-slim` com `libgeos-c1v5` (geometria),
`unzip` (inspeção dos ZIPs INMET) e as dependências Python instaladas via
`pip install -e ".[dev]"`. `~150 MB` instalados.

## Variáveis de ambiente

Copiar `.env.example` para `.env` e ajustar:

| Variável | Default | Uso |
|---|---|---|
| `INMET_TOKEN` | (vazio) | Necessário só para `inmet-live`. Solicitar em portal.inmet.gov.br. |
| `DATA_DIR` | `/data` (container), `./data` (local) | Diretório raiz para `raw/interim/final`. |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`. |

## Run

Padrão: `docker compose run --rm etl <subcomando>`. O volume `./data:/data`
mantém os parquets no host.

```bash
# Estações INMET (rápido, 1-2s)
docker compose run --rm etl inmet-stations

# Histórico INMET completo (vários GB, horas em conexão lenta)
docker compose run --rm etl inmet-history --years 2000-2025

# Derivado diário com QC + ITU
docker compose run --rm etl inmet-history-daily

# IBGE: localidades + malhas + leite
docker compose run --rm etl ibge-localidades
docker compose run --rm etl ibge-malhas --level estados
docker compose run --rm etl ibge-milk --geo-level N3 --codes 31 --years 2000-2023

# NASA POWER
docker compose run --rm etl nasa-power --lat -21.7 --lon -43.4 \
  --start 2024-01-01 --end 2024-12-31

# Tudo de uma vez (sem nasa-power, sem inmet-live)
docker compose run --rm etl all
```

### Testes e lint dentro do container

```bash
docker compose run --rm --entrypoint pytest etl -q
docker compose run --rm --entrypoint ruff etl check src tests main.py
```

## Troubleshooting

**Permissões em `data/`**: o container roda como root por padrão; arquivos
criados em `./data` no host serão owned root. Para evitar:

```bash
docker compose run --rm --user "$(id -u):$(id -g)" etl <cmd>
```

**Proxy corporativo**: setar `HTTPS_PROXY` no `.env`. `requests` honra a
variável.

**Conexão INMET cai durante histórico**: o pipeline é idempotente — re-rodar
`inmet-history` pula ZIPs já baixados e o DuckDB é truncado e re-acumulado.

**Imagem ficou grande**: rebuild com `docker compose build --no-cache` se
suspeitar de cache corrompido. A imagem alvo é ~200 MB.

## Modo dev local (sem Docker)

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
python main.py inmet-stations
```

`DATA_DIR` é resolvido via `os.environ` em `src/utils.py` — fora do container,
default é `./data`.
