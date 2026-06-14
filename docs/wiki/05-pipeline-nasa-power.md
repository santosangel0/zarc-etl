# 05 — Pipeline NASA POWER

NASA POWER fornece dados meteorológicos derivados de reanálise/satélite para
qualquer ponto do globo, **sem token**. No `zarc-etl` ele entra em dois sabores:

| Sabor      | Endpoint                       | Resolução | Uso no projeto                          |
|------------|--------------------------------|-----------|-----------------------------------------|
| **Diário** | `temporal/daily/point`         | 1 dia     | série diária ad-hoc por ponto           |
| **Horário**| `temporal/hourly/point`        | 1 hora    | **imputar buracos horários do INMET**   |

Ambos usam a community `AG` (Agroclimatology) e `format=JSON`.

> ⚠️ **A fonte é reanálise MERRA-2**, numa grade de ~0,5° lat × 0,625° lon
> (~50 km). É um valor *modelado da célula*, **não** uma medição de estação.
> Ótimo como **preditor/fallback** para preencher lacunas, mas não reproduz o
> microclima local (altitude, efeito urbano). Para temperatura, prefira
> **corrigir o viés** contra os dados que a estação tem (offset/regressão por
> estação) antes de usar como valor imputado — ver §Imputação.

---

## 1. Parâmetros

Pedidos por padrão no fluxo horário (configurável via `--parameters`):

| Código  | Significado                    | Unidade |
|---------|--------------------------------|---------|
| `T2M`   | Temperatura a 2 m              | °C      |
| `RH2M`  | Umidade relativa a 2 m         | %       |

Outros parâmetros horários úteis para imputar **outras** colunas do INMET:
`T2MDEW` (ponto de orvalho), `PS` (pressão na superfície), `WS2M`/`WS10M`
(vento), `PRECTOTCORR` (precipitação), `ALLSKY_SFC_SW_DWN` (radiação).
Cada parâmetro pedido vira uma coluna Float64 em minúsculas no parquet.

Sentinela de ausência: **`-999`** → convertido para `null` (`_drop_sentinel`).

---

## 2. Cobertura e limites do endpoint horário

- **Início da cobertura: `2001-01-01`.** Datas anteriores são recusadas pela
  API ("The data starts at 2001/01/01"). O histórico INMET começa em 2000, então
  o **ano 2000 não é imputável** pelo horário do POWER.
- **`time-standard=UTC`** — as chaves vêm `YYYYMMDDHH` em UTC, alinhadas com a
  coluna `data`/hora do INMET (que também é UTC).
- **Limite de extensão (JSON):** requisições acima de **~17 anos** retornam
  HTTP 422 ("shorten your requested time extent"). Por isso o coletor por
  estação **fatia** o período em blocos (`--chunk-years`, padrão **10 anos** —
  margem segura, vale também quando se pede mais parâmetros).

---

## 3. Comandos (CLI)

### 3.1 Diário (ponto único)

```bash
docker compose run --rm etl nasa-power \
  --lat -21.7 --lon -43.4 --start 2024-01-01 --end 2024-12-31
# → data/final/nasa_power_<lat>_<lon>_<start>_<end>.parquet
```

### 3.2 Horário — ponto único (ad-hoc)

```bash
docker compose run --rm etl nasa-power-hourly \
  --lat -21.7 --lon -43.4 --start 2024-01-01 --end 2024-12-31
# → data/final/nasa_power_hourly_<lat>_<lon>_<start>_<end>.parquet  (cd_estacao = null)
```

### 3.3 Horário — por estações INMET (artefato de imputação) ⭐

Lê as coordenadas de `estacoes.parquet` e consolida **todas** as estações
pedidas em um único `nasa_power_hourly.parquet`, pronto para join com
`inmet_historico`.

```bash
# Algumas estações, range curto (teste):
docker compose run --rm etl nasa-power-hourly --codes A422,A360 --years 2024-2024

# Todas as estações, cobertura completa (PESADO — ver §6):
docker compose run --rm etl nasa-power-hourly --all-stations --years 2001-2025
```

Flags relevantes: `--years 2001-2025`, `--chunk-years 10`,
`--parameters T2M,RH2M`, `--sleep 0.5` (pausa entre requests).

Pré-requisito: `estacoes.parquet` deve existir (rode `inmet-stations` antes).
As respostas cruas ficam em cache em `data/raw/nasa_power_hourly/`, então
re-rodar não refaz as chamadas já feitas.

---

## 4. Schema do `nasa_power_hourly.parquet`

| Coluna       | Tipo        | Descrição                                              |
|--------------|-------------|--------------------------------------------------------|
| `cd_estacao` | Utf8        | código INMET (null no modo ponto único)                |
| `datetime`   | Datetime µs | timestamp UTC completo                                 |
| `data`       | Utf8        | `YYYY-MM-DD` (ISO, **igual ao INMET**)                 |
| `hora`       | Int64       | hora UTC **0..23** (chave de join robusta)             |
| `lat`,`lon`  | Float64     | coordenadas da estação/ponto                           |
| `t2m`        | Float64     | temperatura a 2 m (°C)                                 |
| `rh2m`       | Float64     | umidade relativa a 2 m (%)                             |
| `<extras>`   | Float64     | um por parâmetro adicional pedido                      |

Layout físico: **ZSTD**, ordenado por `(cd_estacao, data, hora)`, row-group de
100k — mesmo padrão do `inmet_historico` (predicate pushdown). Validado por
`scripts/validate_parquet.py` (contrato `nasa_power_hourly`).

> **Por que `hora` é inteiro, e não string?** O `hora_utc` cru do INMET tem
> formatos **inconsistentes** entre anos/estações: `"0000 UTC"`, `"00:00:00"`,
> `"00:00"`… Um join por string casaria 0 linhas silenciosamente. A hora
> numérica 0..23 é o denominador comum confiável.

---

## 5. Imputação: como juntar com o INMET

O `inmet_historico` é horário e cru (tudo Utf8). A chave de join é
`(cd_estacao, data, hora)` — basta normalizar a hora crua do INMET para inteiro:

```python
import polars as pl

nasa = pl.read_parquet("data/final/nasa_power_hourly.parquet")

inmet = (
    pl.scan_parquet("data/final/inmet_historico.parquet")
    .filter(pl.col("cd_estacao") == "A360")          # predicate pushdown (arquivo ordenado)
    # hora crua ("0000 UTC" / "00:00:00" / ...) -> inteiro 0..23
    .with_columns(pl.col("hora_utc").str.extract(r"(\d{1,2})").cast(pl.Int64).alias("hora"))
    .with_columns(
        pl.col("temperatura_do_ar_bulbo_seco_horaria_c").cast(pl.Float64, strict=False).alias("temp")
    )
    .collect()
)

j = inmet.join(
    nasa.select(["cd_estacao", "data", "hora", "t2m", "rh2m"]),
    on=["cd_estacao", "data", "hora"], how="left",
)

# Imputação ingênua (substituição direta — ver ressalva de viés abaixo):
j = j.with_columns(pl.coalesce(["temp", "t2m"]).alias("temp_imputada"))
```

Validação real (estação A360, 2024): das **8784** horas, o INMET tinha **3546**
sem temperatura e o NASA POWER cobriu **as 3546** — join casou 8784/8784.

### Correção de viés (recomendado para T2M)

Substituir direto o `t2m` da grade pode introduzir um degrau (a célula MERRA-2
não está na altitude da estação). Antes de imputar, estime um offset por estação
usando as horas em que **ambos** existem:

```python
pareados = j.filter(pl.col("temp").is_not_null() & pl.col("t2m").is_not_null())
offset = (pareados["temp"] - pareados["t2m"]).mean()        # viés médio estação-grade
j = j.with_columns(
    pl.coalesce(["temp", pl.col("t2m") + offset]).alias("temp_imputada")
)
```

(Para RH2M o viés costuma ser menor; ainda assim vale checar por estação.)

---

## 6. Runbook — pull completo

Puxar **todas** as estações × 2001–2025 é pesado:

- ~671 estações × 3 blocos de 10 anos ≈ **~2.000 requisições** (com `--sleep`,
  dezenas de minutos). O cache em `data/raw/nasa_power_hourly/` torna re-execuções
  baratas e idempotentes.
- O parquet consolidado fica na casa de **dezenas de milhões de linhas**
  (estações × horas × anos).

Estratégia sugerida:

```bash
# 1. valida que as estações existem
docker compose run --rm etl inmet-stations

# 2. comece por um subconjunto / período para validar volume e custo
docker compose run --rm etl nasa-power-hourly --codes A360,A422 --years 2024-2024

# 3. valide o artefato
docker compose run --rm --entrypoint python etl \
  scripts/validate_parquet.py /data/final/nasa_power_hourly.parquet

# 4. então rode o pull completo (idempotente via cache)
docker compose run --rm etl nasa-power-hourly --all-stations --years 2001-2025
```

> ⚠️ **Memória:** o modo por estações **acumula todos os frames em memória** e
> escreve o parquet de uma vez só. Para 2 estações × 1 ano (≈17 mil linhas) é
> trivial, mas `--all-stations` × 2001–2025 (~10⁸ linhas) pode **estourar a RAM**
> desta máquina (~4,7 GB). Para o conjunto completo, rode **em lotes** (ex.: por
> UF/região via `--codes`) e concatene os parquets no fim com DuckDB/polars —
> ou peça para adicionar um writer streaming por lote. O cache em
> `data/raw/nasa_power_hourly/` mantém tudo idempotente entre lotes.
>
> Cada invocação **sobrescreve** `nasa_power_hourly.parquet` com as estações
> daquela chamada — em lotes, escreva em nomes distintos e combine ao final.

---

## 7. Código relevante

- `src/collect/nasa_power.py` — `fetch_point` (diário), `fetch_point_hourly`
  (horário, com cache).
- `src/transform/nasa_power.py` — `parse_point`, `parse_point_hourly`,
  `write_hourly_parquet`, `_drop_sentinel`.
- `main.py` — comandos `nasa-power`, `nasa-power-hourly`, `_chunk_year_range`.
- `scripts/validate_parquet.py` — contratos `nasa_power` e `nasa_power_hourly`.
- `tests/test_nasa_power.py` + fixtures `nasa_power_*_jf*.json`.
