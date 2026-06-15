"""Imputação de buracos horários do INMET usando NASA POWER (corrigido por viés).

Contexto (ver `docs/wiki/05` §5):
  • O `inmet_historico` horário tem lacunas (estação fora do ar). O NASA POWER
    (reanálise MERRA-2, ~50 km) é um bom *preditor* dessas lacunas para campos
    espacialmente suaves — temperatura e umidade — mas é a média de uma célula,
    não a medição da estação. Por isso corrigimos o viés célula→estação antes de
    imputar.
  • Variável de interesse: **ITU** (temperatura + umidade). Imputamos os INSUMOS
    horários e recalculamos o ITU diário com `compute_itu` (mesma fórmula do
    produto observado), nunca o ITU diretamente.

Pipeline:
  1. Grade horária do NASA (cobertura completa) ⟕ INMET em (cd_estacao, data, hora).
  2. Correção de viés por (estação, mês). Dois métodos:
       linear  : temp ≈ a + b·t2m            (OLS — encolhe a variância por ~r)
       scaling : temp ≈ μ_inmet + (t2m−μ_nasa)·σ_inmet/σ_nasa  (preserva variância)
     `scaling` é o padrão: o ITU_max depende de extremos e o OLS subestima
     extremos (regressão à média), o que *subdetecta estresse térmico*.
  3. Reaplica o QC ([-10,50] °C, [0,100] %) ao valor corrigido.
  4. Imputa só onde o INMET falta; marca a origem por variável.
  5. Agrega para diário e recalcula ITU (med e max).

Saída: `inmet_historico_diario_imputado.parquet` (schema do diário observado +
colunas `*_origem`/`*_frac_imp`/`itu_imputado`) e um relatório de qualidade com
validação por blocos (hold-out de anos inteiros).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from src.transform.inmet import compute_itu, qc_humidity, qc_temperature
from src.utils import final_dir, get_logger, interim_dir

log = get_logger(__name__)

INMET_TEMP_COL = "temperatura_do_ar_bulbo_seco_horaria_c"
INMET_RH_COL = "umidade_relativa_do_ar_horaria"

# Mínimo de horas pareadas (INMET e NASA presentes) para ajustar uma célula.
MIN_FIT_SAMPLES = 100

# Faixas de estresse térmico por ITU para bovinos (referência NRC/Buffington),
# usadas no relatório para medir "troca de classe" da imputação.
ITU_STRESS_BREAKS = (72.0, 79.0, 89.0)
ITU_STRESS_LABELS = ("conforto", "leve", "moderado", "severo")

# Colunas de estatística produzidas pelo ajuste (precisam ser removidas após usar).
_STAT_COLS = ["mx", "my", "sx", "sy", "r", "fit_level"]


# ── Carga das fontes horárias ─────────────────────────────────────────────────


def load_inmet_hourly(codes: list[str], history_path: Path | None = None) -> pl.DataFrame:
    """INMET horário (temp/umidade) com QC, hora→int e mês, só p/ `codes`."""
    src = history_path or (final_dir() / "inmet_historico.parquet")
    lf = (
        pl.scan_parquet(src)
        .filter(pl.col("cd_estacao").is_in(codes))
        .with_columns(
            # hora crua ("0000 UTC" / "00:00:00" / "00:00") → inteiro 0..23
            pl.col("hora_utc").str.extract(r"(\d{1,2})").cast(pl.Int64).alias("hora"),
            pl.col(INMET_TEMP_COL).cast(pl.Utf8).str.replace_all(",", ".").cast(pl.Float64, strict=False).alias("temp"),
            pl.col(INMET_RH_COL).cast(pl.Utf8).str.replace_all(",", ".").cast(pl.Float64, strict=False).alias("rh"),
        )
        .with_columns(
            qc_temperature(pl.col("temp")).alias("temp"),
            qc_humidity(pl.col("rh")).alias("rh"),
        )
        .select("cd_estacao", "data", "hora", "temp", "rh")
    )
    df = lf.collect()
    # Dedup defensivo: uma linha por (estação, data, hora) — média se houver duplicata.
    return df.group_by(["cd_estacao", "data", "hora"]).agg(
        pl.col("temp").mean(), pl.col("rh").mean()
    )


def load_nasa_hourly(codes: list[str], nasa_path: Path | None = None) -> pl.DataFrame:
    """Grade horária do NASA POWER (t2m/rh2m) só para `codes`."""
    src = nasa_path or (final_dir() / "nasa_power_hourly.parquet")
    return (
        pl.read_parquet(src)
        .filter(pl.col("cd_estacao").is_in(codes))
        .select("cd_estacao", "data", "hora", "t2m", "rh2m")
    )


def assemble_frame(nasa: pl.DataFrame, codes: list[str], history_path: Path | None = None) -> pl.DataFrame:
    """Junta uma grade NASA já em memória com o INMET horário + mes/ano.

    Colunas: cd_estacao, data, hora, t2m, rh2m, temp, rh, mes, ano.
    """
    inmet = load_inmet_hourly(codes, history_path)
    frame = nasa.join(inmet, on=["cd_estacao", "data", "hora"], how="left")
    return frame.with_columns(
        pl.col("data").str.slice(5, 2).cast(pl.Int32).alias("mes"),
        pl.col("data").str.slice(0, 4).cast(pl.Int32).alias("ano"),
    )


def build_frame(codes: list[str], **paths) -> pl.DataFrame:
    """Grade NASA (do parquet consolidado) ⟕ INMET. `temp`/`rh` null onde falta."""
    nasa = load_nasa_hourly(codes, paths.get("nasa_path"))
    return assemble_frame(nasa, codes, paths.get("history_path"))


# ── Ajuste do viés (estação × mês) com cascata de fallback ────────────────────


def _stats(paired: pl.DataFrame, xcol: str, ycol: str, by: list[str]) -> pl.DataFrame:
    agg = [
        pl.len().alias("n"),
        pl.col(xcol).mean().alias("mx"),
        pl.col(ycol).mean().alias("my"),
        pl.col(xcol).std().alias("sx"),
        pl.col(ycol).std().alias("sy"),
        pl.corr(xcol, ycol).alias("r"),
    ]
    return paired.group_by(by).agg(agg) if by else paired.select(agg)


def _ok(row: dict | None) -> bool:
    return bool(
        row
        and row["n"] is not None
        and row["n"] >= MIN_FIT_SAMPLES
        and row["sx"] is not None
        and row["sx"] > 0
        and row["r"] is not None
    )


def fit_params(fit_frame: pl.DataFrame, universe: pl.DataFrame, xcol: str, ycol: str) -> pl.DataFrame:
    """Resolve parâmetros (mx,my,sx,sy,r) por (estação,mês) com fallback.

    Cascata: célula (estação,mês) → estação → global. Garante um parâmetro válido
    para cada (estação,mês) presente em `universe`.
    """
    paired = fit_frame.drop_nulls([xcol, ycol])
    cell = {(r["cd_estacao"], r["mes"]): r for r in _stats(paired, xcol, ycol, ["cd_estacao", "mes"]).iter_rows(named=True)}
    station = {r["cd_estacao"]: r for r in _stats(paired, xcol, ycol, ["cd_estacao"]).iter_rows(named=True)}
    glob = _stats(paired, xcol, ycol, []).row(0, named=True)

    rows = []
    for u in universe.iter_rows(named=True):
        code, mes = u["cd_estacao"], u["mes"]
        c = cell.get((code, mes))
        s = station.get(code)
        if _ok(c):
            chosen, level = c, "cell"
        elif _ok(s):
            chosen, level = s, "station"
        else:
            chosen, level = glob, "global"
        rows.append(
            {
                "cd_estacao": code, "mes": mes,
                "mx": chosen["mx"], "my": chosen["my"],
                "sx": chosen["sx"], "sy": chosen["sy"], "r": chosen["r"],
                "fit_level": level,
            }
        )
    return pl.DataFrame(rows)


def correct(apply_frame: pl.DataFrame, params: pl.DataFrame, xcol: str, out: str, method: str, qc) -> pl.DataFrame:
    """Aplica a correção de viés ao preditor `xcol`, gerando `out` (com QC)."""
    f = apply_frame.join(params, on=["cd_estacao", "mes"], how="left")
    factor = pl.col("r") if method == "linear" else pl.lit(1.0)
    corrected = pl.col("my") + factor * (pl.col("sy") / pl.col("sx")) * (pl.col(xcol) - pl.col("mx"))
    return f.with_columns(qc(corrected).alias(out)).drop(_STAT_COLS)


def impute_hourly(frame: pl.DataFrame, method: str) -> pl.DataFrame:
    """Ajusta nos pares disponíveis e imputa temp/umidade onde o INMET falta."""
    universe = frame.select(["cd_estacao", "mes"]).unique()
    pt = fit_params(frame, universe, "t2m", "temp")
    pr = fit_params(frame, universe, "rh2m", "rh")
    frame = correct(frame, pt, "t2m", "temp_corr", method, qc_temperature)
    frame = correct(frame, pr, "rh2m", "rh_corr", method, qc_humidity)
    return frame.with_columns(
        pl.coalesce(["temp", "temp_corr"]).alias("temp_imp"),
        pl.coalesce(["rh", "rh_corr"]).alias("rh_imp"),
        pl.when(pl.col("temp").is_not_null()).then(pl.lit("inmet"))
        .when(pl.col("temp_corr").is_not_null()).then(pl.lit("nasa")).otherwise(None).alias("temp_src"),
        pl.when(pl.col("rh").is_not_null()).then(pl.lit("inmet"))
        .when(pl.col("rh_corr").is_not_null()).then(pl.lit("nasa")).otherwise(None).alias("rh_src"),
    )


# ── Agregação diária + ITU ────────────────────────────────────────────────────


def _origem(obs: str, imp: str) -> pl.Expr:
    """Origem do dia a partir das contagens de horas (null-safe)."""
    total = pl.col(obs) + pl.col(imp)
    return (
        pl.when(total == 0).then(None)            # nenhum dado (nem obs nem NASA)
        .when(pl.col(imp) == 0).then(pl.lit("inmet"))
        .when(pl.col(obs) == 0).then(pl.lit("nasa"))
        .otherwise(pl.lit("misto"))
    )


def _frac(obs: str, imp: str) -> pl.Expr:
    """Fração de horas imputadas; null (não NaN) quando não há horas."""
    total = pl.col(obs) + pl.col(imp)
    return pl.when(total > 0).then(pl.col(imp) / total).otherwise(None)


def aggregate_daily(hourly: pl.DataFrame, temp_col: str = "temp_imp", rh_col: str = "rh_imp") -> pl.DataFrame:
    """Diário (temp_med/max, umid_med/min, ITU) com origem, fração e horas observadas."""
    daily = (
        hourly.group_by(["cd_estacao", "data"]).agg(
            pl.col(temp_col).mean().alias("temp_med"),
            pl.col(temp_col).max().alias("temp_max"),
            pl.col(rh_col).mean().alias("umid_med"),
            pl.col(rh_col).min().alias("umid_min"),
            pl.len().alias("n_horas"),
            (pl.col("temp_src") == "nasa").sum().alias("_t_imp"),
            (pl.col("temp_src") == "inmet").sum().alias("n_temp_obs"),
            (pl.col("rh_src") == "nasa").sum().alias("_r_imp"),
            (pl.col("rh_src") == "inmet").sum().alias("n_umid_obs"),
        )
        .with_columns(
            _frac("n_temp_obs", "_t_imp").alias("temp_frac_imp"),
            _frac("n_umid_obs", "_r_imp").alias("umid_frac_imp"),
            _origem("n_temp_obs", "_t_imp").alias("temp_origem"),
            _origem("n_umid_obs", "_r_imp").alias("umid_origem"),
            compute_itu(pl.col("temp_med"), pl.col("umid_med")).alias("itu_med"),
            compute_itu(pl.col("temp_max"), pl.col("umid_min")).alias("itu_max"),
        )
        .with_columns(((pl.col("temp_frac_imp") > 0) | (pl.col("umid_frac_imp") > 0)).alias("itu_imputado"))
        .with_columns(pl.col("data").str.strptime(pl.Date, "%Y-%m-%d", strict=False))
        .sort(["cd_estacao", "data"])
    )
    return daily.select(
        "cd_estacao", "data", "temp_med", "temp_max", "umid_med", "umid_min",
        "itu_med", "itu_max", "temp_origem", "umid_origem",
        "temp_frac_imp", "umid_frac_imp", "itu_imputado", "n_horas",
        "n_temp_obs", "n_umid_obs",
    )


# ── Validação por blocos (hold-out de anos) ───────────────────────────────────


def _metrics(truth: np.ndarray, pred: np.ndarray) -> dict:
    m = ~(np.isnan(truth) | np.isnan(pred))
    t, p = truth[m], pred[m]
    if t.size == 0:
        return {"n": 0, "rmse": float("nan"), "mae": float("nan"), "bias": float("nan"), "r2": float("nan"), "var_ratio": float("nan")}
    e = p - t
    ss_res = float((e**2).sum())
    ss_tot = float(((t - t.mean()) ** 2).sum())
    return {
        "n": int(t.size),
        "rmse": float(np.sqrt((e**2).mean())),
        "mae": float(np.abs(e).mean()),
        "bias": float(e.mean()),
        "r2": (1 - ss_res / ss_tot) if ss_tot > 0 else float("nan"),
        "var_ratio": float(p.std() / t.std()) if t.std() > 0 else float("nan"),
    }


def _stress_class(itu: np.ndarray) -> np.ndarray:
    return np.digitize(itu, ITU_STRESS_BREAKS)


def validate(codes: list[str], test_years: list[int], method: str, **paths) -> dict:
    """Hold-out por anos: ajusta no treino, prediz no teste (horas observadas).

    Retorna métricas horárias (temp/umidade) e diárias no nível do ITU, medindo
    o pior caso realista: dias totalmente observados re-imputados 100% pelo NASA.

    `frame` pode ser passado pronto (ex.: grade NASA buscada on-the-fly); caso
    contrário é lido do parquet consolidado.
    """
    frame = paths.pop("frame", None)
    if frame is None:
        frame = build_frame(codes, **paths)
    universe = frame.select(["cd_estacao", "mes"]).unique()
    train = frame.filter(~pl.col("ano").is_in(test_years))
    test = frame.filter(pl.col("ano").is_in(test_years))

    pt = fit_params(train, universe, "t2m", "temp")
    pr = fit_params(train, universe, "rh2m", "rh")
    test = correct(test, pt, "t2m", "temp_corr", method, qc_temperature)
    test = correct(test, pr, "rh2m", "rh_corr", method, qc_humidity)

    out: dict = {"method": method, "test_years": test_years, "hourly": {}, "daily_itu": {}, "per_station": {}}

    # Horário: erro do corrigido vs observado, onde ambos existem.
    for var, xc, yc in [("temp", "temp_corr", "temp"), ("rh", "rh_corr", "rh")]:
        pair = test.drop_nulls([xc, yc])
        out["hourly"][var] = _metrics(pair[yc].to_numpy(), pair[xc].to_numpy())

    # Diário/ITU: dias REALMENTE completos (verdade) vs 100% imputados (predição).
    # Verdade só de dias com ≥23 horas observadas — caso contrário temp_max/umid_min
    # sairiam de horas parciais e o erro mediria agregação, não imputação.
    truth = (
        test.group_by(["cd_estacao", "data"]).agg(
            pl.col("temp").is_not_null().sum().alias("n_obs_t"),
            pl.col("rh").is_not_null().sum().alias("n_obs_r"),
            pl.col("temp").mean().alias("temp_med"),
            pl.col("temp").max().alias("temp_max"),
            pl.col("rh").mean().alias("umid_med"),
            pl.col("rh").min().alias("umid_min"),
        )
        .filter((pl.col("n_obs_t") >= 23) & (pl.col("n_obs_r") >= 23))
        .with_columns(
            pl.col("data").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            compute_itu(pl.col("temp_med"), pl.col("umid_med")).alias("itu_med"),
            compute_itu(pl.col("temp_max"), pl.col("umid_min")).alias("itu_max"),
        )
    )

    pred = aggregate_daily(
        test.with_columns(pl.lit("nasa").alias("temp_src"), pl.lit("nasa").alias("rh_src")),
        temp_col="temp_corr", rh_col="rh_corr",
    )
    j = truth.join(pred, on=["cd_estacao", "data"], suffix="_pred")
    out["daily_itu"]["n_dias_completos"] = j.height
    for col in ["temp_med", "temp_max", "itu_med", "itu_max"]:
        out["daily_itu"][col] = _metrics(j[col].to_numpy(), j[f"{col}_pred"].to_numpy())

    # Troca de classe de estresse (ITU_max).
    tc, pc = _stress_class(j["itu_max"].to_numpy()), _stress_class(j["itu_max_pred"].to_numpy())
    valid = ~(np.isnan(j["itu_max"].to_numpy()) | np.isnan(j["itu_max_pred"].to_numpy()))
    out["daily_itu"]["itu_max_class_flip_pct"] = float((tc[valid] != pc[valid]).mean() * 100) if valid.any() else float("nan")

    # Correlação/viés horário por estação (resposta empírica à pergunta de correlação).
    for code in codes:
        sub = test.filter(pl.col("cd_estacao") == code)
        pt_ = sub.drop_nulls(["temp_corr", "temp"])
        pr_ = sub.drop_nulls(["rh_corr", "rh"])
        out["per_station"][code] = {
            "temp": _metrics(pt_["temp"].to_numpy(), pt_["temp_corr"].to_numpy()),
            "rh": _metrics(pr_["rh"].to_numpy(), pr_["rh_corr"].to_numpy()),
        }
    return out


# ── Pipeline público ──────────────────────────────────────────────────────────


def impute_daily(
    codes: list[str],
    method: str = "scaling",
    output_path: Path | None = None,
    **paths,
) -> tuple[Path, pl.DataFrame]:
    """Imputa e escreve `inmet_historico_diario_imputado.parquet`. Não sobrescreve
    o produto observado."""
    out = output_path or (final_dir() / "inmet_historico_diario_imputado.parquet")
    frame = build_frame(codes, **paths)
    hourly = impute_hourly(frame, method)
    daily = add_tipo(aggregate_daily(hourly))
    daily.write_parquet(out, compression="zstd", compression_level=3, row_group_size=100_000)
    log.info("wrote %s rows=%d (method=%s)", out, daily.height, method)
    return out, daily


def format_report(val: dict, coverage: pl.DataFrame, codes: list[str]) -> str:
    """Relatório de qualidade em Markdown a partir da validação + cobertura."""
    def f(d: dict, k: str, nd: int = 2) -> str:
        v = d.get(k, float("nan"))
        return "—" if v != v else f"{v:.{nd}f}"

    L = []
    L.append("# Relatório de imputação INMET ← NASA POWER\n")
    L.append(f"- Método de correção de viés: **{val['method']}**")
    L.append(f"- Estações da validação ({len(codes)}): {', '.join(codes)}")
    L.append(f"- Hold-out (anos de teste): {val['test_years']}")
    L.append("- Validação = ajusta no treino, prediz no teste; o nível ITU usa o "
             "**pior caso**: dias 100% observados re-imputados inteiramente pelo NASA.\n")

    h = val["hourly"]
    L.append("## Erro horário (corrigido vs observado)\n")
    L.append("| var | n | RMSE | MAE | viés | R² | var_ratio (pred/obs) |")
    L.append("|---|---|---|---|---|---|---|")
    for var, unit in [("temp", "°C"), ("rh", "%")]:
        d = h[var]
        L.append(f"| {var} | {d['n']} | {f(d,'rmse')} {unit} | {f(d,'mae')} {unit} | {f(d,'bias')} {unit} | {f(d,'r2')} | {f(d,'var_ratio')} |")

    di = val["daily_itu"]
    L.append("\n## Erro no nível diário/ITU (dias inteiros imputados)\n")
    L.append("| métrica | n | RMSE | MAE | viés | R² |")
    L.append("|---|---|---|---|---|---|")
    for k, lab in [("temp_med", "temp_med"), ("temp_max", "temp_max"), ("itu_med", "ITU_med"), ("itu_max", "ITU_max")]:
        d = di[k]
        L.append(f"| {lab} | {d['n']} | {f(d,'rmse')} | {f(d,'mae')} | {f(d,'bias')} | {f(d,'r2')} |")
    L.append(f"\n**Troca de classe de estresse (ITU_max):** {f(di,'itu_max_class_flip_pct',1)}% dos dias mudam de faixa quando imputados.")

    L.append("\n## Por estação (teste, horário)\n")
    L.append("| estação | temp R² | temp viés | temp var_ratio | umid R² | umid viés |")
    L.append("|---|---|---|---|---|---|")
    for code in codes:
        ps = val["per_station"].get(code, {})
        t, r = ps.get("temp", {}), ps.get("rh", {})
        L.append(f"| {code} | {f(t,'r2')} | {f(t,'bias')} | {f(t,'var_ratio')} | {f(r,'r2')} | {f(r,'bias')} |")

    L.append("\n## Cobertura (todo o período imputado)\n")
    n_est = coverage.height
    overall = coverage.select(
        dias=pl.col("dias").sum(),
        ft=pl.col("frac_horas_temp_imp").mean(),
        fr=pl.col("frac_horas_umid_imp").mean(),
    ).row(0, named=True)
    L.append(f"- **{n_est} estações**, {overall['dias']:,} dias-estação no total.")
    L.append(f"- Fração média de horas imputadas: temp {overall['ft']*100:.1f}%, umidade {overall['fr']*100:.1f}%.\n")
    shown = coverage.sort("frac_horas_temp_imp", descending=True)
    title = "| estação | dias | % dias c/ temp tocada | % horas temp imp | % horas umid imp |"
    if n_est > 25:
        L.append("Estações com maior fração imputada (top 15):\n")
        shown = shown.head(15)
    L.append(title)
    L.append("|---|---|---|---|---|")
    for row in shown.iter_rows(named=True):
        L.append(
            f"| {row['cd_estacao']} | {row['dias']} | "
            f"{row['frac_dias_temp_tocada']*100:.1f}% | {row['frac_horas_temp_imp']*100:.1f}% | {row['frac_horas_umid_imp']*100:.1f}% |"
        )

    L.append("\n## Ressalvas\n")
    L.append("- **Missingness não é aleatória**: estações falham em eventos extremos "
             "(tempestade/calor), então as métricas medidas em dados *presentes* são "
             "otimistas para os períodos realmente faltantes.")
    L.append("- NASA POWER é média de célula ~50 km (não a estação): bom para temp/umidade, "
             "fraco para precipitação (não imputada aqui).")
    return "\n".join(L) + "\n"


def add_tipo(daily: pl.DataFrame) -> pl.DataFrame:
    """Rotula cada dia-estação em `tipo`: observado / gap-fill / backfill.

    - `observado`: o dia tem ao menos uma hora real do INMET (origem inmet/misto).
    - `gap-fill`: dia 100% imputado DENTRO do período operacional (lacuna real).
    - `backfill`: dia 100% imputado ANTES da 1ª observação da estação — anos em
      que ela ainda não existia (climatologia do ponto, não dado de estação).

    Precisa da série completa por estação (chamar após o concat, não por lote).
    """
    t0 = (
        daily.filter(pl.col("temp_origem") != "nasa")
        .group_by("cd_estacao")
        .agg(pl.col("data").min().alias("_t0"))
    )
    return (
        daily.join(t0, on="cd_estacao", how="left")
        .with_columns(
            pl.when(pl.col("temp_origem") != "nasa").then(pl.lit("observado"))
            .when(pl.col("_t0").is_null() | (pl.col("data") < pl.col("_t0"))).then(pl.lit("backfill"))
            .otherwise(pl.lit("gap-fill"))
            .alias("tipo")
        )
        .drop("_t0")
    )


def coverage_summary(daily: pl.DataFrame) -> pl.DataFrame:
    """Resumo de cobertura por estação: dias e fração imputada."""
    return (
        daily.group_by("cd_estacao")
        .agg(
            pl.len().alias("dias"),
            (pl.col("temp_origem") != "inmet").mean().alias("frac_dias_temp_tocada"),
            pl.col("temp_frac_imp").drop_nulls().mean().alias("frac_horas_temp_imp"),
            pl.col("umid_frac_imp").drop_nulls().mean().alias("frac_horas_umid_imp"),
        )
        .sort("cd_estacao")
    )


# ── Escala completa: busca NASA on-the-fly, por lotes, retomável ──────────────


def station_coords(codes: list[str] | None = None, estacoes_path: Path | None = None) -> pl.DataFrame:
    """(cd_estacao, vl_latitude, vl_longitude) de estacoes.parquet, sem nulos."""
    src = estacoes_path or (final_dir() / "estacoes.parquet")
    e = pl.read_parquet(src, columns=["cd_estacao", "vl_latitude", "vl_longitude"]).drop_nulls().unique("cd_estacao")
    if codes:
        e = e.filter(pl.col("cd_estacao").is_in(codes))
    return e.sort("cd_estacao")


def _year_chunks(years: tuple[int, int], size: int) -> list[tuple[str, str]]:
    yrs = list(range(years[0], years[1] + 1))
    return [(f"{yrs[i]}-01-01", f"{yrs[min(i + size - 1, len(yrs) - 1)]}-12-31") for i in range(0, len(yrs), size)]


def fetch_nasa_frame(
    coords: pl.DataFrame,
    years: tuple[int, int] = (2001, 2024),
    chunk_years: int = 10,
    sleep: float = 0.2,
    parameters: tuple[str, ...] = ("T2M", "RH2M"),
) -> pl.DataFrame:
    """Busca a grade horária do NASA POWER para as estações de `coords` (cacheado).

    Mantém só as estações pedidas em memória — seguro para lotes. Falhas de rede
    por estação são logadas e puladas (não derrubam o lote).
    """
    import time

    from src.collect.nasa_power import fetch_point_hourly
    from src.transform.nasa_power import parse_point_hourly

    chunks = _year_chunks(years, chunk_years)
    frames: list[pl.DataFrame] = []
    for row in coords.iter_rows(named=True):
        code, lat, lon = row["cd_estacao"], row["vl_latitude"], row["vl_longitude"]
        for c_start, c_end in chunks:
            try:
                payload = fetch_point_hourly(lat, lon, c_start, c_end, parameters)
                frames.append(parse_point_hourly(payload, cd_estacao=code))
            except Exception as exc:  # rede/HTTP — pula a estação/chunk
                log.warning("nasa fetch falhou %s %s..%s: %s", code, c_start, c_end, exc)
            time.sleep(sleep)
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical_relaxed").select("cd_estacao", "data", "hora", "t2m", "rh2m")


def impute_all(
    method: str = "scaling",
    years: tuple[int, int] = (2001, 2024),
    batch_size: int = 20,
    codes: list[str] | None = None,
    output_path: Path | None = None,
    staging_dir: Path | None = None,
    **paths,
) -> tuple[Path, pl.DataFrame]:
    """Imputa TODAS as estações em lotes (RAM-safe) e retomável.

    Cada lote: busca NASA on-the-fly (cache) → join INMET → imputa → agrega →
    grava `staging/batch_NNNN.parquet`. Lotes já gravados são pulados. No fim,
    concatena tudo em `inmet_historico_diario_imputado.parquet`. Só o diário
    (pequeno) é mantido entre lotes — o horário (~10⁸ linhas) nunca é materializado.
    """
    coords = station_coords(codes, paths.get("estacoes_path"))
    all_codes = coords["cd_estacao"].to_list()
    staging = staging_dir or interim_dir("impute_batches")
    staging.mkdir(parents=True, exist_ok=True)
    batches = [all_codes[i : i + batch_size] for i in range(0, len(all_codes), batch_size)]
    log.info("impute_all: %d estações em %d lotes de %d (método=%s)", len(all_codes), len(batches), batch_size, method)

    def _write(df: pl.DataFrame, path: Path) -> None:
        df.write_parquet(path, compression="zstd", compression_level=3, row_group_size=100_000)

    for bi, batch in enumerate(batches):
        done = staging / f"batch_{bi:04d}.parquet"          # lote COMPLETO (pula no rerun)
        partial = staging / f"batch_{bi:04d}.partial.parquet"  # incompleto → re-tenta
        if done.exists():
            continue
        nasa = fetch_nasa_frame(coords.filter(pl.col("cd_estacao").is_in(batch)), years)
        if nasa.height == 0:
            log.warning("lote %d/%d sem dados NASA, será re-tentado", bi + 1, len(batches))
            continue
        daily = aggregate_daily(impute_hourly(assemble_frame(nasa, batch, paths.get("history_path")), method))
        missing = sorted(set(batch) - set(daily["cd_estacao"].unique().to_list()))
        if missing:
            # Não marca como completo: falha (transitória?) seria cravada no rerun.
            _write(daily, partial)
            log.warning("lote %d/%d incompleto: faltam %s (gravado .partial)", bi + 1, len(batches), missing)
        else:
            _write(daily, done)
            partial.unlink(missing_ok=True)
            log.info("lote %d/%d ok: %d estações, %d dias", bi + 1, len(batches), len(batch), daily.height)

    # Concatena: prefere o lote completo; cai pro .partial quando só ele existir.
    parts: list[Path] = []
    for bi in range(len(batches)):
        done = staging / f"batch_{bi:04d}.parquet"
        partial = staging / f"batch_{bi:04d}.partial.parquet"
        parts.append(done if done.exists() else partial)
    parts = [p for p in parts if p.exists()]
    full = pl.concat([pl.read_parquet(p) for p in parts])

    # Mantém só estações que REALMENTE observaram algo no período. Estações sem
    # nenhuma hora observada (ex.: começaram a operar depois de `years`) seriam
    # 100% sintéticas — não é gap-filling, é fabricação; ficam de fora.
    have_data = (
        full.group_by("cd_estacao")
        .agg((pl.col("n_temp_obs").sum() + pl.col("n_umid_obs").sum()).alias("obs"))
        .filter(pl.col("obs") > 0)["cd_estacao"]
        .to_list()
    )
    n_drop = full["cd_estacao"].n_unique() - len(have_data)
    if n_drop:
        log.info("descartando %d estações sem dado observável em %s", n_drop, years)
    full = add_tipo(full.filter(pl.col("cd_estacao").is_in(have_data))).sort(["cd_estacao", "data"])

    out = output_path or (final_dir() / "inmet_historico_diario_imputado.parquet")
    _write(full, out)

    no_data = sorted(set(all_codes) - set(have_data))
    log.info("wrote %s rows=%d estações=%d/%d (%d sem dado no período)",
             out, full.height, len(have_data), len(all_codes), len(no_data))
    return out, full


def validate_sample(
    codes: list[str],
    test_years: list[int],
    method: str,
    years: tuple[int, int] = (2001, 2024),
    **paths,
) -> dict:
    """Validação hold-out numa amostra, buscando o NASA on-the-fly (escala completa)."""
    nasa = fetch_nasa_frame(station_coords(codes, paths.get("estacoes_path")), years)
    frame = assemble_frame(nasa, codes, paths.get("history_path"))
    return validate(codes, test_years, method, frame=frame)


def sample_codes(all_codes: list[str], n: int) -> list[str]:
    """Amostra determinística por passo (espalha pela lista ordenada)."""
    if len(all_codes) <= n:
        return all_codes
    step = len(all_codes) / n
    return [all_codes[int(i * step)] for i in range(n)]
