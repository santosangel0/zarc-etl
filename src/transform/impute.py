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
from src.utils import final_dir, get_logger

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


def build_frame(codes: list[str], **paths) -> pl.DataFrame:
    """Grade NASA (completa) ⟕ INMET. `temp`/`rh` ficam null onde o INMET falta.

    Colunas: cd_estacao, data, hora, mes, ano, t2m, rh2m, temp, rh.
    """
    nasa = load_nasa_hourly(codes, paths.get("nasa_path"))
    inmet = load_inmet_hourly(codes, paths.get("history_path"))
    frame = nasa.join(inmet, on=["cd_estacao", "data", "hora"], how="left")
    return frame.with_columns(
        pl.col("data").str.slice(5, 2).cast(pl.Int32).alias("mes"),
        pl.col("data").str.slice(0, 4).cast(pl.Int32).alias("ano"),
    )


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


def _origem(frac: str) -> pl.Expr:
    return (
        pl.when(pl.col(frac) == 0).then(pl.lit("inmet"))
        .when(pl.col(frac) >= 1).then(pl.lit("nasa"))
        .otherwise(pl.lit("misto"))
    )


def aggregate_daily(hourly: pl.DataFrame, temp_col: str = "temp_imp", rh_col: str = "rh_imp") -> pl.DataFrame:
    """Diário (temp_med/max, umid_med/min, ITU) com origem e fração imputada."""
    daily = (
        hourly.group_by(["cd_estacao", "data"]).agg(
            pl.col(temp_col).mean().alias("temp_med"),
            pl.col(temp_col).max().alias("temp_max"),
            pl.col(rh_col).mean().alias("umid_med"),
            pl.col(rh_col).min().alias("umid_min"),
            pl.len().alias("n_horas"),
            (pl.col("temp_src") == "nasa").sum().alias("_t_imp"),
            (pl.col("temp_src") == "inmet").sum().alias("_t_obs"),
            (pl.col("rh_src") == "nasa").sum().alias("_r_imp"),
            (pl.col("rh_src") == "inmet").sum().alias("_r_obs"),
        )
        .with_columns(
            (pl.col("_t_imp") / (pl.col("_t_obs") + pl.col("_t_imp"))).alias("temp_frac_imp"),
            (pl.col("_r_imp") / (pl.col("_r_obs") + pl.col("_r_imp"))).alias("umid_frac_imp"),
        )
        .with_columns(
            _origem("temp_frac_imp").alias("temp_origem"),
            _origem("umid_frac_imp").alias("umid_origem"),
            compute_itu(pl.col("temp_med"), pl.col("umid_med")).alias("itu_med"),
            compute_itu(pl.col("temp_max"), pl.col("umid_min")).alias("itu_max"),
        )
        .with_columns(((pl.col("temp_frac_imp") > 0) | (pl.col("umid_frac_imp") > 0)).alias("itu_imputado"))
        .with_columns(pl.col("data").str.strptime(pl.Date, "%Y-%m-%d", strict=False))
        .drop(["_t_imp", "_t_obs", "_r_imp", "_r_obs"])
        .sort(["cd_estacao", "data"])
    )
    return daily.select(
        "cd_estacao", "data", "temp_med", "temp_max", "umid_med", "umid_min",
        "itu_med", "itu_max", "temp_origem", "umid_origem",
        "temp_frac_imp", "umid_frac_imp", "itu_imputado", "n_horas",
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
    """
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
    daily = aggregate_daily(hourly)
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
    L.append(f"- Estações (piloto): {', '.join(codes)}")
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
    L.append("| estação | dias | % dias c/ temp tocada | % horas temp imp | % horas umid imp |")
    L.append("|---|---|---|---|---|")
    for row in coverage.iter_rows(named=True):
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


def coverage_summary(daily: pl.DataFrame) -> pl.DataFrame:
    """Resumo de cobertura por estação: dias e fração imputada."""
    return (
        daily.group_by("cd_estacao")
        .agg(
            pl.len().alias("dias"),
            (pl.col("temp_origem") != "inmet").mean().alias("frac_dias_temp_tocada"),
            pl.col("temp_frac_imp").mean().alias("frac_horas_temp_imp"),
            pl.col("umid_frac_imp").mean().alias("frac_horas_umid_imp"),
        )
        .sort("cd_estacao")
    )
