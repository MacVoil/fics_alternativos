from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
from shiny import App, reactive, render, ui

# Permite ejecutar la app tanto desde la raiz del proyecto como desde app/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from src.catalogo import load_dims, run_catalogo
from src.ingestion import run_ingestion
from src.processing import run_processing
from src.forecasting import run_forecasting

FORECASTS_DIR = PROJECT_ROOT / "data" / "forecasts"
PRED_PATH = FORECASTS_DIR / "fics_pronósticos_latest.parquet"
OBS_PATH = FORECASTS_DIR / "fics_observados_latest.parquet"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_fondo_value(v: str) -> dict:
    te, ce, cn, tp = v.split("|", 3)
    return {
        "tipo_entidad": int(te),
        "codigo_entidad": int(ce),
        "codigo_negocio": int(cn),
        "tipo_participacion": tp,
    }


def _build_selector_options() -> tuple[dict[str, str], pd.DataFrame]:
    dims = load_dims()
    part = dims["dim_participacion"].copy()

    key_cols = ["tipo_entidad", "codigo_entidad", "codigo_negocio"]

    if "dim_fondo" in dims:
        fondo = dims["dim_fondo"].copy()
        cols = key_cols + [c for c in ["nombre_patrimonio"] if c in fondo.columns]
        part = part.merge(fondo[cols].drop_duplicates(), on=key_cols, how="left")

    if "dim_entidad" in dims:
        ent = dims["dim_entidad"].copy()
        cols = ["tipo_entidad", "codigo_entidad"] + [c for c in ["nombre_entidad"] if c in ent.columns]
        part = part.merge(ent[cols].drop_duplicates(), on=["tipo_entidad", "codigo_entidad"], how="left")

    for c in ["nombre_entidad", "nombre_patrimonio"]:
        if c not in part.columns:
            part[c] = ""

    part = part.drop_duplicates(
        subset=["tipo_entidad", "codigo_entidad", "codigo_negocio", "tipo_participacion"]
    ).reset_index(drop=True)

    part["value"] = (
        part["tipo_entidad"].astype(str)
        + "|"
        + part["codigo_entidad"].astype(str)
        + "|"
        + part["codigo_negocio"].astype(str)
        + "|"
        + part["tipo_participacion"].astype(str)
    )

    part["label"] = (
        part["nombre_entidad"].fillna("").astype(str)
        + " | "
        + part["nombre_patrimonio"].fillna("").astype(str)
        + " | part "
        + part["tipo_participacion"].astype(str)
        + " | ("
        + part["tipo_entidad"].astype(str)
        + "-"
        + part["codigo_entidad"].astype(str)
        + "-"
        + part["codigo_negocio"].astype(str)
        + ")"
    )

    part = part.sort_values("label")
    choices = dict(zip(part["value"], part["label"]))
    return choices, part


def _load_observados() -> pd.DataFrame:
    if not OBS_PATH.exists():
        return pd.DataFrame()
    return pd.read_parquet(OBS_PATH)


def _load_predicciones() -> pd.DataFrame:
    if not PRED_PATH.exists():
        return pd.DataFrame()
    return pd.read_parquet(PRED_PATH)


def _series_name(df: pd.DataFrame) -> pd.Series:
    return (
        df["tipo_entidad"].astype(str)
        + "-"
        + df["codigo_entidad"].astype(str)
        + "-"
        + df["codigo_negocio"].astype(str)
        + "-"
        + df["tipo_participacion"].astype(str)
        + " | "
        + df["tipo_rentabilidad"].astype(str)
    )


def _filter_by_selection(df: pd.DataFrame, selected_values: list[str], tipos_rent: list[str]) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    if selected_values:
        selected_keys = {
            tuple(v.split("|", 3)) for v in selected_values
        }
        out = out[
            out.apply(
                lambda r: (
                    str(r["tipo_entidad"]),
                    str(r["codigo_entidad"]),
                    str(r["codigo_negocio"]),
                    str(r["tipo_participacion"]),
                )
                in selected_keys,
                axis=1,
            )
        ]

    if tipos_rent:
        out = out[out["tipo_rentabilidad"].isin(tipos_rent)]

    return out


# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------

app_ui = ui.page_fluid(
    ui.h2("Dashboard FICs Alternativos"),
    ui.p("Analisis historico y forecasting de rentabilidades por fondo."),
    ui.layout_sidebar(
        ui.sidebar(
            ui.h4("Pipeline"),
            ui.input_action_button("btn_catalogo", "1) Actualizar catalogo", class_="btn-primary"),
            ui.br(),
            ui.br(),
            ui.input_selectize(
                "fondos",
                "Selecciona hasta 5 fondos/participaciones",
                choices={},
                multiple=True,
                options={"maxItems": 5},
            ),
            ui.input_action_button("btn_ingestion", "2) Actualizar datos", class_="btn-primary"),
            ui.br(),
            ui.br(),
            ui.input_action_button("btn_processing", "3) Procesar rentabilidades", class_="btn-primary"),
            ui.br(),
            ui.br(),
            ui.input_action_button("btn_forecasting", "4) Entrenar modelos", class_="btn-primary"),
            ui.hr(),
            ui.input_selectize(
                "tipo_rent",
                "Tipo de rentabilidad",
                choices=[],
                multiple=True,
            ),
            width=360,
        ),
        ui.navset_tab(
            ui.nav_panel(
                "Estado",
                ui.h4("Estado del pipeline"),
                ui.output_text_verbatim("estado_pipeline"),
            ),
            ui.nav_panel(
                "Observados",
                ui.h4("Rentabilidades observadas (entrenamiento)"),
                ui.output_plot("plot_observados"),
                ui.output_data_frame("tabla_observados"),
            ),
            ui.nav_panel(
                "Forecast",
                ui.h4("Predicciones de rentabilidad"),
                ui.output_plot("plot_forecast"),
                ui.output_data_frame("tabla_forecast"),
            ),
            ui.nav_panel(
                "Comparativo",
                ui.h4("Resumen comparativo por fondo y rentabilidad"),
                ui.output_data_frame("tabla_resumen"),
            ),
        ),
    ),
)


# -----------------------------------------------------------------------------
# Server
# -----------------------------------------------------------------------------

def server(input, output, session):
    status_lines = reactive.value([f"[{_now()}] App iniciada."])

    def add_status(msg: str) -> None:
        # Evita crear dependencias reactivas en efectos que solo escriben estado.
        with reactive.isolate():
            lines = status_lines.get().copy()
        lines.append(f"[{_now()}] {msg}")
        status_lines.set(lines)

    def refresh_selector() -> None:
        try:
            choices, _ = _build_selector_options()
            ui.update_selectize("fondos", choices=choices, selected=[])
            add_status(f"Catalogo cargado: {len(choices)} opciones disponibles.")
        except Exception as e:
            add_status(f"No se pudo cargar el catalogo: {e}")

    def refresh_tipos_rent() -> None:
        tipos = set()
        try:
            obs = _load_observados()
            if not obs.empty and "tipo_rentabilidad" in obs.columns:
                tipos.update(obs["tipo_rentabilidad"].dropna().unique().tolist())
        except Exception:
            pass

        try:
            pred = _load_predicciones()
            if not pred.empty and "tipo_rentabilidad" in pred.columns:
                tipos.update(pred["tipo_rentabilidad"].dropna().unique().tolist())
        except Exception:
            pass

        tipos = sorted(tipos)
        ui.update_selectize("tipo_rent", choices=tipos, selected=tipos)

    @reactive.effect
    def _init_loads():
        refresh_selector()
        refresh_tipos_rent()

    @reactive.effect
    @reactive.event(input.btn_catalogo)
    def _run_catalogo():
        try:
            add_status("Ejecutando catalogo...")
            run_catalogo()
            refresh_selector()
            add_status("Catalogo actualizado correctamente.")
        except Exception as e:
            add_status(f"Error en catalogo: {e}")

    @reactive.effect
    @reactive.event(input.btn_ingestion)
    def _run_ingestion():
        try:
            selected = input.fondos() or []
            if not selected:
                add_status("Selecciona al menos un fondo para ejecutar ingestion.")
                return

            fondos = [_parse_fondo_value(v) for v in selected]
            add_status(f"Ejecutando ingestion para {len(fondos)} fondo(s)...")
            df = run_ingestion(fondos)
            add_status(f"Ingestion completada ({len(df):,} filas).")
        except Exception as e:
            add_status(f"Error en ingestion: {e}")

    @reactive.effect
    @reactive.event(input.btn_processing)
    def _run_processing():
        try:
            add_status("Ejecutando processing...")
            df = run_processing()
            add_status(f"Processing completado ({len(df):,} filas).")
        except Exception as e:
            add_status(f"Error en processing: {e}")

    @reactive.effect
    @reactive.event(input.btn_forecasting)
    def _run_forecasting():
        try:
            add_status("Ejecutando forecasting...")
            df = run_forecasting()
            if df is None:
                add_status("Forecasting finalizo sin predicciones validas.")
            else:
                add_status(f"Forecasting completado ({len(df):,} filas de prediccion).")
            refresh_tipos_rent()
        except Exception as e:
            add_status(f"Error en forecasting: {e}")

    @output
    @render.text
    def estado_pipeline():
        return "\n".join(status_lines.get())

    @reactive.calc
    def observed_filtered() -> pd.DataFrame:
        df = _load_observados()
        return _filter_by_selection(df, input.fondos() or [], input.tipo_rent() or [])

    @reactive.calc
    def forecast_filtered() -> pd.DataFrame:
        df = _load_predicciones()
        return _filter_by_selection(df, input.fondos() or [], input.tipo_rent() or [])

    @output
    @render.plot(alt="Series observadas")
    def plot_observados():
        df = observed_filtered()
        if df.empty:
            return None

        plot_df = df.copy()
        plot_df["serie"] = _series_name(plot_df)
        fig = px.line(
            plot_df,
            x="fecha_corte",
            y="rentabilidad",
            color="serie",
            title="Observados por fondo y tipo de rentabilidad",
        )
        fig.update_layout(legend_title_text="Serie")
        return fig

    @output
    @render.data_frame
    def tabla_observados():
        df = observed_filtered().sort_values(["fecha_corte"]).reset_index(drop=True)
        if df.empty:
            return render.DataGrid(pd.DataFrame({"mensaje": ["No hay observados para mostrar."]}))
        return render.DataGrid(df)

    @output
    @render.plot(alt="Series forecast")
    def plot_forecast():
        df = forecast_filtered()
        if df.empty:
            return None

        plot_df = df.copy()
        plot_df["serie"] = _series_name(plot_df)

        y_col = "mean" if "mean" in plot_df.columns else ("p0.5" if "p0.5" in plot_df.columns else None)
        if y_col is None:
            return None

        fig = px.line(
            plot_df,
            x="fecha_corte",
            y=y_col,
            color="serie",
            title=f"Forecast por serie ({y_col})",
        )
        fig.update_layout(legend_title_text="Serie")
        return fig

    @output
    @render.data_frame
    def tabla_forecast():
        df = forecast_filtered().sort_values(["fecha_corte"]).reset_index(drop=True)
        if df.empty:
            return render.DataGrid(pd.DataFrame({"mensaje": ["No hay predicciones para mostrar."]}))
        return render.DataGrid(df)

    @output
    @render.data_frame
    def tabla_resumen():
        obs = observed_filtered()
        pred = forecast_filtered()

        if obs.empty:
            return render.DataGrid(pd.DataFrame({"mensaje": ["No hay datos observados para resumen."]}))

        keys = [
            "tipo_entidad",
            "codigo_entidad",
            "codigo_negocio",
            "tipo_participacion",
            "tipo_rentabilidad",
        ]

        resumen_obs = (
            obs.groupby(keys)
            .agg(
                obs_n=("rentabilidad", "count"),
                obs_media=("rentabilidad", "mean"),
                obs_vol=("rentabilidad", "std"),
                obs_ultima_fecha=("fecha_corte", "max"),
            )
            .reset_index()
        )

        if pred.empty:
            return render.DataGrid(resumen_obs)

        y_col = "mean" if "mean" in pred.columns else ("p0.5" if "p0.5" in pred.columns else None)
        if y_col is None:
            return render.DataGrid(resumen_obs)

        idx = pred.groupby(keys)["fecha_corte"].idxmax()
        pred_last = pred.loc[idx, keys + ["fecha_corte", y_col]].rename(
            columns={"fecha_corte": "fcst_ultima_fecha", y_col: "fcst_ultimo"}
        )

        resumen = resumen_obs.merge(pred_last, on=keys, how="left")
        return render.DataGrid(resumen)


app = App(app_ui, server)
