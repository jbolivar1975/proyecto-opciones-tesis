import os
import glob
import pandas as pd
from dash import Dash, dcc, html, Input, Output, dash_table
import plotly.graph_objects as go

# =================== CARGA DE DATOS ===================

FEAT_PATH = os.path.join("data", "options", "features", "options_features_daily.parquet")
DAILY_DIR = os.path.join("data", "options", "daily")

def load_features():
    if not os.path.exists(FEAT_PATH):
        raise FileNotFoundError(f"No existe {FEAT_PATH}. Ejecuta primero build_options_features.py")
    df = pd.read_parquet(FEAT_PATH)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df

def load_latest_raw():
    """Carga el último snapshot diario crudo (options_YYYY-MM-DD.parquet)."""
    files = glob.glob(os.path.join(DAILY_DIR, "options_*.parquet"))
    if not files:
        print("[WARN] No hay archivos en data/options/daily")
        return None
    latest_file = max(files)  # como el nombre lleva fecha, max() sirve
    df = pd.read_parquet(latest_file)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    print(f"[INFO] Usando snapshot de opciones: {os.path.basename(latest_file)}")
    return df

df_all = load_features()
df_raw_latest = load_latest_raw()   # se usa para Strike vs IV y top-10

TICKERS = sorted(df_all["ticker"].unique())
min_date = df_all["as_of_date"].min().date()
max_date = df_all["as_of_date"].max().date()

# =================== APP DASH ===================

app = Dash(__name__)
app.title = "Dashboard Opciones – Proyecto Tesis"

app.layout = html.Div([
    html.H2("Evolución de métricas de opciones por acción"),

    # ---- Controles ----
    html.Div([
        html.Div([
            html.Label("Ticker"),
            dcc.Dropdown(options=TICKERS, value=TICKERS[0],
                         id="dd-ticker", clearable=False),
        ], style={"width": "30%", "display": "inline-block", "verticalAlign": "top"}),

        html.Div([
            html.Label("Rango de fechas"),
            dcc.DatePickerRange(
                id="dp-range",
                min_date_allowed=min_date,
                max_date_allowed=max_date,
                start_date=min_date,
                end_date=max_date,
            )
        ], style={"width": "50%", "display": "inline-block", "paddingLeft": "20px"})
    ]),

    html.Br(),

    # ---- Series de tiempo del ticker seleccionado ----
    dcc.Graph(id="g-iv"),
    dcc.Graph(id="g-volume"),
    dcc.Graph(id="g-pcr"),

    # ---- Barra Put/Call Ratio por ticker (como tu imagen) ----
    html.H3("Put/Call Ratio por ticker (último día del rango)"),
    dcc.Graph(id="g-pcr-cross"),

    html.Hr(),

    # ---- Strike vs IV + tabla top 10 opciones para el ticker ----
    html.H3("Detalle del último snapshot de opciones (strike vs IV y top 10 por volumen)"),
    dcc.Graph(id="g-strike-iv"),

    dash_table.DataTable(
        id="tbl-top",
        columns=[
            {"name": "Symbol", "id": "contractSymbol"},
            {"name": "Tipo", "id": "type"},
            {"name": "Strike", "id": "strike", "type": "numeric", "format": {"specifier": ".2f"}},
            {"name": "Vencimiento", "id": "expiry"},
            {"name": "Último Precio", "id": "lastPrice", "type": "numeric", "format": {"specifier": ".2f"}},
            {"name": "IV", "id": "impliedVolatility", "type": "numeric", "format": {"specifier": ".2%"}},
            {"name": "Volumen", "id": "volume", "type": "numeric"},
            {"name": "Open Interest", "id": "openInterest", "type": "numeric"},
        ],
        data=[],  # se llena en el callback
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={"fontFamily": "Calibri", "fontSize": 13, "padding": "4px"},
        style_header={"fontWeight": "bold", "backgroundColor": "#f1f5f9"},
    ),
])

# =================== CALLBACK ===================

@app.callback(
    [
        Output("g-iv", "figure"),
        Output("g-volume", "figure"),
        Output("g-pcr", "figure"),
        Output("g-pcr-cross", "figure"),
        Output("g-strike-iv", "figure"),
        Output("tbl-top", "data"),
    ],
    [
        Input("dd-ticker", "value"),
        Input("dp-range", "start_date"),
        Input("dp-range", "end_date"),
    ],
)
def update_charts(ticker, start_date, end_date):
    # --- 1) Series de tiempo para el ticker seleccionado ---
    df = df_all[df_all["ticker"] == ticker].copy()
    df = df[(df["as_of_date"] >= pd.to_datetime(start_date)) &
            (df["as_of_date"] <= pd.to_datetime(end_date))]

    # IV_mean
    fig_iv = go.Figure()
    fig_iv.add_trace(go.Scatter(x=df["as_of_date"], y=df["IV_mean"],
                                mode="lines+markers", name="IV_mean"))
    fig_iv.update_layout(title=f"Volatilidad implícita promedio – {ticker}",
                         xaxis_title="Fecha", yaxis_title="IV_mean")

    # Volumen total
    fig_vol = go.Figure()
    fig_vol.add_trace(go.Bar(x=df["as_of_date"], y=df["Volume_total"],
                             name="Volume_total"))
    fig_vol.update_layout(title=f"Volumen total de opciones – {ticker}",
                          xaxis_title="Fecha", yaxis_title="Volumen")

    # Put/Call Ratio
    fig_pcr = go.Figure()
    fig_pcr.add_trace(go.Scatter(x=df["as_of_date"], y=df["PutCallRatio"],
                                 mode="lines+markers", name="Put/Call Ratio"))
    fig_pcr.update_layout(title=f"Put/Call Ratio – {ticker}",
                          xaxis_title="Fecha", yaxis_title="Put/Call Ratio")

    # --- 2) Barra de Put/Call Ratio por ticker (como tu gráfica) ---
    # Tomamos el último día del rango para cada ticker
    end_dt = pd.to_datetime(end_date)
    df_range = df_all[(df_all["as_of_date"] <= end_dt)]
    # para cada ticker, tomar el registro más reciente (fecha máxima <= end_dt)
    idx = df_range.groupby("ticker")["as_of_date"].idxmax()
    df_last_by_ticker = df_range.loc[idx].sort_values("ticker")

    fig_pcr_cross = go.Figure()
    fig_pcr_cross.add_trace(go.Bar(
        x=df_last_by_ticker["ticker"],
        y=df_last_by_ticker["PutCallRatio"],
    ))
    fig_pcr_cross.update_layout(
        title="Put/Call Ratio por Ticker (Volumen)",
        xaxis_title="Ticker",
        yaxis_title="Put/Call Ratio",
    )

    # --- 3) Strike vs IV + Top 10 por volumen (último snapshot crudo) ---
    if df_raw_latest is not None:
        df_raw_tk = df_raw_latest[df_raw_latest["ticker"] == ticker].copy()
        # Scatter strike vs IV
        fig_si = go.Figure()
        if not df_raw_tk.empty and "impliedVolatility" in df_raw_tk.columns:
            fig_si.add_trace(go.Scatter(
                x=df_raw_tk["strike"],
                y=df_raw_tk["impliedVolatility"],
                mode="markers",
                marker=dict(size=6),
                text=df_raw_tk["type"],
                name="Strike vs IV",
            ))
        fig_si.update_layout(
            title=f"Distribución Strike vs Impl. Volatility – {ticker} (último snapshot)",
            xaxis_title="Strike",
            yaxis_title="Implied Volatility",
        )

        # Top 10 opciones por volumen
        cols = ["contractSymbol", "type", "strike", "expiry",
                "lastPrice", "impliedVolatility", "volume", "openInterest"]
        for c in cols:
            if c not in df_raw_tk.columns:
                df_raw_tk[c] = None

        df_top = (df_raw_tk
                  .sort_values("volume", ascending=False)
                  .head(10)[cols])

        table_data = df_top.to_dict("records")
    else:
        # Si no hay snapshot crudo
        fig_si = go.Figure(layout=dict(
            title="Sin datos crudos de opciones disponibles",
            xaxis_title="Strike", yaxis_title="Implied Volatility"
        ))
        table_data = []

    return fig_iv, fig_vol, fig_pcr, fig_pcr_cross, fig_si, table_data


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)

# Se despliega en http://127.0.0.1:8050/
