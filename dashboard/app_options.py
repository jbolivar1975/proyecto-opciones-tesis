# dashboard/app_options.py

import os
import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

DATA_PATH = os.path.join("data", "options", "features", "options_features_daily.parquet")

def load_data():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"No existe {DATA_PATH}. Ejecuta primero build_options_features.py")
    df = pd.read_parquet(DATA_PATH)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df

df_all = load_data()
TICKERS = sorted(df_all["ticker"].unique())
min_date = df_all["as_of_date"].min().date()
max_date = df_all["as_of_date"].max().date()

app = Dash(__name__)
app.title = "Dashboard Opciones – Proyecto Tesis"

app.layout = html.Div([
    html.H2("Evolución de métricas de opciones por acción"),
    html.Div([
        html.Div([
            html.Label("Ticker"),
            dcc.Dropdown(options=TICKERS, value=TICKERS[0], id="dd-ticker", clearable=False),
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

    dcc.Graph(id="g-iv"),
    dcc.Graph(id="g-volume"),
    dcc.Graph(id="g-pcr"),
])

@app.callback(
    [Output("g-iv","figure"),
     Output("g-volume","figure"),
     Output("g-pcr","figure")],
    [Input("dd-ticker","value"),
     Input("dp-range","start_date"),
     Input("dp-range","end_date")]
)
def update_charts(ticker, start_date, end_date):
    df = df_all[df_all["ticker"] == ticker].copy()
    df = df[(df["as_of_date"] >= pd.to_datetime(start_date)) &
            (df["as_of_date"] <= pd.to_datetime(end_date))]

    # IV
    fig_iv = go.Figure()
    fig_iv.add_trace(go.Scatter(x=df["as_of_date"], y=df["IV_mean"], mode="lines+markers", name="IV_mean"))
    fig_iv.update_layout(title=f"Volatilidad implícita promedio – {ticker}", xaxis_title="Fecha", yaxis_title="IV_mean")

    # Volumen total
    fig_vol = go.Figure()
    fig_vol.add_trace(go.Bar(x=df["as_of_date"], y=df["Volume_total"], name="Volume_total"))
    fig_vol.update_layout(title=f"Volumen total de opciones – {ticker}", xaxis_title="Fecha", yaxis_title="Volumen")

    # Put/Call Ratio
    fig_pcr = go.Figure()
    fig_pcr.add_trace(go.Scatter(x=df["as_of_date"], y=df["PutCallRatio"], mode="lines+markers", name="Put/Call Ratio"))
    fig_pcr.update_layout(title=f"Put/Call Ratio – {ticker}", xaxis_title="Fecha", yaxis_title="PutCallRatio")

    return fig_iv, fig_vol, fig_pcr


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)

# Se despliega en http://127.0.0.1:8050/
