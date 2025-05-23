import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.dependencies import Output, Input
import json

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX, "/assets/linkedin_style.css"])

LINKEDIN_BLUE = "#0A66C2"
TEXT_COLOR_DARK = "rgba(0,0,0,0.9)"
BACKGROUND_PAGE_COLOR = "#F3F2EF"
WINDOW_SEC = 60

app.layout = dbc.Container(fluid=True, style={'backgroundColor': BACKGROUND_PAGE_COLOR, 'padding': '20px'}, children=[
    dbc.Row([
        dbc.Col(html.H1("Análisis de Ofertas de Trabajo", className="app-title"), width=8),
        dbc.Col(html.Div(id="live-count", className="live-count-text"), width=4, className="text-right align-self-center")
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader(html.H4("Top 5 Industrias", className="card-title-custom")),
            dbc.CardBody([
                dcc.Graph(id="top-industries-bar", config={'displayModeBar': False})
            ])
        ]), md=6, className="mb-4"),
        dbc.Col(dbc.Card([
            dbc.CardHeader(html.H4("Top 5 Estados por Ofertas", className="card-title-custom")),
            dbc.CardBody([
                dcc.Graph(id="top-states-bar", config={'displayModeBar': False})
            ])
        ]), md=6, className="mb-4"),
    ]),
    dcc.Interval(id="interval", interval=1_000, n_intervals=0)
])

@app.callback(
    [Output("live-count", "children"),
     Output("top-industries-bar", "figure"),
     Output("top-states-bar", "figure")],
    Input("interval", "n_intervals")
)
def update_data(n):
    try:
        with open('data/current_count.json', 'r') as f:
            data = json.load(f)
    except Exception:
        empty_fig = {"data": [], "layout": {"xaxis": {"visible": False}, "yaxis": {"visible": False}, "annotations": [{"text": "Esperando datos...", "xref": "paper", "yref": "paper", "showarrow": False, "font": {"size": 16}}]}}
        return "Cargando...", empty_fig, empty_fig

    count_text_val = data.get('count', 0)
    
    count_html = [
        html.Span(f"{count_text_val}", style={'fontSize': '2em', 'fontWeight': 'bold', 'color': LINKEDIN_BLUE}),
        html.Span(f" ofertas activas (últimos {WINDOW_SEC}s)", style={'fontSize': '1em', 'marginLeft': '10px'})
    ]

    industries_data = data.get("top_industries", [])
    fig_industries = {
        "data": [{"x": [d["industry"] for d in industries_data], "y": [d["count"] for d in industries_data], "type": "bar", "marker": {"color": LINKEDIN_BLUE}}],
        "layout": {"margin": dict(l=40, r=20, t=20, b=120), "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)', "font": {"color": TEXT_COLOR_DARK}, "yaxis": {"title": "Número de ofertas", "gridcolor": "#e0e0e0"}, "xaxis": {"title": "Industria", "tickangle": -45}}
    }

    states_data = data.get("top_states", [])
    fig_states = {
        "data": [{"x": [d["state"] for d in states_data], "y": [d["count"] for d in states_data], "type": "bar", "marker": {"color": "#5cb85c"}}],
        "layout": {"margin": dict(l=40, r=20, t=20, b=40), "paper_bgcolor": 'rgba(0,0,0,0)', "plot_bgcolor": 'rgba(0,0,0,0)', "font": {"color": TEXT_COLOR_DARK}, "yaxis": {"title": "Número de ofertas", "gridcolor": "#e0e0e0"}, "xaxis": {"title": "Estado"}}
    }

    if not industries_data:
        fig_industries["layout"]["annotations"] = [{"text": "Sin datos de industria", "xref": "paper", "yref": "paper", "showarrow": False, "font": {"size": 16}}]
    if not states_data:
        fig_states["layout"]["annotations"] = [{"text": "Sin datos de estado", "xref": "paper", "yref": "paper", "showarrow": False, "font": {"size": 16}}]

    return count_html, fig_industries, fig_states

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)