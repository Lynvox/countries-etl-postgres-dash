"""
Dash app: DB-backed table with all columns and selected-country flag block.
"""

from __future__ import annotations

import pandas as pd
from dash import Dash, Input, Output, dash_table, html
from sqlalchemy import create_engine, text

from db_config import sqlalchemy_url

_engine = create_engine(sqlalchemy_url(), future=True)


def load_dataframe() -> pd.DataFrame:
    with _engine.connect() as conn:
        return pd.read_sql(text("SELECT * FROM countries"), conn)


df0 = load_dataframe()
# Flag URL column used for preview (table still shows all DB columns).
flag_col = "flags_png" if "flags_png" in df0.columns else None

app: Dash = Dash(__name__)
app.title = "Countries"

app.layout = html.Div(
    [
        html.H2("Countries (from PostgreSQL)"),
        html.Div(
            [
                html.Div(
                    [
                        dash_table.DataTable(
                            id="table",
                            columns=[{"name": c, "id": c} for c in df0.columns],
                            data=df0.to_dict("records"),
                            page_size=15,
                            sort_action="native",
                            filter_action="native",
                            row_selectable="single",
                            selected_rows=[0] if len(df0) else [],
                            style_table={"overflowX": "auto"},
                            style_cell={"textAlign": "left", "minWidth": "80px"},
                        ),
                    ],
                    style={"flex": "1 1 auto", "minWidth": 0},
                ),
                html.Div(
                    [
                        html.H3("Flag (selected row)"),
                        html.Div(id="flag-block"),
                    ],
                    style={
                        "flex": "0 0 340px",
                        "marginLeft": "24px",
                        "alignSelf": "flex-start",
                        "position": "sticky",
                        "top": "16px",
                    },
                ),
            ],
            style={
                "display": "flex",
                "flexDirection": "row",
                "alignItems": "flex-start",
            },
        ),
    ],
    style={"padding": "16px", "fontFamily": "sans-serif"},
)


@app.callback(
    Output("flag-block", "children"),
    Input("table", "derived_virtual_data"),
    Input("table", "derived_virtual_selected_rows"),
)
def update_flag(rows, selected_virtual_rows):
    # derived_virtual_* respects sorting/filtering; selected_rows does not.
    if not rows:
        return html.Span("No data.")
    if selected_virtual_rows:
        idx = selected_virtual_rows[0]
    else:
        idx = 0
    if idx >= len(rows):
        return html.Span("Invalid selection.")
    row = rows[idx]
    url = row.get(flag_col) if flag_col else None
    name = row.get("name_common") or row.get("cca3") or ""
    if not url:
        return html.Span(f"No flag URL for: {name}")
    return html.Div(
        [
            html.P(name),
            html.Img(src=url, style={"maxWidth": "320px", "border": "1px solid #ccc"}),
        ]
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
