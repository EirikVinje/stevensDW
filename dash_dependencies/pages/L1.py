import dash
from dash import html, dcc, Input, Output, callback
import dash_mantine_components as dmc

# Register the page to be home page
dash.register_page(__name__, path="/")


layout = dmc.Container([

    dmc.Grid(
        children=[
            dmc.Col(
                html.Div([
                    html.Header([
                        dcc.Link(html.Img(src='assets/media/logo.jpg'),  href='/'),

                    ]),
                    html.Div([
                        html.H1('Yo')
                    ], className='logoSpace'),
                ])
            )
        ]
    )

], p=0, m=0, fluid=True, className='Home')