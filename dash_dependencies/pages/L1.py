import dash
from dash import html, dcc, Input, Output, callback
import dash_mantine_components as dmc
import dash_dependencies.callbacks
import dash_bootstrap_components as dbc
import plotly.express as px
import dash_dependencies.callbacks as callbacks



                          


# Register the page to be home page
dash.register_page(__name__, path="/")

geomap, pie1, pie2 = callbacks.init_geomap()

layout = html.Div([

                html.Div([
                    html.Header([
                        dcc.Link(html.Img(src='assets/media/logo.jpg'),  href='/'),

                    ], id='headerSpace'),
                    

                    dbc.Row([

                            dbc.Col(dcc.Graph(figure=geomap, id='geomap', config = {'scrollZoom':False}), width=8, className='geomapSpace', style={'border':'1px solid black'}),
                            dbc.Col(html.Div([dcc.Graph(figure=pie1, id='geograph1'), dcc.Graph(figure=pie2, id='geograph2')]), width={"size": 4,"offset": 8}, className='geographSpace')    

                            ], className='geoSpace'),


                    html.Div([


                            dcc.RadioItems(['MongoDB', 'NoSQL','Neo4J'], inline=True, id='radioDB'),
                            html.Br(),
                            dbc.Row([

                            ], id='queryDropdowns')
                            


                            ], className='inputSpace'),

                    
                        
                    ])
            
        
    

], className='Home')