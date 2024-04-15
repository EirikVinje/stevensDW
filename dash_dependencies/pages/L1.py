import dash
from dash import html, dcc, Input, Output, callback
import dash_mantine_components as dmc
import dash_dependencies.callbacks

import plotly.express as px



                          


# Register the page to be home page
dash.register_page(__name__, path="/")


layout = html.Div([

                html.Div([
                    html.Header([
                        dcc.Link(html.Img(src='assets/media/logo.jpg'),  href='/'),

                    ], id='headerSpace'),
                    

                    html.Div(
                        dcc.Graph(id='geomap', config = {'scrollZoom':False}), 
                        
                            className='geomapSpace'),

                    html.Br(),

                    html.Div([
                        dcc.Dropdown(
                                    value=2007,
                                    options=px.data.gapminder()['year'].unique(),
                                    id='geomap-year-slider'),

                        html.Br(),

                        html.Div(id='hover-value')

                            ], className='inputSpace'),

                    
                        
                    ])
            
        
    

], className='Home')