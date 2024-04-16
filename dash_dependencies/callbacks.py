
from dash import Dash, html, dcc, Input, Output, callback, dash_table
import plotly.express as px

from pymongo import MongoClient
import polars as pl
from dw.init_mongodb import TerroristMongoDBDatabase





@callback(
    Output('geomap', 'figure'),
    Input('geomap-year-slider', 'value')
)
def update_geomap(year):

    db = TerroristMongoDBDatabase("data/terrorismdb_no_doubt.csv")
    df = db.get_num_events_all_countries()

    # df = px.data.gapminder().query(f"year=={year}")

    fig = px.choropleth(df, locations="iso_alpha",  
                        color="count", # lifeExp is a column of gapminder
                        hover_name="country", # column to add to hover information
                        color_continuous_scale=px.colors.sequential.Plasma) #range_color

    fig.update(layout_coloraxis_showscale=False)
      
    fig.update_layout(
            autosize=False,
            margin = dict(
                    l=0,
                    r=0,
                    b=0,
                    t=0,
                ),
                width=1910,
                height=1080,
        )
    
    fig.update_traces(customdata=df['country'])
    return fig

@callback(
    Output('hover-value', 'children'),
    Input('geomap', 'clickData'),
    prevent_initial_call=True)

def update_hover(clickData):

    df = px.data.gapminder()

    dff = df.loc[df['country'] == clickData['points'][0]['customdata']]

    return dash_table.DataTable(dff.to_dict('records'), [{"name": i, "id": i} for i in dff.columns])
