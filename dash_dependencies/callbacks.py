
from dash import Dash, html, dcc, Input, Output, callback, dash_table
import plotly.express as px

from pymongo import MongoClient
import polars as pl
import numpy as np
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
                        color=np.log10(df["count"]), # lifeExp is a column of gapminder
                        hover_name="country", # column to add to hover information
                        color_continuous_scale=px.colors.sequential.Plasma) 

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
    Output('geograph1', 'figure'),   
    Output('geograph2', 'figure'),        

    Input('geomap', 'clickData'),
    prevent_initial_call=False)

def update_geograph(clickData):

    if clickData is None:
        clickCountry='United States'
    else:
        clickCountry = clickData['points'][0]['customdata']

    # df = px.data.gapminder()
    db = TerroristMongoDBDatabase("data/terrorismdb_no_doubt.csv")
    df = db.get_events_by_country(clickCountry)
    
    temp = df["year"].value_counts()

    fig1 = px.pie(temp, values='count', names='year', title=f'Terrorist attacks in {clickCountry} per year')
    fig1.update_traces(textposition='inside', textinfo='percent+label')
    fig1.update_layout(
            autosize=True,
            margin = dict(
                    l=0,
                    r=0,
                    b=0,
                    t=50,
                ),
                width=600,
                height=340,
                showlegend=False,
                uniformtext_minsize=10, uniformtext_mode='hide',
                legend=dict(font=dict(size=12)))
    
    fig2 = px.pie(df, values='nkill', names='year', title=f'People killed (%) in {clickCountry} per year')
    fig2.update_traces(textposition='inside', textinfo='value+label')
    fig2.update_layout(
            autosize=True,
            margin = dict(
                    l=0,
                    r=0,
                    b=0,
                    t=50,
                ),
                width=600,
                height=340,
                showlegend=False,
                uniformtext_minsize=10, uniformtext_mode='hide',
                legend=dict(font=dict(size=12)))

    # fig1 = px.line(group, x="year", y="nwound", title=f'Life expectancy in {clickCountry}', markers='*')
    # fig2 = px.line(group2, x="year", y="nkill", title=f'Population in {clickCountry}', markers='*')

    # fig1.update_layout(
    #         autosize=True,
    #         margin = dict(
    #                 l=0,
    #                 r=0,
    #                 b=0,
    #                 t=50,
    #             ),
    #             width=600,
    #             height=340)

    # fig2.update_layout(
    #         autosize=True,
    #         margin = dict(
    #                 l=0,
    #                 r=0,
    #                 b=0,
    #                 t=50,
    #             ),
    #             width=600,
    #             height=340)

    return fig1, fig2
