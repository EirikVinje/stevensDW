
from dash import Dash, html, dcc, Input, Output, callback, dash_table, ctx
import plotly.express as px

from pymongo import MongoClient
import polars as pl
import numpy as np
from dw.init_mongodb import TerroristMongoDBDatabase
import dash_bootstrap_components as dbc




def init_geomap():
    fig = px.choropleth() 

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
    
    fig1 = px.pie()
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
    
    fig2 = px.pie()
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

    
    return fig, fig1, fig2


@callback(
        Output('geomap', 'figure'),
        Input('radioDB', 'value'),
        prevent_initial_call=True
)       
def get_geomap(DB):

    if DB=='MongoDB':
        db = TerroristMongoDBDatabase("data/terrorismdb_no_doubt.csv")
        df = db.get_num_events_all_countries()

    else:
        assert False

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
    Input('radioDB', 'value'),

    prevent_initial_call=True)

def update_geograph(clickData, DB):

    if clickData is None:
        clickCountry='United States'
    else:
        clickCountry = clickData['points'][0]['customdata']


    if DB=='MongoDB':
        db = TerroristMongoDBDatabase("data/terrorismdb_no_doubt.csv")
        df = db.get_events_by_country(clickCountry)

    else:
        assert False
    
    # temp = df["year"].value_counts()

    fig1 = px.pie(df, values='num_events', names='year', title=f'Terrorist attacks in {clickCountry} per year')
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

    return fig1, fig2



@callback(
        Output('queryDropdowns', 'children'),
        Input('radioDB', 'value'),
        prevent_initial_call=True
)       

def get_dropdowns(DB):

        if DB=='MongoDB':
            db = TerroristMongoDBDatabase("data/terrorismdb_no_doubt.csv")
            df = db.get_num_events_all_countries()

            children = [dbc.Col([dcc.Dropdown(options=list(df['country']), value='United States', id='dropdownCounty')], width=2),
                        dbc.Col([dcc.Dropdown(placeholder='Start Year')], width=2, id='dropdownSY'),
                        dbc.Col([dcc.Dropdown(placeholder='End Year')], width=2, id='dropdownEY'),
                        dbc.Col([dcc.Dropdown(placeholder='Attack Type')], width=2, id='dropdownAT'),
                        dbc.Col([dcc.Dropdown(placeholder='Target Type')], width=2, id='dropdownTT'),
                        dbc.Col([dcc.Dropdown(placeholder='Sucsess')], width=2, id='dropdownSucsess')]
        
        return children



@callback(
        Output('dropdownCounty', 'value'),
        Input('geomap', 'clickData'),
        prevent_initial_call=True
)       

def update_dropdown(clickData):

    if clickData is None:
        clickCountry='United States'
    else:
        clickCountry = clickData['points'][0]['customdata']    

    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if trigger_id == "geomap":

        return clickCountry
     