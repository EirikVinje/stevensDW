
from dash import Dash, html, dcc, Input, Output, callback, dash_table, ctx, State
import plotly.express as px

from pymongo import MongoClient
import polars as pl
import numpy as np
from dw.init_mongodb import TerroristMongoDBDatabase
from dw.init_sql import TerroristSQLDatabase
from dw.read_neo4j import TerroristNeo4JDatabase
import dash_bootstrap_components as dbc


Mongodb = TerroristMongoDBDatabase("data/terrorismdb_no_doubt.csv")
SQLdb = TerroristSQLDatabase("data/terrorismdb_no_doubt.csv")
Noedb = TerroristNeo4JDatabase()


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
        df = Mongodb.get_num_events_all_countries()

    elif DB=='NoSQL':
        df = SQLdb.get_num_events_all_countries()

    elif DB=='Neo4J':
        df = Noedb.get_num_events_all_countries()

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
        df = Mongodb.get_events_by_country(clickCountry)

    elif DB=='NoSQL':
        df = SQLdb.get_events_by_country(clickCountry)

    elif DB=='Neo4J':
        df = Noedb.get_events_by_country(clickCountry)

    else:
        assert False
    
    print(df)

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
        Output('tableDropdown', 'children'),

        Input('radioDB', 'value'),
        prevent_initial_call=True
)       

def get_dropdowns(DB):

    if DB=='MongoDB':
        df = Mongodb.get_events_with_criteria()

    elif DB=='NoSQL':
        df = SQLdb.get_events_with_criteria()

    elif DB=='Neo4J':
        df = Noedb.get_events_with_criteria()

    else:
        assert False

    children = [dbc.Col([dcc.Dropdown(options=list(df['country'].unique()), value='United States', id='dropdownCounty')], width=2),
                dbc.Col([dcc.Dropdown(placeholder='Start Year', id='dropdownSY')], width=2),
                dbc.Col([dcc.Dropdown(placeholder='End Year', id='dropdownEY')], width=2),
                dbc.Col([dcc.Dropdown(placeholder='Attack Type', id='dropdownAT')], width=2),
                dbc.Col([dcc.Dropdown(placeholder='Target Type',  id='dropdownTT')], width=2),
                dbc.Col([dcc.Dropdown(placeholder='Sucsess',  id='dropdownSucsess')], width=2)]
    
    return children, [dbc.Col(dcc.Dropdown(list(df.columns), list(df.columns)[0:5], multi=True, id='tableColumns'), width=6)]



@callback(
        Output('dropdownCounty', 'value'),
        Input('geomap', 'clickData'),
        prevent_initial_call=True
)       

def update_dropdown_from_geo(clickData):

    if clickData is None:
        clickCountry='United States'
    else:
        clickCountry = clickData['points'][0]['customdata']    

    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if trigger_id == "geomap":

        return clickCountry
    

@callback(
        Output('queryTable', 'children'),
        
        Output('dropdownSY', 'options'),
        Output('dropdownEY', 'options'),
        Output('dropdownAT', 'options'),
        Output('dropdownTT', 'options'),
        Output('dropdownSucsess', 'options'),

        Input('radioDB', 'value'),
        Input('dropdownCounty', 'value'),

        Input('dropdownSY', 'value'),
        Input('dropdownEY', 'value'),
        Input('dropdownAT', 'value'),
        Input('dropdownTT', 'value'),
        Input('dropdownSucsess', 'value'),
        Input('tableColumns', 'value'),
        
        prevent_initial_call=False

)

def update_dropdowns(DB, dropdownCounty, dropdownSY, dropdownEY, dropdownAT, dropdownTT, dropdownSucsess, tableColumns):

    if DB=='MongoDB':
        df = Mongodb.get_events_with_criteria(country=dropdownCounty, start_year=dropdownSY, end_year=dropdownEY, attack_type=dropdownAT, target_type=dropdownTT, success=dropdownSucsess)

    elif DB=='NoSQL':
        df = SQLdb.get_events_with_criteria(country=dropdownCounty, start_year=dropdownSY, end_year=dropdownEY, attack_type=dropdownAT, target_type=dropdownTT, success=dropdownSucsess)

    elif DB=='Neo4J':
        df = Noedb.get_events_with_criteria(country=dropdownCounty, start_year=dropdownSY, end_year=dropdownEY, attack_type=dropdownAT, target_type=dropdownTT, success=dropdownSucsess)

    else:
        assert False


    dff = df.to_pandas()
    dff = dff[tableColumns]

    years_range = df['year'].unique()
    at_range = df['attacktype'].unique()
    tt_range = df['targettype'].unique()
    sucsess_range = df['success'].unique()


    return dash_table.DataTable(dff.to_dict('records'), [{"name": i, "id": i} for i in dff.columns], fill_width=True), list(years_range), list(years_range), list(at_range), list(tt_range), list(sucsess_range)




