from dash import Dash, html, dcc, Input, Output, callback
import dash
# import dash_mantine_components as dmc
# import dash_bootstrap_components as dbc
import os
from flask import current_app
from flask import current_app
from flask_caching import Cache

assets_path = os.getcwd() + '/dash_dependencies'


app = Dash(__name__, use_pages=True, suppress_callback_exceptions=True, pages_folder=f"{assets_path}/pages", assets_folder=f'{assets_path}/pages')


app.layout = html.Div([dash.page_container,
                       dcc.Location(id="url", refresh=True),
                      ])




cache = Cache(app.server, config={
    'CACHE_TYPE':'filesystem',
    'CACHE_DIR':f'{assets_path}/cache-directory'
})

app.server.config['CACHE'] = cache

with app.server.app_context():
    cache = current_app.config['CACHE']


if __name__ == "__main__":
    app.run_server(debug=True, port=8050)

    