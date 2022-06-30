# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

# импорт библиотек и функций
from dash import Dash, dcc, Input, Output
import dash_bootstrap_components as dbc
from dash.dash_table.Format import Format, Scheme, Sign
import dash_table
from dash_table.Format import Format, Scheme
import dash_html_components as html
import pandas as pd
from datetime import date
import xlrd
xlrd.xlsx.ensure_elementtree_imported(False, None)
xlrd.xlsx.Element_has_iter = True

from graph_functions_dash import get_data, update_graph, get_available_groups
from graph_functions_dash import get_available_subgroups, get_available_products
from graph_functions_dash import data_bars_diverging

import warnings
warnings.filterwarnings("ignore")

# считывание и обработка необходимых даннных для анализа
data = pd.read_csv('data_dash.csv')
data2 = pd.read_csv('data2_dash.csv')
data = data.astype({'date':'datetime64[ns]'})
data2 = data2.astype({'date':'datetime64[ns]'})
data['category'] = data['category'].str.replace('[', '').str.replace(']', '')
data['group'] = data['group'].str.replace('[', '').str.replace(']', '')
data['subgroup'] = data['subgroup'].str.replace('[', '').str.replace(']', '')
data2['category'] = data2['category'].str.replace('[', '').str.replace(']', '')
data2['group'] = data2['group'].str.replace('[', '').str.replace(']', '')
data2['subgroup'] = data2['subgroup'].str.replace('[', '').str.replace(']', '')
nes_data = get_data("Запрашиваемый товар.xlsx", data) # данные только за 2021 год
nes_data2 = get_data("Запрашиваемый товар.xlsx", data2) # данные за 2 гоад
max_year = nes_data['date'].dt.year.max()
min_year = nes_data['date'].dt.year.min()
max_month = nes_data['date'].dt.month.max()
min_month = nes_data['date'].dt.month.min()
max_day = nes_data['date'].dt.day.max()
min_day = nes_data['date'].dt.day.min()

# доступные списки категорий, групп, подгрупп
available_categories = sorted(data["category"].unique())
available_group = sorted(data["group"].unique())
available_subgroup = sorted(data["subgroup"].unique())
available_products = sorted(data["product_code"].unique())

# FILTERS
## filter для даты
date_filter = dbc.FormGroup([  
                dbc.Label("Период", html_for="date-filter"),
                dcc.DatePickerRange(
                    id="date-filter",
                    start_date=date(min_year, min_month, min_day),
                    end_date=date(max_year, max_month, max_day),
                    display_format='D MMM YYYY',
                    style={'color':"#e5e5e5 !important"})])

## filter для горизонта прогнозирования
predskaz_filter = dbc.FormGroup([
                    dbc.Label("Горизонт прогнозирования", 
                                html_for="month_dropdown"),
                    dcc.Dropdown(
                        id="month_dropdown",
                        placeholder='0',
                        value=0,
                        options=[{'label': category, 
                                'value': category}  \
                                    for category in \
                                        [i for i in range(13)]])],
                        className='form-group col-md-6')

## filter для категории
category_filter = dbc.FormGroup([
                    dbc.Label("Категория товара", 
                            html_for="category_dropdown"),
                    dcc.Dropdown(
                        id="category_dropdown",
                        placeholder='Все категории',
                        value=available_categories[0],
                        options=[{'label': category, 
                                'value': category} \
                                    for category in \
                                        available_categories])],
                        className='form-group col-md-6')

## filter для группы
group_filter = dbc.FormGroup([
                    dbc.Label("Группа товара", html_for="group_dropdown"),
                    dcc.Dropdown(
                        id="group_dropdown",
                        placeholder='Все группы',
                        value=None,
                        options=[{'label': group, 
                                'value': group} \
                                    for group in \
                                        available_group])],
                        className='form-group col-md-6')

## filter для подгруппы
subgroup_filter = dbc.FormGroup([
                    dbc.Label("Подгруппа товара", 
                            html_for="subgroup_dropdown"),
                    dcc.Dropdown(
                        id="subgroup_dropdown",
                        placeholder='Все подгруппы',
                        value=None,
                        options=[{'label': subgroup, 
                                'value': subgroup} \
                                    for subgroup in \
                                        available_subgroup])],
                        className='form-group col-md-6')

## filter для кода товара
product_code_filter = dbc.FormGroup([
                        dbc.Label("Код товара", html_for="product_code_dropdown"),
                        dcc.Dropdown(
                            id="product_code_dropdown",
                            placeholder='Все товары',
                            value=None,
                            options=[{'label': product, 
                                    'value': product}  \
                                        for product in \
                                            available_products])],
                            className='form-group col-md-6')

# СARDS
## card для блока описательных статистик
statistics_card = dbc.Card([
                        dbc.CardBody([
                        html.Label("Описательные статистики ряда динамики оборота",
                                    style={'font-size':24,
                                        'text-align':'left'}),
                        dbc.Col([html.Label("Линейная диаграмма демонстрирует" 
                                            " изменение динамики величины"
                                            " оборота за анализируемый период времени." 
                                            " Диаграмма размаха отображает значения описательных"
                                            " статистик анализируемого ряда динамики величины оборота." 
                                            " Гистограмма отображает рассчитанное пороговое значение" 
                                            " коэффициента сезонности для выявления ярко" 
                                            " выраженной сезонности спроса.",
                                    style={'font-size':14,
                                        'text-align':'left',
                                        'color':'#808080',
                                        'font-family': 'sans-serif'})], 
                                    width={'size':8}),
                        html.Br(),
                        dbc.Row([
                                dbc.Col([
                                    dcc.Graph(id='turnover_graph')], 
                                    width={'size':4}),
                                dbc.Col([
                                    dcc.Graph(id='graph_boxplot')], 
                                    width={'size':4}),
                                dbc.Col([
                                    dcc.Graph(id='hist_seasonality')], 
                                    width={'size':4})])], 
                                    style={'height': '40rem'})])

## card для блока сезонных колебаний
seasonality_card = dbc.Card([
                        dbc.CardBody([
                            html.Label("Сезонные колебания",
                                        style={'font-size':24,
                                            'text-align':'left'}),
                            html.Br(),
                            dbc.Col([html.Label("Столбчатая диаграмма отображает процентные" 
                                            " отклонения величины оборота по месяцам относительно" 
                                            " среднемесячного значения. Красным цветом выделены" 
                                            " месяцы пониженного спроса, зеленым – повышенного спроса." 
                                            " В таблице представлены месячные значения величины оборота" 
                                            " и относительные значения отклонений величины оборота.",
                                        style={'font-size':14,
                                                'text-align':'left',
                                                'color':'#808080',
                                                'font-family': 'sans-serif'})], 
                                        width={'size':8}),
                            html.Br(),
                            dbc.Row([
                                dbc.Col([
                                dcc.Graph(id='seasonality_barchart')], 
                                        width={'size':8}),
                                dbc.Col([
                                    dash_table.DataTable(
                                        id='season_otkl',
                                        sort_action='native',
                                        columns=[{'name': i, 'id': i, 
                                                'type': 'numeric', 
                                                'format': Format(precision=2, 
                                                                scheme=Scheme.fixed,
                                                                sign=Sign.positive)}
                                                for i in ['Месяц', 'Оборот, шт', 'Отклонение, %']],
                                            style_cell={
                                                'width': '100px',
                                                'minWidth': '100px',
                                                'maxWidth': '100px',
                                                'overflow': 'hidden',
                                                'textOverflow': 'ellipsis',
                                                'text-align': 'center',
                                                'font-family': 'sans-serif',
                                                'font-size': '14px'},
                                            style_header={
                                                'backgroundColor': 'white',
                                                'fontWeight': 'bold',
                                                'text-align': 'center',
                                                'font-family': 'sans-serif',
                                                'font-size': '14px'},
                                            style_cell_conditional=[{'if': {'column_id': c},
                                                                    'textAlign': 'right'
                                                                    } for c in ['Оборот, шт', 
                                                                            'Отклонение, %']],
                                            page_action='none',
                                            style_table={'height': '26rem', 
                                                        'overflowY': 'auto'})], 
                            width={'size':4})])], 
                            style={'height': '40rem'})])

## card для блока прогнозирования
models_card = dbc.Card([
                dbc.CardBody([
                    html.Label("Прогноз величины оборота",
                                style={'font-size':24,
                                    'text-align':'left'}),
                    html.Br(),
                    html.Label("График демонстрирует фактическую и прогнозную величину оборота" 
                                " по месяцам 2020-2021. В таблице приведены значения метрик" 
                                " точности предсказаний построенной модели.",
                                style={'font-size':14,
                                    'text-align':'left',
                                    'color':'#808080',
                                    'font-family': 'sans-serif'}),
                    html.Br(),
                    predskaz_filter,
                    dbc.Col([
                        dcc.Graph(id='model_graph_predskaz'),
                        html.Br(),
                        html.Br(),
                        html.Br(),
                        dash_table.DataTable(
                                    id='model_table_predskaz',
                                    sort_action='native',
                                    columns=[{'name': i, 'id': i, 'type': 'numeric'}
                                            for i in ['MAPE, %', 'MAE, шт', 
                                                    'RMSE, шт', 'R²']],
                                    style_cell={
                                        'width': '70px',
                                        'minWidth': '50px',
                                        'maxWidth': '70px',
                                        'textOverflow': 'ellipsis',
                                        'text-align': 'right',
                                        'font-family': 'sans-serif',
                                        'font-size': '14px'},
                                    style_header={
                                        'backgroundColor': 'white',
                                        'fontWeight': 'bold',
                                        'text-align': 'center',
                                        'font-family': 'sans-serif',
                                        'font-size': '14px'},
                                    page_action='none',
                                    style_table={'height': '20rem', 
                                            'overflowY': 'auto'})], 
                    width={'size':12})], 
                    style={'height': '68rem'})])

## card для блока xyz-анализа
xyz_card = dbc.Card(
                [dbc.CardBody(
                    [html.Label("XYZ-анализ",
                                style={'font-size':24,
                                    'text-align':'left'}),
                    html.Br(),
                    html.Br(),
                    html.Label("На диаграмме приведено распределение групп товаров" 
                                " по секторам X, Y, Z. В таблице представлено" 
                                " распределение товаров по группам X, Y, Z.",
                                style={'font-size':14,
                                        'text-align':'left',
                                        'color':'#808080',
                                        'font-family': 'sans-serif'}),
                    html.Br(),
                    html.Br(),
                    html.Br(),
                    dbc.Col([
                        dcc.Graph(id='xyz_lin_graph'),
                        html.Br(),
                        html.Br(),
                        html.Br(),
                        dash_table.DataTable(
                            id='xyz_table',
                            sort_action='native',
                            columns=[{'name': i, 'id': i, 'type': 'numeric'}
                                    for i in ['Категория', 'Группа', 'Подгруппа', 'Код товара', 
                                                'Коэфф. вариации, %', 'XYZ сектор']],
                            style_cell={
                                'width': '70px',
                                'minWidth': '50px',
                                'maxWidth': '70px',
                                'textOverflow': 'ellipsis',
                                'text-align': 'center',
                                'font-family': 'sans-serif',
                                'font-size': '12px',
                            },
                            style_header={
                                'backgroundColor': 'white',
                                'fontWeight': 'bold',
                                'text-align': 'center',
                                'font-family': 'sans-serif',
                                'font-size': '12px',
                            },
                            page_action='none',
                            style_table={'height': '20rem', 'overflowY': 'auto'},
                            style_cell_conditional=[{'if': {'column_id': c},
                                                    'textAlign': 'right'
                                                    } for c in ['Код товара', 
                                                        'Коэфф. вариации, %']])
                            ], width={'size':12})
                        ], style={'height': '68rem'})])

## card для блока кластерного анализа
cluster_card = dbc.Card([
                    dbc.CardBody(
                        [html.Label("Кластерный анализ",
                                    style={'font-size':24,
                                        'text-align':'left'}),
                            html.Br(),
                            html.Label("График демонстрирует изменение динамики" 
                                        " средней величины оборота по выделенным кластерам." 
                                        " В таблице приведено распределение товаров" 
                                        " по выделенным кластерам.",
                                        style={'font-size':14,
                                                'text-align':'left',
                                                'color':'#808080',
                                                'font-family': 'sans-serif'}),
                            html.Br(),
                            html.Br(),
                            html.Br(),
                            dbc.Col([
                                    dcc.Graph(id='cluster_lin_graph'),
                                    html.Br(),
                                    html.Br(),
                                    html.Br(),
                                    dash_table.DataTable(
                                        id='cluster_table',
                                        sort_action='native',
                                        columns=[{'name': i, 'id': i, 'type': 'numeric'}
                                                for i in ['Категория', 'Группа', 'Подгруппа', 
                                                        'Код товара', 'Кластер']],
                                        style_cell={
                                            'width': '70px',
                                            'minWidth': '50px',
                                            'maxWidth': '70px',
                                            'overflow': 'hidden',
                                            'textOverflow': 'ellipsis',
                                            'text-align': 'center',
                                            'font-family': 'sans-serif',
                                            'font-size': '12px'},
                                        style_header={
                                            'backgroundColor': 'white',
                                            'fontWeight': 'bold',
                                            'text-align': 'center',
                                            'font-family': 'sans-serif',
                                            'font-size': '12px'},
                                        style_cell_conditional=[{'if': {'column_id': c},
                                                                'textAlign': 'right'
                                                                } for c in ['Код товара', 'Кластер']],
                                        page_action='none',
                                        style_table={'height': '20rem', 'overflowY': 'auto'})], 
                                        width={'size':12})], style={'height': '68rem'})])


# LAYOUT
app = Dash(__name__,
            external_stylesheets=[dbc.themes.FLATLY])

app.layout = html.Div([
            dbc.Row([
                    dbc.Col([html.Label('Динамика оборота товаров сети магазинов "Улыбка радуги"', 
                                        style={"text-align": "left",
                                            "font-size": 30}), 
                            html.Br(),
                            html.Label("Дашборд позволяет выявить наличие сезонных колебаний" 
                                        " в динамике спроса на товары, построить прогноз величины"
                                        " оборота товаров, проанализировать стабильность продаж," 
                                        " а также выделить группы товаров со схожим характером" 
                                        " изменения динамики спроса.", 
                                        style={"text-align": "left",
                                            'font-size': 16,
                                            "color": "#808080"}
                                        )], width=8, style={'margin-top': '8px'}), 
                    dbc.Col([date_filter], width={'size':2, 'offset':1})], 
                                            style={'margin-top': '8px',
                                                'margin-bottom': '16px',
                                                'margin-left': '8px'}),
            dbc.Row([html.Div(category_filter, 
                        style={'width':'400px'}), 
                    html.Div(group_filter, 
                        style={'width':'400px'}),
                    html.Div(subgroup_filter, 
                        style={'width':'400px'}),
                    html.Div(product_code_filter, 
                        style={'width':'400px'})]
                    ),
            dbc.Row([
                    dbc.Col([statistics_card]),
            ], style={'margin-bottom': '16px'}),                
            dbc.Row([
                    dbc.Col([seasonality_card]),
                    ], style={'margin-bottom': '16px'}),
            dbc.Row([dbc.Col([models_card], 
                    style={'margin-bottom': '16px'}, width={'size':4}),
                    dbc.Col([xyz_card],  
                    style={'margin-bottom': '16px'}, width={'size':4}),
                    dbc.Col([cluster_card],  
                    style={'margin-bottom': '16px'}, width={'size':4})])
                    ], 
            style={'margin-left': '16px',
                    'margin-right': '16px'})


# CALLBACKS
# callbacks for filters
@app.callback(
    Output('group_dropdown', 'options'),
    Input('category_dropdown', 'value'))
def update_groups_dropdown(category):
    return get_available_groups(category, data, "group")

@app.callback(
    Output('subgroup_dropdown', 'options'),
    Input('group_dropdown', 'value'))
def update_subgroups_dropdown(category):
    return get_available_subgroups(category, data, "subgroup")

@app.callback(
    Output('product_code_dropdown', 'options'),
    Input('subgroup_dropdown', 'value'))
def update_products_dropdown(category):
    return get_available_products(category, data, "product_code")


# callbacks for graphs
## обновление graph_turnover и graph_boxplot
@app.callback(
    [Output('turnover_graph', 'figure'),
    Output('graph_boxplot', 'figure'),
    Output('hist_seasonality', 'figure')],
    [Input('category_dropdown', 'value'),
    Input('group_dropdown', 'value'),
    Input('subgroup_dropdown', 'value'),
    Input('product_code_dropdown', 'value'),
    Input('date-filter', 'start_date'),
    Input('date-filter', 'end_date')])
def update_graph_turnover(category, group, subgroup, 
                        product_code, start_date, end_date):
    return update_graph(data, "lin_turn", start_date, end_date, 
                        category, group, subgroup, product_code)
    
## обновление hist_seasonality
# @app.callback(
#     Output('hist_seasonality', 'figure'),
#     [Input('category_dropdown', 'value'),
#     Input('group_dropdown', 'value'),
#     Input('subgroup_dropdown', 'value'),
#     Input('product_code_dropdown', 'value'),
#     Input('date-filter', 'start_date'),
#     Input('date-filter', 'end_date')])
# def update_hist_season(category, group, subgroup, product_code, 
#                         start_date, end_date):
#     return update_graph(data, "hist_seasonality", start_date, end_date, 
#                         category, group, subgroup, product_code)

## обновление season_plot
@app.callback(
    Output('seasonality_barchart', 'figure'),
    [Input('category_dropdown', 'value'),
    Input('group_dropdown', 'value'),
    Input('subgroup_dropdown', 'value'),
    Input('product_code_dropdown', 'value'),
    Input('date-filter', 'start_date'),
    Input('date-filter', 'end_date')])
def update_graph_seas_plot(category, group, subgroup, product_code, 
                            start_date, end_date):
    return update_graph(data, "seas_plot", start_date, end_date, 
                        category, group, subgroup, product_code)

## обновление season_table
@app.callback(
    [Output('season_otkl', 'data'),
    Output('season_otkl', 'style_data_conditional')],
    [Input('category_dropdown', 'value'),
    Input('group_dropdown', 'value'),
    Input('subgroup_dropdown', 'value'),
    Input('product_code_dropdown', 'value'),
    Input('date-filter', 'start_date'),
    Input('date-filter', 'end_date')])
def update_seas_table_graph(category, group, subgroup, product_code, 
                            start_date, end_date):
    res = update_graph(data, 'seas_tab', start_date, end_date, 
                        category, group, subgroup, product_code)
    filtered_data = res[0]
    hm = res[1]
    lm = res[2]
    return filtered_data.to_dict('records'), data_bars_diverging(
        filtered_data, 'Отклонение, %', hm, lm)

## обновление graph_predskaz
@app.callback(
    Output('model_graph_predskaz', 'figure'),
    [Input('category_dropdown', 'value'),
    Input('group_dropdown', 'value'),
    Input('subgroup_dropdown', 'value'),
    Input('product_code_dropdown', 'value'),
    Input('month_dropdown', 'value')])
def update_graph_predskaz(category, group, subgroup, product_code, vals=0):
    return update_graph(data2, "graph_predskaz", None, None, category, 
                        group, subgroup, product_code, vals)

## обновление table_predskaz
@app.callback(
    Output('model_table_predskaz', 'data'),
    [Input('category_dropdown', 'value'),
    Input('group_dropdown', 'value'),
    Input('subgroup_dropdown', 'value'),
    Input('product_code_dropdown', 'value')])
def update_table_predskaz(category, group, subgroup, product_code):
    filtered_data = update_graph(data2, "table_predskaz", None, None, 
                                category, group, subgroup, product_code)
    return filtered_data.to_dict('records')

## обновление xyz_graph
@app.callback(
    Output('xyz_lin_graph', 'figure'),
    Input('category_dropdown', 'value'))
def update_graph_xyz(category):
    return update_graph(data, "xyz_graph", None, 
                        None, category)

## update xyz_table
@app.callback(
    Output('xyz_table', 'data'),
    Input('category_dropdown', 'value'))
def update_xyz_table(category):
    filtered_data = update_graph(data, "xyz_table", 
                                None, None, category)
    return filtered_data.to_dict('records') 

## обновление cluster_graph
@app.callback(
    Output('cluster_lin_graph', 'figure'),
    Input('category_dropdown', 'value'))
def update_graph_cluster(category):
    return update_graph(data, "cluster_graph",  
                        None, None, category)

## обновление cluster_table
@app.callback(
    Output('cluster_table', 'data'),
    Input('category_dropdown', 'value'))
def update_cluster_table(category):
    filtered_data = update_graph(data, "cluster_table", 
                                None, None, category)
    return filtered_data.to_dict('records')


## запуск дашборда
if __name__ == '__main__':
    app.run_server(debug=True)

