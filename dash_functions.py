import plotly.graph_objects as go
import pandas as pd
import numpy as np
import datetime as DT

from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from tqdm.autonotebook import tqdm
from tslearn.clustering import TimeSeriesKMeans
from datetime import datetime

import xlrd
xlrd.xlsx.ensure_elementtree_imported(False, None)
xlrd.xlsx.Element_has_iter = True

import warnings
warnings.filterwarnings("ignore")

# вспомогательная функция
def get_key(d, value):
    for k, v in d.items():
        if v == value:
            return k


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


# FUNCTION №1
# подготовка данных для анализа
def data_processing(data_csv_file, spravochnik_csv_file):
    # подготовка данных о продажах
    data = pd.read_csv(data_csv_file, sep=';')
    data = data.rename(columns={'Код товара':'product_code', ' Дата':'date', 
                            ' Оборот шт':'turnoverOfGoods', 
                            ' Оборот руб за вычетом скидки':'turnoverWithDisc', 
                            ' Валовый % скидки':'percentOfDisc'}) 
    data['percentOfDisc'] = data['percentOfDisc'].str.replace('%', '') \
                                                    .str.replace(',', '.')
    data['turnoverWithDisc'] = data['turnoverWithDisc'].str.replace(',', '.') \
                                                        .str.replace(' ', '')
    data['turnoverOfGoods'] = data['turnoverOfGoods'].str.replace(' ', '')
    data = data.astype({'percentOfDisc':'float',
                         'turnoverWithDisc':'float', 
                            'date':'datetime64[ns]'})
    for i in range(data.shape[0]):
        try:
            float(data.loc[i]['turnoverOfGoods'])
        except Exception:
            data = data.drop(i, axis=0)
    data = data.reset_index(drop=True)
    data = data.astype({'turnoverOfGoods':'float'})
    data.loc[(data.turnoverOfGoods < 0), 'turnoverOfGoods'] = 0
    data = data[['date', 'product_code', 'turnoverOfGoods', 
                        'percentOfDisc', 'turnoverWithDisc']]
    # подготовка справочника товаров
    spravka = pd.read_csv(spravochnik_csv_file, sep=';')
    spravka = spravka.rename(columns={' Категория':'category', 
                                        ' Группа':'group', 
                                        ' Подгруппа':'subgroup', 
                                        'Код товара':'product_code'})
    spravka = spravka[['product_code', 'category', 'group', 'subgroup']]
    main_data = data.merge(spravka, how='left', on='product_code')
    main_data['category'] = main_data['category'].str.replace('[', '').str.replace(']','')
    main_data['subgroup'] = main_data['subgroup'].str.replace('[', '').str.replace(']','')
    return main_data


# FUNCTION №2
# исключение влияния регулярных акций
def exclude_infl_15(data):
    # обработка данных величины оборота в дни 
    # проведения регулярной акции (15 числа каждого месяца)
    data['month'] = data['date'].dt.month
    isk_15 = data[data['date'].dt.day != 15] \
            .groupby(['product_code', 'month'], 
                    as_index=False) \
                .agg({'turnoverOfGoods':'mean'})
    prod_lst = list(isk_15['product_code'].unique())
    isk_15['turnoverOfGoods'] = round(isk_15['turnoverOfGoods'])
    data = data.query('product_code not in @add_lst') \
                .reset_index(drop=True)
    for i in prod_lst:
        for j in range(1, 13):
            if data[(data['product_code'] == i) \
                & (data['date'].dt.month == j) \
                & (data['date'].dt.day == 15)].shape[0] != 0:
                data.loc[((data.product_code == i) \
                    & (data.date.dt.day == 15) \
                    & (data.date.dt.month == j)), 
                    'turnoverOfGoods'] = float(isk_15[(isk_15['product_code'] == i) \
                        & (isk_15['month'] == j)]['turnoverOfGoods'])
    data = data.query('product_code not in @add_lst')
    return data


# FUNCTION №3
# извлечение необходимых данных в соответствии с запросом
def get_data(file_excel, data):
    # получение данных величины оборота 
    # по запрашиваемому товару/группе/подгруппе/категории
    workbook = xlrd.open_workbook(file_excel)
    worksheet = workbook.sheet_by_index(0)
    good = worksheet.cell_value(0, 0)
    # если запрашивается товар по коду
    if type(good) == float:
        good = int(good)
        data = data[data['product_code'] == good]
        data = data.reset_index(drop=True)
        if data.shape[0] == 0:
            return 'Группа товаров не найдена в базе!'
    else:
        # если запрашивается категория
        if data[data['category'] == good].shape[0] != 0:
            data = data[data['category'] == good]
            data = data.reset_index(drop=True)
        # если запрашивается группа
        elif data[data['group'] == good].shape[0] != 0:
            data = data[data['group'] == good]
            data = data.reset_index(drop=True)
        # если запрашивается подгруппа
        elif data[data['subgroup'] == good].shape[0] != 0:
            data = data[data['subgroup'] == good]
            data = data.reset_index(drop=True)
        # если данные по искомому товару не найдены в базе
        else:
            return 'Группа товаров не найдена в базе!'
    return data


# FUNCTION №4
## вывод линейной диаграммы
def get_statistics_lin_graph(data):
    month_lst = ['Январь', 'Февраль', 'Март', 
                'Апрель', 'Май', 'Июнь', 'Июль', 
                'Август', 'Сентябрь', 'Октябрь', 
                'Ноябрь', 'Декабрь']
    df_agg_m = data[["date", "turnoverOfGoods"]].resample("M", on="date") \
                                                .sum().reset_index()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=month_lst, y=df_agg_m['turnoverOfGoods'],
                    mode='lines+markers',
                    name='', marker_color='rgb(27, 158, 119)',
                    line_color='rgb(27, 158, 119)'))
    fig.update_layout( 
                yaxis_title_text='Оборот, шт', 
                showlegend = False, template="simple_white", 
                yaxis_tickformat = 'f.')
    return fig


# FUNCTION №5
## вывод диаграммы размаха
def get_statistics_boxplot(data):
    df_agg_m = data[["date", "turnoverOfGoods"]].resample("M", on="date").sum().reset_index()
    fig = go.Figure()
    fig.add_trace(go.Box(
                        y=df_agg_m['turnoverOfGoods'],
                        name="",
                        marker_color='rgb(27, 158, 119)',
                        boxpoints='all',
                        line_color='rgb(27, 158, 119)', hoverinfo='y'
                        ))
    fig.update_layout( 
                    xaxis_title_text='', 
                    yaxis_title_text='Оборот, шт', 
                    showlegend = False, template="simple_white", 
                    yaxis_tickformat = 'f.')
    return fig


# FUNCTION №6
# набор функций для определения порогового значения коэф. сезонности        
## для вывода значения
def find_seas_threshold_value(data, good='колготки', percent=75):
    # workbook = xlrd.open_workbook(file_excel)
    # worksheet = workbook.sheet_by_index(0)
    # good = worksheet.cell_value(0, 0)
    if type(good) == float:
        good = int(good)
        data2 = data[data['product_code'] == good]
        cat = data2['category'].unique()[0]
        data2 = data.reset_index(drop=True)
        if data2.shape[0] == 0:
            return 'Группа товаров не найдена в базе!'
    else:
        # если запрашивается категория
        if data[data['category'] == good].shape[0] != 0:
            data2 = data[data['category'] == good]
            data2 = data2.reset_index(drop=True)
            cat = data2['category'].unique()[0]
        # если запрашивается группа
        elif data[data['group'] == good].shape[0] != 0:
            data2 = data[data['group'] == good]
            data2 = data2.reset_index(drop=True)
            cat = data2['category'].unique()[0]
        # если запрашивается подгруппа
        elif data[data['subgroup'] == good].shape[0] != 0:
            data2 = data[data['subgroup'] == good]
            data2 = data2.reset_index(drop=True)
            cat = data2['category'].unique()[0]
        # если данные по искомому товару не найдены в базе
        else:
            return 'Группа товаров не найдена в базе!'
    # определяем категорию, для которой будем считать порог сезонности
    cat_df = data[data['category'] == cat]
    cat_df_gr = cat_df.groupby(['product_code', 'month'], as_index=False) \
        .agg({'turnoverOfGoods':'sum'})
    cat_df_gr['mean'] = 0
    prod_lst = list(cat_df_gr['product_code'].unique())
    # определяем для каждого товара для каждого месяца коэффициент сезонности
    for i in prod_lst:
        mean_turn = cat_df_gr[cat_df_gr['product_code'] == i]['turnoverOfGoods'].mean()
        cat_df_gr.loc[(cat_df_gr.product_code == i), 'mean'] = mean_turn
    cat_df_gr['seasonality'] = cat_df_gr['turnoverOfGoods'] / cat_df_gr['mean']
    # строим распределение значений коэффициентов сезонности
    perc_seas = round(np.percentile(cat_df_gr['seasonality'], percent), 2)
    return perc_seas


# FUNCTION №7
## для отображения графика
def find_seas_threshold_graph(data, good='колготки', percent=75):
    if type(good) == float:
        good = int(good)
        data2 = data[data['product_code'] == good]
        cat = data2['category'].unique()[0]
        data2 = data.reset_index(drop=True)
        if data2.shape[0] == 0:
            return 'Группа товаров не найдена в базе!'
    else:
        # если запрашивается категория
        if data[data['category'] == good].shape[0] != 0:
            data2 = data[data['category'] == good]
            data2 = data2.reset_index(drop=True)
            cat = data2['category'].unique()[0]
        # если запрашивается группа
        elif data[data['group'] == good].shape[0] != 0:
            data2 = data[data['group'] == good]
            data2 = data2.reset_index(drop=True)
            cat = data2['category'].unique()[0]
        # если запрашивается подгруппа
        elif data[data['subgroup'] == good].shape[0] != 0:
            data2 = data[data['subgroup'] == good]
            data2 = data2.reset_index(drop=True)
            cat = data2['category'].unique()[0]
        # если данные по искомому товару не найдены в базе
        else:
            return 'Группа товаров не найдена в базе!'
    # определяем категорию, для которой будем считать порог сезонности
    cat_df = data[data['category'] == cat]
    cat_df['month'] = cat_df['date'].dt.month
    cat_df_gr = cat_df.groupby(['product_code', 'month'], as_index=False) \
        .agg({'turnoverOfGoods':'sum'})
    cat_df_gr['mean'] = 0
    prod_lst = list(cat_df_gr['product_code'].unique())
    # определяем для каждого товара для каждого месяца коэффициент сезонности
    for i in prod_lst:
        mean_turn = cat_df_gr[cat_df_gr['product_code'] == i]['turnoverOfGoods'].mean()
        cat_df_gr.loc[(cat_df_gr.product_code == i), 'mean'] = mean_turn
    cat_df_gr['seasonality'] = cat_df_gr['turnoverOfGoods'] / cat_df_gr['mean']
    # строим распределение значений коэффициентов сезонности
    perc_seas = round(np.percentile(cat_df_gr['seasonality'], percent), 2)
    fig = go.Figure()
    fig.add_trace(go.Histogram(
                                x=cat_df_gr[cat_df_gr['seasonality'] < 5]['seasonality'],
                                histnorm='percent',
                                name='control', 
                                marker_color='rgb(27, 158, 119)',
                                opacity=0.5, xbins = dict(size = 0.1), 
                                ))
    fig.add_vline(x=perc_seas, line_width=2.5, line_dash="dash", line_color="black", 
                    annotation_text=f"Пороговое значение={str(perc_seas).replace('.', ',')}", 
                    annotation_position="top",
                    annotation_font_color="black",
                    annotation_font_size=14)
    fig.update_layout(yaxis_title_text='Частота, %', 
                        bargap=0, 
                        bargroupgap=0, showlegend = False, 
                        template="simple_white")

    return fig


# набор функций для выявления и визуализации сезонных колебаний

# FUNCTION №8
## вывод графического результата
def find_season_graph(data, threshold):
    month_lst = ['Январь', 'Февраль', 'Март', 
                'Апрель', 'Май', 'Июнь', 'Июль', 
                'Август', 'Сентябрь', 'Октябрь', 
                'Ноябрь', 'Декабрь']
    data_agg_m = data[['date', 'turnoverOfGoods']] \
                    .resample('M', on='date') \
                        .sum().reset_index()
    data_agg_m['date'] = month_lst
    data_agg_m['mean'] = data_agg_m['turnoverOfGoods'].mean()
    data_agg_m['seasonality'] = data_agg_m['turnoverOfGoods'] / data_agg_m['mean']
    df = data_agg_m
    df['otklonenie'] = df['seasonality'] - 1
    text_lst = []
    y_lst = []
    color_lst = []
    threshold = (threshold - 1) * 100
    for i in df['otklonenie'].to_list():
        y_lst.append(round(i, 4) * 100)
        if abs(i) * 100 > threshold:
            if i < 0:
                text_lst.append(format(i * 100, '.2f'))
                color_lst.append('crimson')
            else:
                text_lst.append("+" + (format(i * 100, '.2f')))
                color_lst.append('green')
        elif i < 0:
            text_lst.append(format(i * 100, '.2f'))
            color_lst.append('lightgray')
        else:
            text_lst.append("+" + (format(i * 100, '.2f')))
            color_lst.append('lightgray')
    
    fig = go.Figure(go.Bar(x=month_lst, y=y_lst,base=0,
                            marker_color=color_lst, text=text_lst, 
                            textposition='outside'),
                            layout_yaxis_range=[min(y_lst) - 0.2 * np.abs(min(y_lst)), max(y_lst) + 0.2 * np.abs(max(y_lst))])
    fig.update_layout(showlegend = False, template="simple_white", yaxis_title="Отклонение оборота в %",)
    fig.add_hline(y=threshold, line_width=2, line_dash="dash", line_color="black", 
                    annotation_text=f"Пороговое значение={int(threshold)}%", 
                    annotation_position="bottom left")
    fig.add_hline(y=-threshold, line_width=2, line_dash="dash", line_color="black", 
                    annotation_text=f"Пороговое значение={int(threshold)}%", 
                    annotation_position="bottom left")
    fig.add_hline(y=0, line_width=1, line_color="black")
    return fig


# FUNCTION №9
## вывод табличного результата
def find_season_table(data, threshold):
    month_lst = ['Январь', 'Февраль', 'Март', 
                'Апрель', 'Май', 'Июнь', 'Июль', 
                'Август', 'Сентябрь', 'Октябрь', 
                'Ноябрь', 'Декабрь']
    data_agg_m = data[['date', 'turnoverOfGoods']] \
                    .resample('M', on='date') \
                        .sum().reset_index()
    data_agg_m['date'] = month_lst
    data_agg_m['mean'] = data_agg_m['turnoverOfGoods'].mean()
    data_agg_m['seasonality'] = data_agg_m['turnoverOfGoods'] / data_agg_m['mean']
    seas_month_high = []
    seas_month_low = []
    for i in range(data_agg_m.shape[0]):
        if data_agg_m.loc[i]['seasonality'] > threshold:
            seas_month_high.append(i)
        if data_agg_m.loc[i]['seasonality'] < (1 - (threshold - 1)):
            seas_month_low.append(i)
    df = data_agg_m
    df['otklonenie'] = df['seasonality'] - 1
    df['turnoverOfGoods'] = pd.Series(["{0:.0f}".format(val) for val in df['turnoverOfGoods']], index = df.index)
    df = df.loc[:, :'seasonality'].rename({'date':'Месяц', 'turnoverOfGoods':'Оборот, шт', 'seasonality':'Отклонение, %'}, 
                            axis=1)[['Месяц', 'Оборот, шт', 'Отклонение, %']]
    df['Отклонение, %'] = df['Отклонение, %'] * 100 - 100
    color_lst = []
    for i in range(12):
        if i in seas_month_high:
            color_lst.append('green')
        elif i in seas_month_low:
            color_lst.append('red')
        else:
            color_lst.append('white')
    return df, seas_month_high, seas_month_low 



# набор функций для построения модели ряда
models_dct = {'linear add':0, 'linear mul':0, 'polynomial2 add':0, 'polynomial2 mul':0,   
                'polynomial3 add':0, 'polynomial3 mul':0, 'log add':0, 'log mul':0, 
                'exp add':0, 'exp mul':0, 'pow add':0, 'pow mul':0}

# FUNCTION №10
## перебор моделей, получение данных для таблицы
def find_model_predskaz(data, models_dct=models_dct):
    # подбор наилучшей модели ряда динамики оборота
    models_lst = list(models_dct.keys())
    r2_lst = []
    mape_lst = []
    mae_lst = []
    mse_lst = []
    rmse_lst = []
    tr_lst = []
    seas_lst = []
    data_agg_m = data[['date', 'turnoverOfGoods']].resample('M', on='date') \
                                                    .sum().reset_index()
    ls_time = [i for i in range(1, 25)]
    data_agg_m = pd.concat([data_agg_m[:12], data_agg_m[24:]])
    data_agg_m['date'] = ls_time
    data_agg_m = data_agg_m.reset_index(drop=True)

    x = data_agg_m['date'].to_list()
    y = data_agg_m['turnoverOfGoods'].to_list()

    for i in models_lst:
        data_agg_m2 = data_agg_m.copy()
        if 'linear' in i:
            k = np.polyfit(x, y, 1)[0]
            b = np.polyfit(x, y, 1)[1]
            lst = []
            for j in x:
                lst.append(k * j + b)
            data_agg_m2['trend'] = lst
            if 'add' in i:
                data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] - data_agg_m2['trend']
                data_agg_m2['total_ind_seas'] = 0
                for j in range(12):
                    data_agg_m2.loc[(data_agg_m2.date == j+1), 
                                        'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                            'seasonality'].mean()
                    data_agg_m2.loc[(data_agg_m2.date == j+13), 
                                        'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                        'seasonality'].mean()
                data_agg_m2['total_ind'] = data_agg_m2['total_ind_seas'].sum()
                data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] - data_agg_m2['total_ind']/12
                data_agg_m2['model_data'] = data_agg_m2['trend'] + data_agg_m2['clear_ind_seas']
                data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
                models_dct['linear add'] = r2_score(data_agg_m2['turnoverOfGoods'], 
                                                    data_agg_m2['model_data'])
                r2_lst.append(round(r2_score(data_agg_m2['turnoverOfGoods'], 
                                                    data_agg_m2['model_data']), 3))
                mape_lst.append(round(np.mean(np.abs(
                    (data_agg_m2["turnoverOfGoods"] - data_agg_m2['model_data']
                    ) / data_agg_m2["turnoverOfGoods"])) * 100, 3))
                mae_lst.append(int(mean_absolute_error(data_agg_m2['turnoverOfGoods'], 
                                                        data_agg_m2['model_data'])))
                mse_lst.append(int(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                            data_agg_m2['model_data'])))
                rmse_lst.append(int(np.sqrt(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                                data_agg_m2['model_data']))))
                tr_lst.append('линейный')
                seas_lst.append('аддитивная')
            else:
                data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] / data_agg_m2['trend']
                data_agg_m2['total_ind_seas'] = 0
                for j in range(12):
                    data_agg_m2.loc[(data_agg_m2.date == j+1), 
                                    'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                        'seasonality'].mean()
                    data_agg_m2.loc[(data_agg_m2.date == j+13), 
                                    'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                        'seasonality'].mean()
                data_agg_m2['total_ind'] = data_agg_m2.loc[:11, 'total_ind_seas'].sum()
                data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] / (
                                                    data_agg_m2['total_ind']/12)
                data_agg_m2['model_data'] = data_agg_m2['trend'] * data_agg_m2['clear_ind_seas']
                data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
                models_dct['linear mul'] = r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])
                r2_lst.append(round(r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data']), 3))
                mape_lst.append(round(np.mean(np.abs(
                                    (data_agg_m2["turnoverOfGoods"] - data_agg_m2['model_data']
                                        ) / data_agg_m2["turnoverOfGoods"])) * 100, 3))
                mae_lst.append(int(mean_absolute_error(data_agg_m2['turnoverOfGoods'], 
                                                        data_agg_m2['model_data'])))
                mse_lst.append(int(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                        data_agg_m2['model_data'])))
                rmse_lst.append(int(np.sqrt(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                                data_agg_m2['model_data']))))
                tr_lst.append('линейный')
                seas_lst.append('мультипликативная')
        elif 'polynomial2' in i:
            a = np.polyfit(x, y, 2)[0]
            b = np.polyfit(x, y, 2)[1]
            c = np.polyfit(x, y, 2)[2]
            if 'add' in i:
                lst = []
                for j in x:
                    lst.append(a * j**2 + b * j + c)
                data_agg_m2['trend'] = lst
                data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] - data_agg_m2['trend']
                data_agg_m2['total_ind_seas'] = 0
                for j in range(12):
                    data_agg_m2.loc[(data_agg_m2.date == j+1), 
                                    'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                    'seasonality'].mean()
                    data_agg_m2.loc[(data_agg_m2.date == j+13), 
                                        'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                        'seasonality'].mean()
                data_agg_m2['total_ind'] = data_agg_m2['total_ind_seas'].sum()
                data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] - data_agg_m2['total_ind']/12
                data_agg_m2['model_data'] = data_agg_m2['trend'] + data_agg_m2['clear_ind_seas']
                data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
                models_dct['polynomial2 add'] = r2_score(data_agg_m2['turnoverOfGoods'], 
                                                            data_agg_m2['model_data'])
                r2_lst.append(round(r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data']), 3))
                mape_lst.append(round(np.mean(np.abs((data_agg_m2["turnoverOfGoods"] - data_agg_m2['model_data']
                                                        ) / data_agg_m2["turnoverOfGoods"])) * 100, 3))
                mae_lst.append(int(mean_absolute_error(data_agg_m2['turnoverOfGoods'], 
                                                        data_agg_m2['model_data'])))
                mse_lst.append(int(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                        data_agg_m2['model_data'])))
                rmse_lst.append(int(np.sqrt(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                                data_agg_m2['model_data']))))
                tr_lst.append('полином 2й степени')
                seas_lst.append('аддитивная')
            else:
                lst = []
                for j in x:
                    lst.append(a * j**2 + b * j + c)
                data_agg_m2['trend'] = lst
                data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] / data_agg_m2['trend']
                data_agg_m2['total_ind_seas'] = 0
                for j in range(12):
                    data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                    data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                        'seasonality'].mean()
                data_agg_m2['total_ind'] = data_agg_m2.loc[:11, 'total_ind_seas'].sum()
                data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] / (data_agg_m2['total_ind']/12)
                data_agg_m2['model_data'] = data_agg_m2['trend'] * data_agg_m2['clear_ind_seas']
                data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
                models_dct['polynomial2 mul'] = r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])
                r2_lst.append(round(r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data']), 3))
                mape_lst.append(round(np.mean(np.abs((data_agg_m2["turnoverOfGoods"] - data_agg_m2['model_data']
                                                        ) / data_agg_m2["turnoverOfGoods"])) * 100, 3))
                mae_lst.append(int(mean_absolute_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                mse_lst.append(int(mean_squared_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                rmse_lst.append(int(np.sqrt(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                                data_agg_m2['model_data']))))
                tr_lst.append('полином 2й степени')
                seas_lst.append('мультипликативная')
        elif 'polynomial3' in i:
            a = np.polyfit(x, y, 3)[0]
            b = np.polyfit(x, y, 3)[1]
            c = np.polyfit(x, y, 3)[2]
            d = np.polyfit(x, y, 3)[3]
            if 'add' in i:
                lst = []
                for j in x:
                    lst.append(a * j**3 + b * j**2 + c * j + d)
                data_agg_m2['trend'] = lst
                data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] - data_agg_m2['trend']
                data_agg_m2['total_ind_seas'] = 0
                for j in range(12):
                    data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                            'seasonality'].mean()
                    data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                        'seasonality'].mean()
                data_agg_m2['total_ind'] = data_agg_m2['total_ind_seas'].sum()
                data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] - data_agg_m2['total_ind']/12
                data_agg_m2['model_data'] = data_agg_m2['trend'] + data_agg_m2['clear_ind_seas']
                data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
                models_dct['polynomial3 add'] = r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])
                r2_lst.append(round(r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data']), 3))
                mape_lst.append(round(np.mean(np.abs((data_agg_m2["turnoverOfGoods"] - data_agg_m2['model_data']
                                                        ) / data_agg_m2["turnoverOfGoods"])) * 100, 3))
                mae_lst.append(int(mean_absolute_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                mse_lst.append(int(mean_squared_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                rmse_lst.append(int(np.sqrt(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                                data_agg_m2['model_data']))))
                tr_lst.append('полином 3й степени')
                seas_lst.append('аддитивная')
            else:
                lst = []
                for j in x:
                    lst.append(a * j**3 + b * j**2 + c * j + d)
                data_agg_m2['trend'] = lst
                data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] / data_agg_m2['trend']
                data_agg_m2['total_ind_seas'] = 0
                for j in range(12):
                    data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                    data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                data_agg_m2['total_ind'] = data_agg_m2.loc[:11, 'total_ind_seas'].sum()
                data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] / (data_agg_m2['total_ind']/12)
                data_agg_m2['model_data'] = data_agg_m2['trend'] * data_agg_m2['clear_ind_seas']
                data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
                models_dct['polynomial3 mul'] = r2_score(data_agg_m2['turnoverOfGoods'], 
                                                            data_agg_m2['model_data'])
                r2_lst.append(round(r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data']), 3))
                mape_lst.append(round(np.mean(np.abs((data_agg_m2["turnoverOfGoods"] - data_agg_m2['model_data']
                                                        ) / data_agg_m2["turnoverOfGoods"])) * 100, 3))
                mae_lst.append(int(mean_absolute_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                mse_lst.append(int(mean_squared_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                rmse_lst.append(int(np.sqrt(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                                data_agg_m2['model_data']))))
                tr_lst.append('полином 3й степени')
                seas_lst.append('мультипликативная')
        elif 'log' in i:
            a = np.polyfit(np.log(x), y, 1)[0]
            b = np.polyfit(np.log(x), y, 1)[1]
            if 'add' in i:
                lst = []
                for j in x:
                    lst.append(a * np.log(j) + b)
                data_agg_m2['trend'] = lst
                data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] - data_agg_m2['trend']
                data_agg_m2['total_ind_seas'] = 0
                for j in range(12):
                    data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                    data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                data_agg_m2['total_ind'] = data_agg_m2['total_ind_seas'].sum()
                data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] - data_agg_m2['total_ind']/12
                data_agg_m2['model_data'] = data_agg_m2['trend'] + data_agg_m2['clear_ind_seas']
                data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
                models_dct['log add'] = r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])
                r2_lst.append(round(r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data']), 3))
                mape_lst.append(round(np.mean(np.abs((data_agg_m2["turnoverOfGoods"] - data_agg_m2['model_data']
                                                        ) / data_agg_m2["turnoverOfGoods"])) * 100, 3))
                mae_lst.append(int(mean_absolute_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                mse_lst.append(int(mean_squared_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                rmse_lst.append(int(np.sqrt(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                                data_agg_m2['model_data']))))
                tr_lst.append('логарифмический')
                seas_lst.append('аддитивная')
            else:
                lst = []
                for j in x:
                    lst.append(a * np.log(j) + b)
                data_agg_m2['trend'] = lst
                data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] / data_agg_m2['trend']
                data_agg_m2['total_ind_seas'] = 0
                for j in range(12):
                    data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                    data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                data_agg_m2['total_ind'] = data_agg_m2.loc[:11, 'total_ind_seas'].sum()
                data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] / (data_agg_m2['total_ind']/12)
                data_agg_m2['model_data'] = data_agg_m2['trend'] * data_agg_m2['clear_ind_seas']
                data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
                models_dct['log mul'] = r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])
                r2_lst.append(round(r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data']), 3))
                mape_lst.append(round(np.mean(np.abs((data_agg_m2["turnoverOfGoods"] - data_agg_m2['model_data']
                                                        ) / data_agg_m2["turnoverOfGoods"])) * 100, 3))
                mae_lst.append(int(mean_absolute_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                mse_lst.append(int(mean_squared_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                rmse_lst.append(int(np.sqrt(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                                data_agg_m2['model_data']))))
                tr_lst.append('логарифмический')
                seas_lst.append('мультипликативная')
        elif 'exp' in i:
            k = np.polyfit(x, np.log(y), 1)[0]
            ln_b = np.polyfit(x, np.log(y), 1)[1]
            b = np.exp(ln_b)
            if 'add' in i:
                lst = []
                for j in x:
                    lst.append(b * np.exp(k * j))
                data_agg_m2['trend'] = lst
                data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] - data_agg_m2['trend']
                data_agg_m2['total_ind_seas'] = 0
                for j in range(12):
                    data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                        'seasonality'].mean()
                    data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                        'seasonality'].mean()
                data_agg_m2['total_ind'] = data_agg_m2['total_ind_seas'].sum()
                data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] - data_agg_m2['total_ind']/12
                data_agg_m2['model_data'] = data_agg_m2['trend'] + data_agg_m2['clear_ind_seas']
                data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
                models_dct['exp add'] = r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])
                r2_lst.append(round(r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data']), 3))
                mape_lst.append(round(np.mean(np.abs((data_agg_m2["turnoverOfGoods"] - data_agg_m2['model_data']
                                                        ) / data_agg_m2["turnoverOfGoods"])) * 100, 3))
                mae_lst.append(int(mean_absolute_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                mse_lst.append(int(mean_squared_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                rmse_lst.append(int(np.sqrt(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                                data_agg_m2['model_data']))))
                tr_lst.append('экспоненциальный')
                seas_lst.append('аддитивная')
            else:
                lst = []
                for j in x:
                    lst.append(b * np.exp(k * j))
                data_agg_m2['trend'] = lst
                data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] / data_agg_m2['trend']
                data_agg_m2['total_ind_seas'] = 0
                for j in range(12):
                    data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                    data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                data_agg_m2['total_ind'] = data_agg_m2.loc[:11, 'total_ind_seas'].sum()
                data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] / (data_agg_m2['total_ind']/12)
                data_agg_m2['model_data'] = data_agg_m2['trend'] * data_agg_m2['clear_ind_seas']
                data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
                models_dct['exp mul'] = r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])
                r2_lst.append(round(r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data']), 3))
                mape_lst.append(round(np.mean(np.abs((data_agg_m2["turnoverOfGoods"] - data_agg_m2['model_data']
                                                                ) / data_agg_m2["turnoverOfGoods"])) * 100, 3))
                mae_lst.append(int(mean_absolute_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                mse_lst.append(int(mean_squared_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                rmse_lst.append(int(np.sqrt(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                                data_agg_m2['model_data']))))
                tr_lst.append('экспоненциальный')
                seas_lst.append('мультипликативная')
        else:
            k = np.polyfit(np.log(x), np.log(y), 1)[0]
            ln_b = np.polyfit(np.log(x), np.log(y), 1)[1]
            b = np.exp(ln_b)
            if 'add' in i:
                lst = []
                for j in x:
                    lst.append(b * (j ** k))
                data_agg_m2['trend'] = lst
                data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] - data_agg_m2['trend']
                data_agg_m2['total_ind_seas'] = 0
                for j in range(12):
                    data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                        'seasonality'].mean()
                    data_agg_m2.loc[(data_agg_m.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                        'seasonality'].mean()
                data_agg_m2['total_ind'] = data_agg_m2['total_ind_seas'].sum()
                data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] - data_agg_m2['total_ind']/12
                data_agg_m2['model_data'] = data_agg_m2['trend'] + data_agg_m2['clear_ind_seas']
                data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
                models_dct['pow add'] = r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])
                r2_lst.append(round(r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data']), 3))
                mape_lst.append(round(np.mean(np.abs((data_agg_m2["turnoverOfGoods"] - data_agg_m2['model_data']
                                                        ) / data_agg_m2["turnoverOfGoods"])) * 100, 3))
                mae_lst.append(int(mean_absolute_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                mse_lst.append(int(mean_squared_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                rmse_lst.append(int(np.sqrt(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                                data_agg_m2['model_data']))))
                tr_lst.append('степенной')
                seas_lst.append('аддитивная')
            else:
                lst = []
                for j in x:
                    lst.append(b * (j ** k))
                data_agg_m2['trend'] = lst
                data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] / data_agg_m2['trend']
                data_agg_m2['total_ind_seas'] = 0
                for j in range(12):
                    data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                        'seasonality'].mean()
                    data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                        'seasonality'].mean()
                data_agg_m2['total_ind'] = data_agg_m2.loc[:11, 'total_ind_seas'].sum()
                data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] / (data_agg_m2['total_ind']/12)
                data_agg_m2['model_data'] = data_agg_m2['trend'] * data_agg_m2['clear_ind_seas']
                data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
                models_dct['pow mul'] = r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])
                r2_lst.append(round(r2_score(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data']), 3))
                mape_lst.append(round(np.mean(np.abs((data_agg_m2["turnoverOfGoods"] - data_agg_m2['model_data']
                                                        ) / data_agg_m2["turnoverOfGoods"])) * 100, 3))
                mae_lst.append(int(mean_absolute_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                mse_lst.append(int(mean_squared_error(data_agg_m2['turnoverOfGoods'], data_agg_m2['model_data'])))
                rmse_lst.append(int(np.sqrt(mean_squared_error(data_agg_m2['turnoverOfGoods'], 
                                                                data_agg_m2['model_data']))))
                tr_lst.append('степенной')
                seas_lst.append('мультипликативная')
    k = get_key(models_dct, max(models_dct.values()))
    dct = {k:0}    
    accuracy_dct = {'Тренд':tr_lst, 'Сезонность':seas_lst, 
                        'MAPE':mape_lst, 'MAE':mae_lst, 
                        'RMSE':rmse_lst, 'R^2':r2_lst}
    df_metrics = pd.DataFrame(data=accuracy_dct)
    return dct, df_metrics


# FUNCTION №11
## формирование данных для графика
def find_model_predskaz_data(data, models_dct=find_model_predskaz(nes_data2)[0], n=0):
    models_lst = list(models_dct.keys())
    data_agg_m = data[['date', 'turnoverOfGoods']].resample('M', on='date') \
                                                    .sum().reset_index()
    ls_time = [i for i in range(1, 25)]
    data_agg_m = pd.concat([data_agg_m[:12], data_agg_m[24:]])
    data_agg_m['date'] = ls_time
    data_agg_m = data_agg_m.reset_index(drop=True)
    x = data_agg_m['date'].to_list()
    y = data_agg_m['turnoverOfGoods'].to_list()
    data_agg_m2 = data_agg_m.copy()
    if 'linear' in models_lst[0]:
        k = np.polyfit(x, y, 1)[0]
        b = np.polyfit(x, y, 1)[1]
        lst = []
        for j in x:
            lst.append(k * j + b)
        data_agg_m2['trend'] = lst
        if 'add' in models_lst[0]:
            data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] - data_agg_m2['trend']
            data_agg_m2['total_ind_seas'] = 0
            for j in range(12):
                data_agg_m2.loc[(data_agg_m2.date == j+1), 
                                        'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                            'seasonality'].mean()
                data_agg_m2.loc[(data_agg_m2.date == j+13), 
                                        'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                        'seasonality'].mean()
            data_agg_m2['total_ind'] = data_agg_m2['total_ind_seas'].sum()
            data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] - data_agg_m2['total_ind']/12
            data_agg_m2['model_data'] = data_agg_m2['trend'] + data_agg_m2['clear_ind_seas']
            data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
        else:
            data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] / data_agg_m2['trend']
            data_agg_m2['total_ind_seas'] = 0
            for j in range(12):
                data_agg_m2.loc[(data_agg_m2.date == j+1), 
                                    'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                        'seasonality'].mean()
                data_agg_m2.loc[(data_agg_m2.date == j+13), 
                                    'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                        'seasonality'].mean()
            data_agg_m2['total_ind'] = data_agg_m2.loc[:11, 'total_ind_seas'].sum()
            data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] / (
                                                    data_agg_m2['total_ind']/12)
            data_agg_m2['model_data'] = data_agg_m2['trend'] * data_agg_m2['clear_ind_seas']
            data_agg_m2['model_data'] = round(data_agg_m2['model_data'])     
    elif 'polynomial2' in models_lst[0]:
        a = np.polyfit(x, y, 2)[0]
        b = np.polyfit(x, y, 2)[1]
        c = np.polyfit(x, y, 2)[2]
        if 'add' in models_lst[0]:
            lst = []
            for j in x:
                lst.append(a * j**2 + b * j + c)
            data_agg_m2['trend'] = lst
            data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] - data_agg_m2['trend']
            data_agg_m2['total_ind_seas'] = 0
            for j in range(12):
                data_agg_m2.loc[(data_agg_m2.date == j+1), 
                                    'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                    'seasonality'].mean()
                data_agg_m2.loc[(data_agg_m2.date == j+13), 
                                        'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                        'seasonality'].mean()
            data_agg_m2['total_ind'] = data_agg_m2['total_ind_seas'].sum()
            data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] - data_agg_m2['total_ind']/12
            data_agg_m2['model_data'] = data_agg_m2['trend'] + data_agg_m2['clear_ind_seas']
            data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
        else:
            lst = []
            for j in x:
                lst.append(a * j**2 + b * j + c)
            data_agg_m2['trend'] = lst
            data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] / data_agg_m2['trend']
            data_agg_m2['total_ind_seas'] = 0
            for j in range(12):
                data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                        'seasonality'].mean()
            data_agg_m2['total_ind'] = data_agg_m2.loc[:11, 'total_ind_seas'].sum()
            data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] / (data_agg_m2['total_ind']/12)
            data_agg_m2['model_data'] = data_agg_m2['trend'] * data_agg_m2['clear_ind_seas']
            data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
    elif 'polynomial3' in models_lst[0]:
        a = np.polyfit(x, y, 3)[0]
        b = np.polyfit(x, y, 3)[1]
        c = np.polyfit(x, y, 3)[2]
        d = np.polyfit(x, y, 3)[3]
        if 'add' in models_lst[0]:
            lst = []
            for j in x:
                lst.append(a * j**3 + b * j**2 + c * j + d)
            data_agg_m2['trend'] = lst
            data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] - data_agg_m2['trend']
            data_agg_m2['total_ind_seas'] = 0
            for j in range(12):
                data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                        'seasonality'].mean()
                data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
            data_agg_m2['total_ind'] = data_agg_m2['total_ind_seas'].sum()
            data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] - data_agg_m2['total_ind']/12
            data_agg_m2['model_data'] = data_agg_m2['trend'] + data_agg_m2['clear_ind_seas']
            data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
        else:
            lst = []
            for j in x:
                lst.append(a * j**3 + b * j**2 + c * j + d)
            data_agg_m2['trend'] = lst
            data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] / data_agg_m2['trend']
            data_agg_m2['total_ind_seas'] = 0
            for j in range(12):
                data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                'seasonality'].mean()
                data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                'seasonality'].mean()
            data_agg_m2['total_ind'] = data_agg_m2.loc[:11, 'total_ind_seas'].sum()
            data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] / (data_agg_m2['total_ind']/12)
            data_agg_m2['model_data'] = data_agg_m2['trend'] * data_agg_m2['clear_ind_seas']
            data_agg_m2['model_data'] = round(data_agg_m2['model_data'])       
    elif 'log' in models_lst[0]:
        a = np.polyfit(np.log(x), y, 1)[0]
        b = np.polyfit(np.log(x), y, 1)[1]
        if 'add' in models_lst[0]:
            lst = []
            for j in x:
                lst.append(a * np.log(j) + b)
            data_agg_m2['trend'] = lst
            data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] - data_agg_m2['trend']
            data_agg_m2['total_ind_seas'] = 0
            for j in range(12):
                data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                'seasonality'].mean()
                data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                'seasonality'].mean()
            data_agg_m2['total_ind'] = data_agg_m2['total_ind_seas'].sum()
            data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] - data_agg_m2['total_ind']/12
            data_agg_m2['model_data'] = data_agg_m2['trend'] + data_agg_m2['clear_ind_seas']
            data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
        else:
            lst = []
            for j in x:
                lst.append(a * np.log(j) + b)
            data_agg_m2['trend'] = lst
            data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] / data_agg_m2['trend']
            data_agg_m2['total_ind_seas'] = 0
            for j in range(12):
                data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                'seasonality'].mean()
                data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                'seasonality'].mean()
            data_agg_m2['total_ind'] = data_agg_m2.loc[:11, 'total_ind_seas'].sum()
            data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] / (data_agg_m2['total_ind']/12)
            data_agg_m2['model_data'] = data_agg_m2['trend'] * data_agg_m2['clear_ind_seas']
            data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
    elif 'exp' in models_lst[0]:
        k = np.polyfit(x, np.log(y), 1)[0]
        ln_b = np.polyfit(x, np.log(y), 1)[1]
        b = np.exp(ln_b)
        if 'add' in models_lst[0]:
            lst = []
            for j in x:
                lst.append(b * np.exp(k * j))
            data_agg_m2['trend'] = lst
            data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] - data_agg_m2['trend']
            data_agg_m2['total_ind_seas'] = 0
            for j in range(12):
                data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
            data_agg_m2['total_ind'] = data_agg_m2['total_ind_seas'].sum()
            data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] - data_agg_m2['total_ind']/12
            data_agg_m2['model_data'] = data_agg_m2['trend'] + data_agg_m2['clear_ind_seas']
            data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
        else:
            lst = []
            for j in x:
                lst.append(b * np.exp(k * j))
            data_agg_m2['trend'] = lst
            data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] / data_agg_m2['trend']
            data_agg_m2['total_ind_seas'] = 0
            for j in range(12):
                data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                'seasonality'].mean()
                data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                'seasonality'].mean()
            data_agg_m2['total_ind'] = data_agg_m2.loc[:11, 'total_ind_seas'].sum()
            data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] / (data_agg_m2['total_ind']/12)
            data_agg_m2['model_data'] = data_agg_m2['trend'] * data_agg_m2['clear_ind_seas']
            data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
    else:
        k = np.polyfit(np.log(x), np.log(y), 1)[0]
        ln_b = np.polyfit(np.log(x), np.log(y), 1)[1]
        b = np.exp(ln_b)
        if 'add' in models_lst[0]:
            lst = []
            for j in x:
                lst.append(b * (j ** k))
            data_agg_m2['trend'] = lst
            data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] - data_agg_m2['trend']
            data_agg_m2['total_ind_seas'] = 0
            for j in range(12):
                data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                data_agg_m2.loc[(data_agg_m.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
            data_agg_m2['total_ind'] = data_agg_m2['total_ind_seas'].sum()
            data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] - data_agg_m2['total_ind']/12
            data_agg_m2['model_data'] = data_agg_m2['trend'] + data_agg_m2['clear_ind_seas']
            data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
        else:
            lst = []
            for j in x:
                lst.append(b * (j ** k))
            data_agg_m2['trend'] = lst
            data_agg_m2['seasonality'] = data_agg_m2['turnoverOfGoods'] / data_agg_m2['trend']
            data_agg_m2['total_ind_seas'] = 0
            for j in range(12):
                data_agg_m2.loc[(data_agg_m2.date == j+1), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
                data_agg_m2.loc[(data_agg_m2.date == j+13), 'total_ind_seas'] = data_agg_m2.loc[[j, j+12], 
                                                                                    'seasonality'].mean()
            data_agg_m2['total_ind'] = data_agg_m2.loc[:11, 'total_ind_seas'].sum()
            data_agg_m2['clear_ind_seas'] = data_agg_m2['total_ind_seas'] / (data_agg_m2['total_ind']/12)
            data_agg_m2['model_data'] = data_agg_m2['trend'] * data_agg_m2['clear_ind_seas']
            data_agg_m2['model_data'] = round(data_agg_m2['model_data'])
    data_for_plot = data_agg_m2
    ls = nes_data2['date'].dt.strftime('%m/%Y').unique()
    ls = list(ls)
    data_for_plot['dates'] = ls
    data_for_plot['dates'] = data_for_plot['dates'].str.replace('2019', '2020')
    # построение прогноза
    data_lst = data_for_plot['model_data'].to_list()
    date_lst = data_for_plot['date'].to_list()
    trend_lst = data_for_plot['trend'].to_list()
    for i in range(data_agg_m2.loc[data_agg_m2.shape[0]-1,'date']+1, 
                    data_agg_m2.loc[data_agg_m2.shape[0]-1,'date']+1+n):
        date_lst.append(i)
        tr = k * i + b 
        trend_lst.append(tr)
        seas = data_agg_m2.loc[data_agg_m2.loc[data_agg_m2.shape[0]-1,
                                        'date']+1 - 13]['clear_ind_seas']
        data_lst.append(tr + seas)
    s_y = nes_data2['date'].max().year 
    s_m = nes_data2['date'].max().month
    start_date = DT.datetime(s_y, s_m, 1)
    if (s_m + n) % 12 > 0 and (s_m + n) % 12 < 12:
        e_y = s_y + 1
        e_m = (s_m + n) % 12
    else:
        e_y = s_y
        e_m = s_m + n
    end_date = DT.datetime(e_y, e_m, 1) 
    res = pd.date_range(
        min(start_date, end_date),
        max(start_date, end_date)
                            ).strftime('%m/%Y').tolist()
    dates_lst = ls + sorted(list(set(res[31:])))
    d = {'date':date_lst, 'turnover':data_lst, 'dates':dates_lst, 
        'trend':trend_lst}
    df_plt = pd.DataFrame(data=d)
    df_plt['dates'] = df_plt['dates'].str.replace('2019', '2020')
    return data_for_plot, df_plt


# FUNCTION №12
## для отрисовки графика
def find_model_predskaz_graph(data_for_plot=find_model_predskaz_data(nes_data2)[0], df_plt=find_model_predskaz_data(nes_data2)[1]):
    fig = go.Figure()
    fig.add_trace(go.Scatter(name='Оборот предсказанный', x=df_plt['dates'], 
                                y=df_plt['turnover'],
                                mode='lines',
                                line = dict(color='rgb(27, 158, 119)', width=2, dash='dash')))
    fig.add_trace(go.Scatter(name='Оборот фактический', x=data_for_plot['dates'], 
                                    y=data_for_plot['turnoverOfGoods'],
                                    mode='lines',
                                    line = dict(color='rgb(27, 158, 119)')))
    fig.add_trace(go.Scatter(x=df_plt['dates'], y=df_plt['trend'],
                            mode='lines',
                            name='Тренд', line = dict(color='gray', width=1)))
    fig.update_layout(yaxis_title_text='Оборот, шт', 
                        showlegend = True, template="simple_white", 
                        yaxis_tickformat = 'f.',     
                        legend=dict(orientation="h", yanchor="bottom", y=1.1, xanchor="right", x=1,
                        font=dict(size=12)), width=550)
    fig.update_xaxes(tickangle=45, tickfont=dict(size=12), dtick="M2", tickformat="%b\n%Y")
    return fig


# FUNCTION №13
## для построения таблицы
def find_model_predskaz_table(df=find_model_predskaz(nes_data2)[1]):
    SUP = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹") 
    df = df.rename(columns={'R^2':"R2".translate(SUP), 'MAPE':'MAPE, %', 'MAE':'MAE, шт', 'RMSE':'RMSE, шт'})
    df = df.sort_values(['R²', 'MAPE, %'], ascending=False).reset_index(drop=True)
    df = df.astype({'MAPE, %':'str', 'MAE, шт':'str', 'RMSE, шт':'str', 'R²':'str'})
    df['MAPE, %'] = df['MAPE, %'].str.replace('.', ',')
    df['MAE, шт'] = df['MAE, шт'].str.replace('.', ',')
    df['RMSE, шт'] = df['RMSE, шт'].str.replace('.', ',')
    df['R²'] = df['R²'].str.replace('.', ',')
    df = df.head(1)
    df = df[['MAPE, %', 'MAE, шт', 'RMSE, шт', 'R²']]
    return df


# набор функций для XYZ-анализа

# FUNCTION №14
## формирование данных для таблиц и графика
def get_xyz_group_data(data=nes_data, full_data=data):  
    if data['product_code'].nunique() != 1:
        # отсеивание товаров (продающихся реже, чем 1 раз в месяц)
        my_data = data[data['date'].dt.year == 2021]
        prod_lst = my_data.groupby(['product_code', 'month'], 
                                        as_index=False) \
                                        .agg({'turnoverOfGoods':'sum'}) \
                            .groupby('product_code', as_index=False) \
                                .agg({'month':'count'})
        prod_lst = prod_lst[prod_lst['month'] == 12]['product_code'] \
                                                    .to_list()
        my_data = my_data.query('product_code in @prod_lst') \
                                        .reset_index(drop=True)
        # определение порогов для групп XYZ (по категориям)
        cat = data['category'].unique()[0]
        cat_data = full_data[full_data['category'] == cat] \
                                    .reset_index(drop=True)
        cat_data = cat_data[cat_data['date'].dt.year == 2021]
        prod_lst = cat_data.groupby(['product_code', 'month'], 
                        as_index=False) \
                        .agg({'turnoverOfGoods':'sum'}) \
                            .groupby('product_code', as_index=False) \
                                .agg({'month':'count'})
        prod_lst = prod_lst[prod_lst['month'] == 12]['product_code'] \
                                                    .to_list()
        cat_data = cat_data.query('product_code in @prod_lst') \
                                    .reset_index(drop=True)
        std_cat_df = cat_data.groupby(['product_code', 'month'], 
                        as_index=False) \
                        .agg({'turnoverOfGoods':'sum'}) \
                            .groupby('product_code', as_index=False) \
                                .agg({'turnoverOfGoods':'std'}) \
                                    .rename({'turnoverOfGoods':'std'}, axis=1)
        mean_cat_df = cat_data.groupby(['product_code', 'month'], 
                        as_index=False) \
                        .agg({'turnoverOfGoods':'sum'}) \
                            .groupby('product_code', as_index=False) \
                                .agg({'turnoverOfGoods':'mean'}) \
                                    .rename({'turnoverOfGoods':'mean'}, axis=1)
        cat_df = mean_cat_df.merge(std_cat_df, how='inner', on='product_code')
        cat_df = mean_cat_df.merge(std_cat_df, how='inner', on='product_code')
        cat_df['var'] = cat_df['std'] / cat_df['mean'] * 100
        x_lim = np.percentile(cat_df['var'], 33)
        y_lim = np.percentile(cat_df['var'], 66)
        # проведение XYZ-группировки товаров
        std_df = my_data.groupby(['category', 'group', 'subgroup', 
                            'product_code', 'month'], as_index=False) \
                            .agg({'turnoverOfGoods':'sum'}) \
                                .groupby(['category', 'group', 'subgroup', 
                                    'product_code'], as_index=False) \
                                    .agg({'turnoverOfGoods':'std'}) \
                                        .rename({'turnoverOfGoods':'std'}, axis=1)
        mean_df = my_data.groupby(['category', 'group', 'subgroup', 
                            'product_code', 'month'], as_index=False) \
                            .agg({'turnoverOfGoods':'sum'}) \
                                .groupby(['category', 'group', 
                                        'subgroup', 'product_code'], 
                                    as_index=False) \
                                    .agg({'turnoverOfGoods':'mean'}) \
                                        .rename({'turnoverOfGoods':'mean'}, 
                                        axis=1)
        df1 = mean_df.merge(std_df, how='inner', on=['category', 'group', 
                                                'subgroup', 'product_code'])
        df1['var'] = df1['std'] / df1['mean'] * 100
        x_group = df1[df1['var'] <= x_lim]['product_code'].to_list()
        y_group = df1[(df1['var'] > x_lim) & (df1['var'] <= y_lim)]['product_code'] \
                                                                .to_list()
        z_group = df1[df1['var'] > y_lim]['product_code'].to_list()
        # проведение XYZ-группировки (групп товаров)
        std_df = my_data.groupby(['group', 'month'], as_index=False) \
                            .agg({'turnoverOfGoods':'sum'}) \
                                .groupby('group', as_index=False) \
                                    .agg({'turnoverOfGoods':'std'}) \
                                        .rename({'turnoverOfGoods':'std'}, axis=1)
        mean_df = my_data.groupby(['group', 'month'], as_index=False) \
                            .agg({'turnoverOfGoods':'sum'}) \
                                .groupby('group', as_index=False) \
                                    .agg({'turnoverOfGoods':'mean'}) \
                                        .rename({'turnoverOfGoods':'mean'}, axis=1)
        df = mean_df.merge(std_df, how='inner', on='group')
        df['var'] = df['std'] / df['mean'] * 100
        return df1, x_lim, y_lim, x_group, y_group, z_group, my_data, df
    else:
        return 'Невозможно провести XYZ-анализ'


# FUNCTION №15
## отрисовка таблицы 
def get_xyz_group_table2(df1=get_xyz_group_data()[0], x_lim=get_xyz_group_data()[1], y_lim=get_xyz_group_data()[2]): 
    df1['XYZ'] = 0
    df1.loc[(df1['var'] <= x_lim), 'XYZ'] = 'X'
    df1.loc[((df1['var'] > x_lim) & (df1['var'] <= y_lim)), 'XYZ'] = 'Y'
    df1.loc[(df1['var'] > y_lim), 'XYZ'] = 'Z'
    df1 = df1[['category', 'group', 'subgroup', 'product_code', 'var', 'XYZ']]
    df1 = df1.rename(columns={'category':'Категория', 'group':'Группа', 
                                'subgroup':'Подгруппа', 'product_code':'Код товара', 
                                'var':'Коэфф. вариации, %', 'XYZ':'XYZ сектор'})
    df1['Категория'] = df1['Категория'].str.replace(']', '').str.replace('[', '')
    df1['Коэфф. вариации, %'] = pd.Series(["{0:.2f}" \
                .format(val) for val in df1['Коэфф. вариации, %']])
    df1['Коэфф. вариации, %'] = df1['Коэфф. вариации, %'] \
                                        .str.replace(".", ",")
    df1 = df1.head(30)
    return df1



# FUNCTION №16
## отрисовка линейной диаграммы
def get_xyz_group_lin_graph(data=nes_data, df=get_xyz_group_data()[7], x_lim=get_xyz_group_data()[1], y_lim=get_xyz_group_data()[2]): 
    cat = data['category'].unique()[0]
    sort_df = df.sort_values('var')
    sort_df = sort_df.loc[sort_df['group'] != '[женские]колг.жен.']
    xl = sort_df[sort_df['var'] >= x_lim].iloc[0]['group']
    yl = sort_df[sort_df['var'] >= y_lim].iloc[0]['group']
    xcenter = sort_df.iloc[round(sort_df[sort_df['var'] <= x_lim].shape[0] / 2)]['group']
    ycenter = sort_df[(sort_df['var'] >= x_lim) & (sort_df['var'] < y_lim)] \
                    .iloc[round(sort_df[(sort_df['var'] >= x_lim) & (sort_df['var'] < y_lim)] \
                        .shape[0] / 2)]['group']
    zcenter = sort_df[sort_df['var'] >= y_lim].iloc[round(sort_df[sort_df['var'] >= y_lim] \
                                            .shape[0] / 2)]['group']
    xcenter = xcenter.replace('[', '').replace(']', '')
    ycenter = ycenter.replace('[', '').replace(']', '')
    zcenter = zcenter.replace('[', '').replace(']', '')
    sort_df['var'] = round(sort_df['var'], 2)
    fig = go.Figure()
    sort_df['group'] = sort_df['group'].str.replace('[', '') \
                                        .str.replace(']', '')
    fig.add_trace(go.Scatter(x=sort_df['group'], y=sort_df['var'],
                                        mode='markers+lines', name='', 
                                        line = dict(color='rgb(27, 158, 119)', width=2)))
    fig.update_layout(yaxis_title_text='Коэффициент вариации, %',
                        showlegend=False, template="simple_white", 
                        yaxis_tickformat = 'f.', width=550)
    fig.add_vline(x=xl, line_width=2, line_dash="dash", line_color="black")
    fig.add_vline(x=yl, line_width=2, line_dash="dash", line_color="black")
    fig.add_trace(go.Scatter(
                    x=[xcenter, ycenter, zcenter],
                    y=[sort_df['var'].max(), sort_df['var'].max(), 
                    sort_df['var'].max()],
                    mode="text",
                    text=["Группа X", "Группа Y", "Группа Z"],
                    textposition="bottom center", textfont_size=12
                    ))
    fig.update_xaxes(tickangle=45, tickfont=dict(size=12))
    return fig 


# набор функций для кластерного анализа

# FUNCTION №17
## формирование данных для таблицы и графиков
def get_clusters_data(data=nes_data):
    # кластеризация товаров внутри одной подгруппы, группы или категории
    if data['product_code'].nunique() != 1:
        # формирование датафрейма для кластеризации
        df_month = data.groupby('product_code')[['date', 'turnoverOfGoods']] \
                        .resample('M', on='date').sum().reset_index()
        df_month['date'] = df_month['date'].dt.month
        df_class_month = pd.pivot_table(df_month, values='turnoverOfGoods', 
                            index='product_code', columns='date').reset_index()
        df_class_month = df_class_month.fillna(0)
        # стандартизация данных
        scaler = StandardScaler()
        df_scaled_month = scaler.fit_transform(df_class_month.iloc[:,1:].T).T
        # выбор оптимального числа кластеров
        silhouette = []
        K = range(3, 9)
        for k in tqdm(K):
            kmeanModel = TimeSeriesKMeans(n_clusters=k, metric="euclidean", 
                                            n_jobs=6, max_iter=10)
            kmeanModel.fit(df_scaled_month)
            silhouette.append(silhouette_score(df_scaled_month, kmeanModel.labels_))
        k_opt = silhouette.index(max(silhouette)) + 3
        n_clusters = k_opt
        ts_kmeans = TimeSeriesKMeans(n_clusters=n_clusters, metric='euclidean', 
                                        n_jobs=3, max_iter=10)
        ts_kmeans.fit(df_scaled_month)
        # формирование датафрейма с отнесением товаров к кластерам
        df_class_month['cluster'] = ts_kmeans.predict(df_scaled_month)
        df = pd.DataFrame(df_class_month.groupby('cluster')['product_code'] \
                                        .value_counts())
        df_class_month = df_class_month.merge(data[['category', 'group', 
                                'subgroup', 'product_code']].drop_duplicates(), 
                                how='left', on='product_code')
        df_class_month = df_class_month[['category', 'group', 'subgroup', 
                                        'product_code', 'cluster']]
        df_class_month['cluster'] = df_class_month['cluster'] + 1
        df_class_month['category'] = df_class_month['category'].str.replace('[', '') \
                                                                .str.replace(']', '')
        df_class_month = df_class_month.rename(columns={'category':'Категория', 
                                                        'group':'Группа', 
                                                        'subgroup':'Подгруппа', 
                                                        'product_code':'Код товара', 
                                                        'cluster':'Кластер'})
        ls = data['date'].dt.strftime('%m/%Y').unique()
        ls = list(ls)
        s_y = data['date'].max().year 
        s_m = data['date'].max().month
        start_date = DT.datetime(s_y, s_m, 1)
        if (s_m) % 12 > 0 and (s_m) % 12 < 12:
            e_y = s_y + 1
            e_m = (s_m) % 12
        else:
            e_y = s_y
            e_m = s_m
            end_date = DT.datetime(e_y, e_m, 1) 
            res = pd.date_range(
                    min(start_date, end_date),
                    max(start_date, end_date)
                                        ).strftime('%m/%Y').tolist()
        dates_lst = ls + sorted(list(set(res[31:])))
        dates_lst = dates_lst[-12:]
        df = df.rename(columns={'product_code':'n'})
        df2 = df.reset_index()
        df2 = df2.drop('n', axis=1)
        df_clus = df_month.merge(df2, how='left', on='product_code')
        return df, df_class_month, df_clus, n_clusters, dates_lst
    else:
        return "" 


# FUNCTION №18
## отрисовка таблицы
def get_clusters_table(df_class_month=get_clusters_data()[1]):
    df_class_month = df_class_month.head(30)
    return df_class_month


# FUNCTION №19
## отрисовка линейной диаграммы
def get_clusters_lin_graph(df_clus=get_clusters_data()[2], n_clusters=get_clusters_data()[3], dates_lst=get_clusters_data()[4]):
    fig = go.Figure()
    for i in range(n_clusters):
        df_plt = df_clus.groupby(['date', 'cluster'], as_index=False) \
                        .agg({'turnoverOfGoods':'mean'})
        df_plt = df_plt[df_plt['cluster'] == i]
        df_plt['date'] = dates_lst
        fig.add_trace(go.Scatter(x=df_plt['date'], y=df_plt['turnoverOfGoods'],
                                    mode='lines', name=f'Кластер {i+1}', 
                                    line = dict(width=2)))
    fig.update_layout(yaxis_title_text='Оборот, шт',
                        showlegend=True, template="simple_white", 
                        yaxis_tickformat = 'f.',   
                        legend=dict(orientation="h", yanchor="bottom", y=1.1, xanchor="right", x=1,
                        font=dict(size=12)), width=550)
    fig.update_xaxes(tickangle=45, tickfont=dict(size=12))
    return fig



# FUNCTION №20
def filter_data(category, group, subgroup, product_code, start_date, end_date, df):
    filtered_df = df.copy()
    if category is not None:
        filtered_df = filtered_df[filtered_df["category"] == category]
    if group is not None:
        filtered_df = filtered_df[filtered_df["group"] == group]
    if subgroup is not None:
        filtered_df = filtered_df[filtered_df["subgroup"] == subgroup]
    if product_code is not None:
       filtered_df = filtered_df[filtered_df["product_code"] == product_code]
    if start_date is not None:
        if type(start_date) == str:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        filtered_df = filtered_df[filtered_df["date"] >= start_date]
    if end_date is not None:
        if type(end_date) == str:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        filtered_df = filtered_df[filtered_df["date"] <= end_date]
    return filtered_df



# FUNCTION №21
# обновление графиков и таблиц 
def update_graph(df, val,  start_date=None, end_date=None, category=None, group=None, subgroup=None, product_code=None, vals=None):
    filtered_df = filter_data(category, group, subgroup, product_code, start_date, end_date, df)
    if val == 'lin_turn':
        if category == 'для окраски волос':
            perc = 65
        else:
            perc = 75
        return get_statistics_lin_graph(filtered_df), get_statistics_boxplot(filtered_df), find_seas_threshold_graph(filtered_df, category, percent=perc)
    if val == 'boxplot':
        return get_statistics_boxplot(filtered_df)
    if val == 'hist_seasonality':
        if category == 'для окраски волос':
            return find_seas_threshold_graph(filtered_df, category, percent=65)
        else:
            return find_seas_threshold_graph(filtered_df, category, percent=75)
    if category == 'для окраски волос':
        threshold = find_seas_threshold_value(filtered_df, category, percent=65)
    else:
        threshold = find_seas_threshold_value(filtered_df, category, percent=75)
    if val == "seas_plot":
        return find_season_graph(filtered_df, threshold)
    if val == 'seas_table':
        return find_season_table(filtered_df, threshold)
    if val == 'graph_predskaz':
        return find_model_predskaz_graph(data_for_plot=find_model_predskaz_data(filtered_df, n=vals)[0], df_plt=find_model_predskaz_data(filtered_df, n=vals)[1])
    if val == 'table_predskaz':
        return find_model_predskaz_table(df=find_model_predskaz(filtered_df)[1])
    if val == 'xyz_graph':
        return get_xyz_group_lin_graph(data=filtered_df, df=get_xyz_group_data(data=filtered_df, full_data=df)[7], x_lim=get_xyz_group_data(data=filtered_df, full_data=df)[1], y_lim=get_xyz_group_data(data=filtered_df, full_data=df)[2])
    if val == 'cluster_graph':
        return get_clusters_lin_graph(df_clus=get_clusters_data(data=filtered_df)[2], n_clusters=get_clusters_data(data=filtered_df)[3], dates_lst=get_clusters_data(data=filtered_df)[4])
    if val == 'xyz_table':
        return get_xyz_group_table2(df1=get_xyz_group_data(data=filtered_df, full_data=df)[0], x_lim=get_xyz_group_data(data=filtered_df, full_data=df)[1], y_lim=get_xyz_group_data(data=filtered_df, full_data=df)[2])
    if val == 'cluster_table':
        return get_clusters_table(df_class_month=get_clusters_data(data=filtered_df)[1])
    if val == 'seas_tab':
        mydata = find_season_table(filtered_df, threshold)
        return mydata[0], mydata[1], mydata[2]  


# FUNCTION №22
# формирование словаря с доступными группами товаров
def get_available_groups(category, df, type_to_get):
    type = ""
    if type_to_get == "category":
        type = "group"
    elif type_to_get == "group":
        type = "category"
    if category is not None:
        avlbl_categories = df[df[type] == category][type_to_get].unique()
    else:
        avlbl_categories = df[type_to_get].unique()
    return [{'label': categ, 'value': categ} for categ in sorted(avlbl_categories)]


# FUNCTION №23
# формирование словаря с доступными подгруппами товаров
def get_available_subgroups(category, df, type_to_get):
    type = ""
    if type_to_get == "group":
        type = "subgroup"
    elif type_to_get == "subgroup":
        type = "group"
    if category is not None:
        avlbl_categories = df[df[type] == category][type_to_get].unique()
    else:
        avlbl_categories = df[type_to_get].unique()
    return [{'label': categ, 'value': categ} for categ in sorted(avlbl_categories)]


# FUNCTION №24
# формирование словаря с доступными товарами (код товара)
def get_available_products(category, df, type_to_get):
    type = ""
    if type_to_get == "subgroup":
        type = "product_code"
    elif type_to_get == "product_code":
        type = "subgroup"
    if category is not None:
        avlbl_categories = df[df[type] == category][type_to_get].unique()
    else:
        avlbl_categories = df[type_to_get].unique()
    return [{'label': categ, 'value': categ} for categ in sorted(avlbl_categories)]


# FUNCTION №25
# выделение строк таблицы (месяцы повышенного и пониженного спроса)
def data_bars_diverging(df, column, high_month=None, low_month=None, color_above='green', color_below='red'):
    ranges = df[column].to_list()
    styles = []
    for i in range(len(ranges)):
        style = {
            'if': {"row_index": i
            },
            'paddingBottom': 8,
            'paddingTop': 8,
        }
        bound = ranges[i]
        if bound > 0 and i in high_month:
            background = (
                """
                    {color_above}
                """.format(
                    color_above=color_above
                )
            )
        elif bound < 0 and i in low_month:
            background = (
                """
                   {color_below}
                """.format(
                    color_below=color_below
                )
            )
        else:
             background = (
                """
                   {colr}
                """.format(
                    colr='white'
                )
            )
        style['background'] = background
        styles.append(style)
    return styles