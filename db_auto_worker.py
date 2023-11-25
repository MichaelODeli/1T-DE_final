from sqlalchemy import text, create_engine, DateTime, engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import requests
from datetime import datetime, timedelta


# file for Apache Airflow

def get_conn():
    # после перехода на Airflow - переделать получение данных
    db = "postgresql+psycopg2"
    login = "postgres"
    password = "postgres"
    ip = "localhost"
    port = "5432"
    name_bd = "currency_dwh"

    engine = create_engine(f"{db}://{login}:{password}@{ip}:{port}/{name_bd}")
    if not database_exists(engine.url):
        create_database(engine.url)

    conn = engine.connect()

    return conn


def get_data_for_raw_layer(API_KEY, symbols):
    """
    Генерирует данные для сырого слоя DWH за счет получения информации с API.
    ---
    ---
    Параметры:\n
    - API_KEY - API ключ для сайта alphavantage.co\n
    - symbols - названия курсов, для которых нужно получить данные\n
    ---
    Вывод: \n
    pd.DataFrame - данные, полученные с API
    """
    if type(symbols) == str: symbols = [symbols]
    else: pass
    total_list = []
    for symbol in symbols:
        access_link = f"https://www.alphavantage.co/query"
        # задаем параметры для работы с API
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol' : symbol,
            'interval': '15min',
            'outputsize': 'full',
            'apikey': API_KEY
        }
        response = requests.get(access_link, params=params)
        result = response.json()
        datetimes_list = list(result['Time Series (15min)'].keys())
        stats_list = [
        [
            datetime_list, 
            result['Meta Data']['2. Symbol'],
            result['Time Series (15min)'][datetime_list]['1. open'],
            result['Time Series (15min)'][datetime_list]['2. high'],
            result['Time Series (15min)'][datetime_list]['3. low'],
            result['Time Series (15min)'][datetime_list]['4. close'],
            result['Time Series (15min)'][datetime_list]['5. volume']
        ] 
        for datetime_list in datetimes_list]
        total_list += stats_list
    return pd.DataFrame(data=total_list, columns=['date', 'currency', 'open', 'high', 'low', 'close', 'volume'])


def upload_data_to_raw_layer(raw_data: pd.DataFrame, conn:engine.base.Connection):
    """
    Отправка данных на raw слой DWH.
    ---
    ---
    Параметры:
    - raw_data: pd.DataFrame с данными, которые нужно занести на raw слой. 
    - conn: подключение к БД
    ---
    Вывод:
    True в случае успешного обновления raw-слоя. Иначе - возврат ошибки.
    """
    try:
        # определим типы данных для колонок
        dtype = {
            "date": DateTime
        }
        # переносим данные в raw-слой
        raw_data.to_sql('raw_data', conn, if_exists='append', dtype=dtype)
        # выдываем sql-команду для удаления дублей по двум колонкам.
        sql = '''
        DELETE FROM raw_data T1
            USING   raw_data T2
            WHERE T1.ctid < T2.ctid 
            AND T1.date = T2.date 
            AND T1.currency  = T2.currency;
        '''
        conn.execute(sql)
        return True
    except Exception as e:
        return e


def core_layer_data_worker(conn:engine.base.Connection):
    """
    Генерация данных для core-слоя.
    ---
    ---
    Параметры:
    - conn: подключение к БД.
    ---
    Вывод:
    Обновленные таблицы в БД
    """
    data_from_db = pd.read_sql('SELECT * FROM raw_data;', conn)

    # создаем "родительскую" таблицу для всех курсов
    total_df = pd.DataFrame(columns=['cur_id', 'cur_name'], data=list(zip(range(len(data_from_db['currency'].unique())), data_from_db['currency'].unique())))
    total_df.to_sql('core_currencies', conn, if_exists='replace', index=False)
    conn.execute('ALTER TABLE core_currencies ADD PRIMARY KEY (cur_id);')

    # генерируем таблицы с данными по отдельным курсам.
    dtype = {"date": DateTime}
    for cur_name in data_from_db['currency'].unique():
        # создаем копию датафрейма только по нужному курсу
        copy_df = data_from_db[data_from_db['currency']==cur_name].copy(deep=True)
        copy_df = copy_df[['date', 'open', 'high', 'low', 'close', 'volume']]
        # добавляем столбцы с идентификатоами курсов и нумерацией колонок
        copy_df['cur_id'] = [total_df[total_df['cur_name']==cur_name]['cur_id'].tolist()[0]]*len(copy_df)
        copy_df['id'] = range(len(copy_df))
        copy_df = copy_df[['id', 'cur_id', 'date', 'open', 'high', 'low', 'close', 'volume']]
        # переносим данные в БД, не забыв о добавлении foreign ключа
        copy_df.to_sql(f'core_{cur_name.lower()}_curdata', conn, if_exists='replace', index=False, dtype=dtype)
        conn.execute(f'ALTER TABLE core_{cur_name}_curdata ADD CONSTRAINT cur_id FOREIGN KEY (cur_id) REFERENCES core_currencies (cur_id);')


def mart_layer_worker(conn:engine.base.Connection):
    """
    Генерация данных для слоя mart.
    ---
    ---
    Параметры:
    - conn: подключение к БД.
    ---
    Вывод:
    Обновленные таблицы в БД
    """
    total_df = pd.read_sql('select * from core_currencies;', conn)

    # генерация таблиц с полными данными по всему слепку
    full_mart_layer = pd.DataFrame(columns=['datestamp', 'currency_name', 'total_volume', 'open_price', 'close_price', 'difference', 'max_value_time', 'max_price_time', 'min_price_time'])
    dtype = {"datestamp": DateTime}
    for cur_name in total_df['cur_name']:
        # получаем данные по нужному курсу
        data_df = pd.read_sql(f'select * from core_{cur_name.lower()}_curdata', conn).sort_values(by='date', ascending=False)

        # проходимся по всем датам и считаем статистику
        for selected_date in data_df['date'].dt.date.unique():
            # выбор части датафрейма
            selected_date_df = data_df[data_df['date'].dt.date==selected_date].sort_values(by='date', ascending=True)

            # получаем временной промежуток для самого большого чиста торгов
            high_time1 = selected_date_df[selected_date_df['volume'] == max(selected_date_df['volume'])]['date'].tolist()[0]
            max_value_time = str((high_time1 - timedelta(minutes=15)).time()) + ' - ' + str(high_time1.time())

            # получаем временной промежуток для самой высокой цены
            high_time2 = selected_date_df[selected_date_df['high'] == max(selected_date_df['high'])]['date'].tolist()[0]
            max_price_time = str((high_time2 - timedelta(minutes=15)).time()) + ' - ' + str(high_time2.time())

            # получаем временной промежуток для самой низкой цены
            high_time3 = selected_date_df[selected_date_df['low'] == min(selected_date_df['low'])]['date'].tolist()[0]
            min_price_time = str((high_time3 - timedelta(minutes=15)).time()) + ' - ' + str(high_time3.time())

            # обновляем фрейм
            full_mart_layer.loc[-1] = [
                selected_date,
                cur_name, 
                sum(selected_date_df['volume']), 
                selected_date_df.head(1)['open'].tolist()[0],
                selected_date_df.tail(1)['close'].tolist()[0],
                selected_date_df.tail(1)['close'].tolist()[0]/selected_date_df.head(1)['open'].tolist()[0]*100-100, 
                max_value_time, 
                max_price_time, 
                min_price_time]  # adding a row
            full_mart_layer.index = full_mart_layer.index + 1  # shifting index
            full_mart_layer = full_mart_layer.sort_index()  # sorting by index
    # заносим данные в mark-таблицу с перезаписью
    full_mart_layer.to_sql('mart_full', conn, if_exists='replace', dtype=dtype)

    # генерация таблиц по данным за последние сутки
    diff_mart_layer = pd.DataFrame(columns=['dates_diff', 'currency_name', 'total_volume', 'open_price', 'close_price', 'difference'])
    for currency_name in total_df['cur_name']:
        data_df = full_mart_layer[full_mart_layer['currency_name'] == currency_name].copy(deep=True).tail(2)
        dates_diff = ' - '.join([str(i) for i in data_df['datestamp'].tolist()])
        data_df = data_df[['total_volume', 'open_price', 'close_price', 'difference']]
        diff_mart_layer.loc[-1] = [
            dates_diff,
            currency_name,
            data_df.diff().tail(1)['total_volume'].tolist()[0],
            data_df.diff().tail(1)['open_price'].tolist()[0],
            data_df.diff().tail(1)['close_price'].tolist()[0],
            data_df.diff().tail(1)['difference'].tolist()[0]
        ]
        diff_mart_layer.index = diff_mart_layer.index + 1  # shifting index
        diff_mart_layer = diff_mart_layer.sort_index()  # sorting by index
    diff_mart_layer.to_sql('mart_delta', conn, if_exists='replace')