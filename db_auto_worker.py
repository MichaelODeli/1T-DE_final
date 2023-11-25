from sqlalchemy import text, create_engine, DateTime, engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import requests


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
    params:\n
    API_KEY - API ключ для сайта alphavantage.co\n
    symbols - названия курсов, для которых нужно получить данные\n
    ---
    output: \n
    pd.DataFrame
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
    try:
        dtype = {
            "date": DateTime
        }
        raw_data.to_sql('raw_data', conn, if_exists='append', dtype=dtype)
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
    data_from_db = pd.read_sql('SELECT * FROM raw_data;', conn)

    # создаем "родительскую" таблицу для всех курсов
    total_df = pd.DataFrame(columns=['cur_id', 'cur_name'], data=list(zip(range(len(data_from_db['currency'].unique())), data_from_db['currency'].unique())))
    total_df.to_sql('core_currencies', conn, if_exists='replace', index=False)
    conn.execute('ALTER TABLE core_currencies ADD PRIMARY KEY (cur_id);')

    # генерируем таблицы с данными по отдельным курсам.
    dtype = {"date": DateTime}
    for cur_name in data_from_db['currency'].unique():
        copy_df = data_from_db[data_from_db['currency']==cur_name].copy(deep=True)
        copy_df = copy_df[['date', 'open', 'high', 'low', 'close', 'volume']]
        copy_df['cur_id'] = [total_df[total_df['cur_name']==cur_name]['cur_id'].tolist()[0]]*len(copy_df)
        copy_df['id'] = range(len(copy_df))
        copy_df = copy_df[['id', 'cur_id', 'date', 'open', 'high', 'low', 'close', 'volume']]
        copy_df.to_sql(f'core_{cur_name.lower()}_curdata', conn, if_exists='replace', index=False, dtype=dtype)
        conn.execute(f'ALTER TABLE core_{cur_name}_curdata ADD CONSTRAINT cur_id FOREIGN KEY (cur_id) REFERENCES core_currencies (cur_id);')