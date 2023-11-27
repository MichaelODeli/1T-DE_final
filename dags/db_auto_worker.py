from sqlalchemy import create_engine, DateTime, engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import requests
from datetime import timedelta, datetime
import project_settings as ps

# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG

# Operators; we need this to operate!
from airflow.operators.python import task

def get_conn():
    db = ps.get_data_db()
    login = ps.get_data_login()
    password = ps.get_data_password()
    ip = ps.get_data_ip()
    port = ps.get_data_port()
    name_bd = ps.get_data_name_bd()

    # если базы данных нет - она будет создана
    engine = create_engine(f"{db}://{login}:{password}@{ip}:{port}/{name_bd}")
    if not database_exists(engine.url):
        create_database(engine.url)

    pgconn = engine.connect()

    return pgconn

with DAG(
    "currency_updater",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(days=1)
    },
    description="DAG by MichaelODeli",
    start_date=datetime(2023, 11, 26),
    catchup=False,
    tags=["1T"],
) as dag:
    pgconn = get_conn()

    @task(task_id="raw_layer_data_worker")
    def raw_layer_data_worker(pgconn:engine.base.Connection, API_KEY=str(ps.params_apikey()), symbols=list(ps.params_currencies())):
        """
        Отправка данных на raw слой DWH.
        ---
        ---
        Параметры:
        - pgconn: подключение к БД
        - API_KEY - API ключ для сайта alphavantage.co\n
        - symbols - названия курсов, для которых нужно получить данные\n
        ---
        Вывод:
        True в случае успешного обновления raw-слоя. Иначе - возврат ошибки.
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
        raw_data = pd.DataFrame(data=total_list, columns=['date', 'currency', 'open', 'high', 'low', 'close', 'volume'])

        try:
            # определим типы данных для колонок
            dtype = {
                "date": DateTime
            }
            # переносим данные в raw-слой
            raw_data.to_sql('raw_data', pgconn, if_exists='append', dtype=dtype)
            # выдываем sql-команду для удаления дублей по двум колонкам.
            sql = '''
            DELETE FROM raw_data T1
                USING   raw_data T2
                WHERE T1.ctid < T2.ctid 
                AND T1.date = T2.date 
                AND T1.currency  = T2.currency;
            '''
            pgconn.execute(sql)
            return True
        except Exception as e:
            return e

    @task(task_id="core_layer_data_worker")
    def core_layer_data_worker(pgconn:engine.base.Connection):
        """
        Генерация данных для core-слоя.
        ---
        ---
        Параметры:
        - pgconn: подключение к БД.
        ---
        Вывод:
        Обновленные таблицы в БД
        """
        data_from_db = pd.read_sql('SELECT * FROM raw_data;', pgconn)

        # создаем "родительскую" таблицу для всех курсов
        total_df = pd.DataFrame(columns=['cur_id', 'cur_name'], data=list(zip(range(len(data_from_db['currency'].unique())), data_from_db['currency'].unique())))
        pgconn.execute('DROP TABLE IF EXISTS core_currencies CASCADE')
        total_df.to_sql('core_currencies', pgconn, if_exists='replace', index=False)
        pgconn.execute('ALTER TABLE core_currencies ADD PRIMARY KEY (cur_id);')

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
            copy_df.to_sql(f'core_{cur_name.lower()}_curdata', pgconn, if_exists='replace', index=False, dtype=dtype)
            pgconn.execute(f'ALTER TABLE core_{cur_name}_curdata ADD CONSTRAINT cur_id FOREIGN KEY (cur_id) REFERENCES core_currencies (cur_id);')

    @task(task_id="mart_layer_worker")
    def mart_layer_worker(pgconn:engine.base.Connection):
        """
        Генерация данных для слоя mart.
        ---
        ---
        Параметры:
        - pgconn: подключение к БД.
        ---
        Вывод:
        Обновленные таблицы в БД
        """
        total_df = pd.read_sql('select * from core_currencies;', pgconn)

        # генерация таблиц с полными данными по всему слепку
        full_mart_layer = pd.DataFrame(columns=['datestamp', 'currency_name', 'total_volume', 'open_price', 'close_price', 'difference', 'max_value_time', 'max_price_time', 'min_price_time'])
        dtype = {"datestamp": DateTime}
        for cur_name in total_df['cur_name']:
            # получаем данные по нужному курсу
            data_df = pd.read_sql(f'select * from core_{cur_name.lower()}_curdata', pgconn).sort_values(by='date', ascending=False)

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
                    sum(selected_date_df['volume'].astype(float)), 
                    selected_date_df.head(1)['open'].tolist()[0],
                    selected_date_df.tail(1)['close'].tolist()[0],
                    selected_date_df.tail(1)['close'].astype(float).tolist()[0]/selected_date_df.head(1)['open'].astype(float).tolist()[0]*100-100, 
                    max_value_time, 
                    max_price_time, 
                    min_price_time]  # adding a row
                full_mart_layer.index = full_mart_layer.index + 1  # shifting index
                full_mart_layer = full_mart_layer.sort_index()  # sorting by index
        # заносим данные в mark-таблицу с перезаписью
        full_mart_layer.to_sql('mart_full', pgconn, if_exists='replace', dtype=dtype)

        # генерация таблиц по данным за последние сутки
        diff_mart_layer = pd.DataFrame(columns=['dates_diff', 'currency_name', 'total_volume', 'open_price', 'close_price', 'difference'])
        for currency_name in total_df['cur_name']:
            data_df = full_mart_layer[full_mart_layer['currency_name'] == currency_name].copy(deep=True).tail(2)
            dates_diff = ' - '.join([str(i) for i in data_df['datestamp'].tolist()])
            data_df = data_df[['total_volume', 'open_price', 'close_price', 'difference']]
            data_df['total_volume'] = data_df['total_volume'].astype(float)
            data_df['open_price'] = data_df['open_price'].astype(float)
            data_df['close_price'] = data_df['close_price'].astype(float)
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
        diff_mart_layer.to_sql('mart_delta', pgconn, if_exists='replace')


    raw_layer_data_worker(pgconn=pgconn) >> core_layer_data_worker(pgconn) >> mart_layer_worker(pgconn)