from sqlalchemy import text, create_engine, DateTime, engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd


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