 # API-ключ от сайта www.alphavantage.co
def params_apikey(): return 'NONE'
# валюты и акции для наблюдения
def params_currencies(): return ['AAPL', 'GOOGL', 'AMD', 'IBM']

# параметры подключения к БД
def get_data_db(): return "postgresql+psycopg2"
def get_data_login(): return "postgres"
def get_data_password(): return "postgres"
def get_data_ip(): return "host.docker.internal"
def get_data_port(): return "2345"
def get_data_name_bd(): return "currency_dwh"