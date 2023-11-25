import requests
import pandas as pd
import project_settings as sets

API_KEY = str(sets.params_apikey())
symbols_from_settings = list(sets.params_currencies())


def get_data_for_raw_layer(API_KEY:str = API_KEY, symbols:list=symbols_from_settings) -> pd.DataFrame:
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
        try:
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
        except Exception as e: 
            print(e)
            continue
    return pd.DataFrame(data=total_list, columns=['date', 'currency', 'open', 'high', 'low', 'close', 'volume'])

def get_data_for_core_layer():
    """
    Генерирует данные для слоя ядра, используя данные из сырого слоя.
    """
    return None

def get_data_for_mart_layer(mode):
    """
    Генерирует данные для слоя витрин.\n
    """
    if mode == 'increment':
        pass
    elif mode == 'full':
        pass
    else:
        raise ValueError('Expected "increment" or "full" mode.')
