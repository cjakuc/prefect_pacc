from prefect.blocks.system import Secret
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
import os, requests
import datetime as dt

SECRET_BLOCK = Secret.load("alpha-vantage-api-key")

# Access the stored secret
SECRET_BLOCK.get()

@flow
def run_pipeline(tickers: list[tuple]):
    df_list = retrieve_data(tickers)
    df_list = process_data(df_list)
    save_data(df_list)

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=5)
def retrieve_data(tickers: list[tuple], date: dt.date = str(dt.date.today())) -> list[dict]:
    dict_list = []
    for tup in tickers:
        # TODO: add validation that data types are right and tuple length is right
        ticker, name = tup
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=5min&apikey={SECRET_BLOCK}'
        r = requests.get(url)
        data = r.json()
        my_dict = data['Time Series (5min)']
        my_dict['ticker'] = ticker
        my_dict['name'] = name
        dict_list.append(my_dict)

    return(dict_list)

@task
def process_data(dict_list: list[dict]) -> list[pd.DataFrame]:
    df_list = []
    for my_dict in dict_list:
        df = pd.DataFrame()
        ticker = my_dict['ticker']
        name = my_dict['name']
        for time_dict in list(my_dict.keys()):
            time_df = pd.DataFrame(my_dict[time_dict])
            df.concat(time_df)
        df['ticker'] = ticker
        df.name = name
        df_list.append(df)
    return df_list

@task
def save_data(df_list: list[pd.DataFrame], directory: str = 'data'):
    cwd = os.getcwd()
    path = f"{cwd}/{directory}/alphavantage"
    if not os.path.isdir(path):
        os.makedirs(path)
    for df in df_list:
        # filename = str(df.name)
        df.to_csv(f"{path}/{df.name}.csv")


if __name__ == "__main__":
    apple = ('AAPL', 'Apple')
    google = ('GOOG', 'Google')
    tesla = ('TSLA', 'Tesla')
    tickers = [apple, google, tesla]

    run_pipeline(tickers=tickers)