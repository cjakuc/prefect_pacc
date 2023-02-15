import yfinance as yf
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
import os
import datetime as dt


@flow
def run_pipeline(tickers: list[tuple]):
    df_list = retrieve_data(tickers)
    df_list = process_data(df_list)
    save_data(df_list)
    # print("Flow complete")

@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=5)
def retrieve_data(tickers: list[tuple], date: dt.date = str(dt.date.today())) -> list[pd.DataFrame]:
    df_list = []
    for tup in tickers:
        # TODO: add validation that data types are right and tuple length is right
        ticker, name = tup
        df = yf.download(ticker)
        df.name = name
        df_list.append(df)

    return(df_list)

@task
def process_data(df_list: list[pd.DataFrame]) -> list[pd.DataFrame]:
    for df in df_list:
        df = df.reset_index()
        df = df.loc[df['Date'] == str(dt.date.today())]
    return df_list

@task
def save_data(df_list: list[pd.DataFrame], directory: str = 'data'):
    cwd = os.getcwd()
    path = f"{cwd}/{directory}/yfinance"
    if not os.path.isdir(path):
        os.makedirs(path)
    for df in df_list:
        # TODO: df.name is a custom attribute so doesn;t work with pickling; come up with a diffeent way to pass through the city name
        # filename = str(df.name)
        print(df.shape)
        df.to_csv(f"{path}/{df.name}.csv")

if __name__ == "__main__":
    apple = ('AAPL', 'Apple')
    google = ('GOOG', 'Google')
    tesla = ('TSLA', 'Tesla')
    tickers = [apple, google, tesla]

    run_pipeline(tickers=tickers)
