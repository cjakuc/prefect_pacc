from prefect import flow, task
import pandas as pd
import os
from openmeteo_py import Hourly, Daily, Options, OWmanager

@flow
def run_pipeline(coordinates: list[tuple]):
    dict_list = retrieve_data(coordinates)
    df_list = process_data(dict_list)
    save_data(df_list)
    # print("Flow complete")

@task
def retrieve_data(coordinates: list[tuple]) -> list[dict]:
    dict_list = []
    for tup in coordinates:
        # TODO: add validation that data types are right and tuple length is right
        lat,lon,city_name = tup[0], tup[1], tup[2]
        hourly = Hourly()
        daily = Daily()
        options = Options(lat,lon)
        mgr = OWmanager(options,
                        hourly.all(),
                        daily.all()
                        )
        data = mgr.get_data()
        data['name'] = city_name
        dict_list.append(data)

    return(dict_list)

@task
def process_data(dict_list: list[pd.DataFrame]) -> list[pd.DataFrame]:
    df_list = []
    for my_dict in dict_list:
        df = pd.DataFrame(my_dict['hourly'])
        df.name = my_dict['name']
        df_list.append(df)
    return df_list

@task
def save_data(df_list: list[pd.DataFrame], directory: str = 'data'):
    cwd = os.getcwd()
    path = f"{cwd}/{directory}/weather"
    if not os.path.isdir(path):
        os.makedirs(path)
    for df in df_list:
        filename = str(df.name)
        df.to_csv(f"{path}/{df.name}.csv")

if __name__ == "__main__":
    nyc_coordinates = (40.7128, 74.0060, 'NYC')
    sg_coordinates = (40.1321, 74.0346, 'Sea Girt')
    bld_coordinates = (40.0150, 105.2705, 'Boulder')
    elika_coordinates = (36.222, 22.9223, 'Elika')
    coordinates = [nyc_coordinates,
                    sg_coordinates,
                    bld_coordinates,
                    elika_coordinates
                    ]

    run_pipeline(coordinates=coordinates)
