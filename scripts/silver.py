import pandas as pd
from datetime import datetime
from log import grava_log 

def transform_files_in_df(path):
    import glob
    filelist = glob.glob(path)
    df_list = [pd.read_json(file) for file in filelist]
    return pd.concat(df_list)

def transform_to_string_and_null(df):
    columns_string = df.select_dtypes(include="object")
    columns_numeric = df.select_dtypes(include=['float64', 'int64'])
    columns_date = df.select_dtypes(include=['datetime', 'datetime64', 'datetime64[ns]', 'datetimetz'])
    df[columns_string.columns] = df[columns_string.columns].fillna('').astype(str)
    df[columns_numeric.columns] = df[columns_numeric.columns].astype(str).replace('<NA>', '')
    df[columns_date.columns] = df[columns_date.columns].astype(str)
    return df 
    

def silver_transform_peoples():

    arq_ori_name = "./data/bronze/peoples_*"
    arq_dest_name = "./data/silver/peoples.csv"

    df = transform_files_in_df(arq_ori_name)
    df = df.explode('films').explode('species').explode('vehicles').explode('starships')
    df['dt_insert'] = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    #Parse para String e Tratamento de Nulos
    df = transform_to_string_and_null(df)
    df = df.drop_duplicates()

    df.to_csv(arq_dest_name, index=False)

    grava_log('./data/logs/silver.log', f"Arquivo {arq_dest_name} gravado com sucesso!")

    return True

def silver_transform_planets():

    arq_ori_name = "./data/bronze/planets_*"
    arq_dest_name = "./data/silver/planets.csv"

    df = transform_files_in_df(arq_ori_name)
    df = df.explode('residents').explode('films')
    df['dt_insert'] = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    #Parse para String e Tratamento de Nulos
    df = transform_to_string_and_null(df)
    df = df.drop_duplicates()

    df.to_csv(arq_dest_name, index=False)

    grava_log('./data/logs/silver.log', f"Arquivo {arq_dest_name} gravado com sucesso!")

    return True

def silver_transform_films():
        
    arq_ori_name = "./data/bronze/films_*"
    arq_dest_name = "./data/silver/films.csv"

    df = transform_files_in_df(arq_ori_name)
    df = df.explode('characters').explode('planets').explode('starships').explode('vehicles').explode('species')
    df['dt_insert'] = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    #Parse para String e Tratamento de Nulos
    df = transform_to_string_and_null(df)
    df = df.drop_duplicates()

    df.to_csv(arq_dest_name, index=False)

    grava_log('./data/logs/silver.log', f"Arquivo {arq_dest_name} gravado com sucesso!")

    return True
