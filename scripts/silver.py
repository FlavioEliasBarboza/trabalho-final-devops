import pandas as pd
from datetime import datetime
from log import grava_log 

def transform_files_in_df(path):
    import glob
    filelist = glob.glob(path)
    df_list = [pd.read_json(file) for file in filelist]
    return pd.concat(df_list)

def rename_fields(df, path_metadado):

    df_meta = pd.read_excel(path_metadado)
    for index, column in df_meta.iterrows():
        df.rename(columns={ f"{column['nome_original'].strip()}" : f"{column['nome'].strip()}"}, inplace=True) 

    return df

def transform_to_string_and_null(df):
    columns_string = df.select_dtypes(include="object")
    columns_numeric = df.select_dtypes(include=['float64', 'int64'])
    columns_date = df.select_dtypes(include=['datetime', 'datetime64', 'datetime64[ns]', 'datetimetz'])
    df[columns_string.columns] = df[columns_string.columns].fillna('').astype(str)
    df[columns_numeric.columns] = df[columns_numeric.columns].astype(str).replace('<NA>', '')
    df[columns_date.columns] = df[columns_date.columns].astype(str)
    return df   

def silver_transform(object):

    arq_ori_name = f"./data/bronze/{object}_*"
    arq_dest_name = f"./data/silver/{object}.csv"
    path_metadado = f"./metadados/{object}_to_silver.xlsx"

    df = transform_files_in_df(arq_ori_name)
    df = transform_to_string_and_null(df)
    df = rename_fields(df, path_metadado)
    df = df.drop_duplicates()

    df.to_csv(arq_dest_name, index=False)
    grava_log('./data/logs/silver.csv', f"Arquivo {arq_dest_name} gravado com sucesso!")

    return True
