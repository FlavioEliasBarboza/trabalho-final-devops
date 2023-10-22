import pandas as pd
from log import grava_log 

def transform_files_in_df(path):
    import glob
    
    try: 
        filelist = glob.glob(path)
        df_list = [pd.read_json(file) for file in filelist]
        return pd.concat(df_list)
    except Exception as e:
        grava_log("ERRO", 'transform_files_in_df', f"Erro no processo: {e}")

def transform_text(df):
    return df.replace('\n', ' ', regex=True).replace('\r', ' ', regex=True)

def rename_fields(df, path):

    df_meta = pd.read_excel(path)
    for index, column in df_meta.iterrows():
        df.rename(columns={ f"{column['nome_original'].strip()}" : f"{column['nome'].strip()}"}, inplace=True) 

    return df

def verify_duplicates_key(df, path, object):
    
    df_meta = pd.read_excel(path)
    df_aux = df_meta[['nome_original']].where(df_meta["chave"] == 1).dropna()

    for column in df_aux['nome_original']:
        if(len(df[column].unique()) != len(df[column])):
            raise Exception(f"Erro na tabela '{object}': Coluna Chave '{column}' contem duplicadas!")

def verify_not_null_fields(df, path, object):

    df_meta = pd.read_excel(path)
    df_aux = df_meta[['nome_original']].where(df_meta["permite_nulo"] == 0).dropna()

    for column in df_aux['nome_original']:
        if(len(df[df[column].isna()]) > 0):
            raise Exception(f"Erro na tabela '{object}': Coluna '{column}' não permite nulos!")

def transform_null_values(df, object):
    try:

        df.replace("", None, regex=True, inplace = True)
        df.replace("unknown", None, regex=True, inplace = True)
        df.replace("n/a", None, regex=True, inplace = True)
        df.replace("<NA>", None, regex=True, inplace = True)

        return df
    except Exception as e:
        grava_log("ERRO", 'transform_null_values', f"Erro no objeto {object}: {e}")

def transform_to_string(df):
    return df.astype(str)

def silver_transform(object, arq_ori_name, arq_dest_name, path_metadado):

    df = transform_files_in_df(arq_ori_name)
    df = transform_to_string(df)
    df = transform_null_values(df, object)
    df = transform_text(df)

    verify_not_null_fields(df, path_metadado, object)
    verify_duplicates_key(df, path_metadado, object)

    df = rename_fields(df, path_metadado)
    df = df.drop_duplicates()

    df.to_csv(arq_dest_name, sep=';', index=False)
    grava_log("INFO", 'silver_transform', f"Arquivo {arq_dest_name} gravado com sucesso!")

if __name__ == "__main__":
    silver_transform('peoples')
    silver_transform('planets')
    silver_transform('films')