import pandas as pd
import ast
from log import grava_log 

def count_series(df, column):
    df_copy = df.copy()
   
    def contar_elementos(arr):
        arr = arr.replace("'",'').replace(']','').replace('[','').split(',')
        return len(arr)

    df_copy['qtd'] = df_copy[column].apply(contar_elementos)
    return df_copy['qtd']

def calculate_series(df, df2, column_calc, type_calc):

    df_copy = df.copy()

    df_copy['url_planetas'] = df_copy['url_planetas'].apply(ast.literal_eval)
    df_copy = df_copy.explode('url_planetas')
    df_result = df_copy[['url_origem', 'url_planetas']].merge(df2[['url_origem', column_calc]], left_on='url_planetas', right_on='url_origem', how='left')
    df_result = df_result[['url_origem_x', 'url_planetas', column_calc]].dropna()

    if(type_calc == 'min'):
        df_result = df_result.groupby('url_origem_x')[column_calc].min().reset_index()
        return df_result[column_calc]
    elif(type_calc == 'max'):
        df_result = df_result.groupby('url_origem_x')[column_calc].max().reset_index()
        return df_result[column_calc]
    elif(type_calc == 'mean'):
        df_result = df_result.groupby('url_origem_x')[column_calc].mean().reset_index()
        return df_result[column_calc]

def gold_transform():

    arq_dest_name = './data/gold/filme_consolidado.csv'

    try:

        df_fc = pd.DataFrame()  
    
        films = pd.read_csv("./data/silver/films.csv", sep=';')
        planets = pd.read_csv("./data/silver/planets.csv", sep=';')

        df_fc['titulo'] = films['titulo']
        df_fc['num_episodio'] = films['num_episodio'] 
        df_fc['nom_diretor'] = films['nom_diretor'] 
        df_fc['nom_produtor'] = films['nom_produtor'] 
        df_fc['data_lancamento'] = films['dt_release'] 
        df_fc['qtd_personagens'] = count_series(films, 'url_pesonagens')
        df_fc['qtd_planetas'] = count_series(films, 'url_planetas')
        df_fc['qtd_naves'] = count_series(films, 'url_naves')
        df_fc['qtd_veiculos'] = count_series(films, 'url_veiculos')
        df_fc['qtd_especies'] = count_series(films, 'url_especies')

        df_fc['med_periodo_rotacao_planetas'] = calculate_series(films, planets, 'num_periodo_rotacao', 'mean')
        df_fc['min_populacao_planetas'] = calculate_series(films, planets, 'num_populacao', 'min')  
        df_fc['med_populacao_planetas'] = calculate_series(films, planets, 'num_populacao', 'mean')   
        df_fc['max_populacao_planetas'] = calculate_series(films, planets, 'num_populacao', 'max')   

        df_fc.to_csv(arq_dest_name, sep=';', index=False)
        grava_log("INFO", 'gold_transform', f"Arquivo {arq_dest_name} gravado com sucesso!")

    except Exception as e:
        grava_log("ERRO", 'gold_transform', f"Erro no processo: {e}")


if __name__ == "__main__":
    gold_transform()