import pandas as pd

def count_series(df, column):
   
    def contar_elementos(arr):
        arr = arr.replace("'",'').replace(']','').replace('[','').split(',')
        return len(arr)

    df['qtd'] = df[column].apply(contar_elementos)
    return df['qtd']

def gold_transform():

    df_fc = pd.DataFrame()  
    
    films = pd.read_csv("./data/silver/films.csv", sep=';')
    peoples = pd.read_csv("./data/silver/peoples.csv", sep=';')
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

    #df_fc['med_periodo_rotacao_planetas'] = None 
    #df_fc['min_populacao_planetas'] = None 
    #df_fc['med_populacao_planetas'] = None 
    #df_fc['max_populacao_planetas'] = None 

    df_fc.to_csv('./data/gold/filme_consolidado.csv', sep=';', index=False)

if __name__ == "__main__":
    gold_transform()