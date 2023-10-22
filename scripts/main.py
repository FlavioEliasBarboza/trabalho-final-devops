from bronze import bronze_ingestion
from silver import silver_transform
from gold import gold_transform
from config import config
import pandas as pd

if __name__ == "__main__":

    # Ingestão 
    for i in range(1,10):
        bronze_ingestion(config, i)

    # Carregamento Silver 
    for c in config:
        silver_transform(
            c['object'], 
            f"./data/bronze/{c['object']}_*",
            c['silver'],
            c['meta_silver'])

    # Carregamento Gold 
    gold_transform()

    print(pd.read_csv('./data/gold/filme_consolidado.csv', sep=";"))