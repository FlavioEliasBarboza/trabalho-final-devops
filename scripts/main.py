from bronze import bronze_ingestion
from silver import *
from gold import gold_transform
from config import config

if __name__ == "__main__":

    # Ingestão 
    for i in range(1,10):
        bronze_ingestion(config, i)

    # Carregamento Silver 
    silver_transform('peoples')
    silver_transform('planets')
    silver_transform('films')

    # Carregamento Gold 
    gold_transform()