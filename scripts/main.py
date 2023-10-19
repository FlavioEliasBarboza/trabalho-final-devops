from bronze import bronze_ingestion
from silver import *
from config import config

if __name__ == "__main__":

    # Ingestão 
    for i in range(1,10):
        bronze_ingestion(config, i)

    # Tratamento Silver 
    silver_transform_peoples()
    
    #silver_transform_planets()
    #silver_transform_films()