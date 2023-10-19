import requests
import json
import logging
from config import config


def grava_log(filename, msg):
    log_format = '%(asctime)s:%(levelname)s:%(filename)s: %(message)s'
    logging.basicConfig(filename=filename,
                        filemode='w',
                        level=logging.DEBUG,
                        format=log_format)
    logger = logging.getLogger('root')


    logger.info(f'{msg}')


def bronze_ingestion(config, page):    

    for item in config:
        
        response = requests.get(f"{item['url']}?page={page}")
        content = json.loads(response.content.decode("utf-8"))

        if(content.get('results')):
            content_serialized = json.dumps(content['results'], indent=4)

            arq_name = item['bronze'].replace('#', str(page))
            with open(arq_name, "w") as outfile:
                outfile.write(content_serialized)

            grava_log('./data/logs/bronze.log', f"Arquivo {arq_name} gravado com sucesso!")


# Ingestão 
for i in range(1,10):
    bronze_ingestion(config, i)

# Tratamento Silver 
silver_transform()