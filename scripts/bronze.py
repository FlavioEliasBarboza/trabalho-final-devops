
def bronze_ingestion(config, page):  

    import requests
    import json
    from log import grava_log 

    for item in config:
        
        response = requests.get(f"{item['url']}?page={page}")
        content = json.loads(response.content.decode("utf-8"))

        if(content.get('results')):
            content_serialized = json.dumps(content['results'], indent=4)

            arq_name = item['bronze'].replace('#', str(page))
            with open(arq_name, "w") as outfile:
                outfile.write(content_serialized)

            grava_log('./data/logs/bronze.csv', f"Arquivo {arq_name} gravado com sucesso!")

