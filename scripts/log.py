import pandas as pd
import os
from datetime import datetime

def grava_log(type, function, msg):

    FILE_LOG = "./data/logs.csv"

    dict = {
        'datetime': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        'type': [type], 
        'function': [function],
        'message': [msg]
    }

    df = pd.DataFrame.from_dict(dict)

    try:
        
        if(os.path.isfile(FILE_LOG)):
            df.to_csv(FILE_LOG, mode="a", index=False, header=False)
        else:
            df.to_csv(FILE_LOG, mode="a", index=False)
    
    except:
        raise Exception("Erro ao tentar gravar log!")
    