import os
from datetime import datetime
import requests
from requests.exceptions import HTTPError
import logging
from configs import config

def log_config(process_name):
    # Setting log level
    now_utc = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    logging.basicConfig(level=logging.INFO,filename="log_"+process_name+"_"+now_utc+".txt")

def save_file(path,df,filename=None):
    if not filename:
        now_utc = datetime.utcnow().strftime("%Y%m%d_%H%M%S")+".csv"
        fullname = os.path.join(path, now_utc)
    else:
        fullname = os.path.join(path, filename)

    if not os.path.exists(path):
        os.mkdir(path)
 
    df.to_csv(fullname)
    return 

def get_fullpath(relative_path):
    absolute_path = os.path.dirname(os.getcwd())
    full_path = os.path.join(absolute_path, relative_path)
    return(full_path)

def req_get(url,headers,stream):
    try:
        response = requests.get(url, headers=headers,stream=True) 
        response.raise_for_status()
    except HTTPError as http_err:
        logging.info(f'HTTP error occurred: {http_err}')  
    except Exception as err:
        logging.info(f'Other error occurred: {err}') 
    else:
        logging.info('Success!')
    return response