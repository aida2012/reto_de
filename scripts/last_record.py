import pandas as pd
import zlib
import json
import logging
from datetime import datetime
import traceback

import sys
sys.path.append('../')
import configs
from configs import config

from utilities import save_file,get_fullpath,req_get,log_config
log_config("last_record")

logging.info("URL is:{}".format(config.URL))


def decompress_data(response):
    data = zlib.decompress(response.content, zlib.MAX_WBITS|32)
    data_string = data.decode('utf-8')
    data_dict = json.loads(data_string)
    df = pd.DataFrame(data_dict)
    return df


def main():
    logging.info("Obtaining response")
    response = req_get(config.URL,config.HEADERS,True)
    try:
        if response:
            logging.info("Obtaining last record for each town")
            df = decompress_data(response)
            df_max = df.groupby(['idmun'])['hloc'].max()
            result = pd.merge(df, df_max,how='inner', on=["idmun", "hloc"])

            logging.info("Saving file")
            relative_path = 'data/data_municipio_x_hora/'
            path = get_fullpath(relative_path)
            save_file(path,result)
            logging.info("File saved")
            logging.info("Process complete")
        else:
            logging.info("No data to save")
    except Exception as e:
        logging.error(str(e))
        logging.exception(traceback.print_exc())
    


if __name__ == '__main__':
    main()