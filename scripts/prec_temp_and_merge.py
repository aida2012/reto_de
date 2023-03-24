
import pandas as pd
import os
import logging
from datetime import datetime
import traceback
import sys
sys.path.append('../')
import configs
from configs import config

from utilities import save_file,get_fullpath,log_config

log_config("prec_temp_merge")
# Setting log level
#now_utc = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
#logging.basicConfig(level=logging.INFO,filename="log_prec_temp_merge_"+now_utc+".txt")

def temp_prec_mean(path_src, path_tgt):
    # Obtaining list of files
    files = []
    try:
        for f in os.listdir(path_src):
            if len(f.split("."))>1 and f.split(".")[1]=="csv":
                files.append(f)
    except Exception as e:
        logging.error(str(e))
        logging.exception(traceback.print_exc())

    # Obtaining data from last or last two files (if exist)
    if len(files)>0:
        try:
            files.sort(reverse=True) 
            df_file = pd.read_csv(path_src+files[0])        
        
            if len(files)>1:
                df_file2 = pd.read_csv(path_src+files[1])
                df_file = pd.concat([df_file,df_file2])

            cols = ['idmun','hloc','temp', 'prec']
            df_temp_prec = df_file[cols]

            temp_prec_mean_df = df_temp_prec.groupby('idmun').mean()
            save_file(path_tgt,temp_prec_mean_df)
        except Exception as e:
            logging.error(str(e))
            logging.exception(traceback.print_exc())
    else:
        logging.info("There is no data available in path:{}".format(path_src))
        return

    return temp_prec_mean_df


def merge_dfs(path_source,path_target,df):
    dirs = []
    try:
        for d in os.listdir(path_source):
            dirs.append(d)
    except Exception as e:
        logging.error(str(e))
        logging.exception(traceback.print_exc())

    if len(dirs)>0:
        dirs.sort(reverse=True) 
        try:
            df_data = pd.read_csv(path_source+dirs[0]+'/data.csv')  
        except Exception as e:
            logging.error(str(e))
            logging.exception(traceback.print_exc())
        
        df_data.rename(columns={"Cve_Mun": "idmun"}, inplace=True)
    else:
        logging.info("There is no data available to merge")

    result = pd.merge(df, df_data,how='inner', on=["idmun"])
    try:
        save_file(path_target,result)
        logging.info("merged_file saved")
        save_file(path_target,result,'current.csv')
        logging.info("current.csv file saved")
    except Exception as e:
        logging.error(str(e))
        logging.exception(traceback.print_exc())
    return


def main():
    logging.info("Obtaining temperature and precipitations mean table")
    relative_path_source = 'data/data_municipio_x_hora/'
    path_source = get_fullpath(relative_path_source)

    relative_path_target = 'data/data_temp_prec_mean/'
    path_target = get_fullpath(relative_path_target)
    
    df_temp_prec_mean = temp_prec_mean(path_source,path_target)

    if df_temp_prec_mean is not None:
        logging.info("Merging mean values with data")
        data_relative_path_source = 'data/data_municipios/'
        filepath_source = get_fullpath(data_relative_path_source)
    
        merged_relative_path_target = 'data/data_merged/'
        filepath_target = get_fullpath(merged_relative_path_target)
        merge_dfs(filepath_source,filepath_target,df_temp_prec_mean)
    else:
        logging.info("The merge was not done")
    logging.info("Process complete")


if __name__ == '__main__':
    main()



