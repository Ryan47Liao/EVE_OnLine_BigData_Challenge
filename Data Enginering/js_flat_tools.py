# killmail_id = 51068899.0
# victim_character_id = 93247997.0

import os
import pandas as pd
import json
import tqdm
import numpy as np
from multiprocessing import Process
import multiprocessing
from functools import reduce
import datetime

def df_slicer(df,nslice):
    """_summary_
    divides a data frame into n equally distributed slices 
    Args:
        df (pd.DataFrame): the data frame to be sliced 
        nslice (int): number of slices to be divied into 

    Returns:
        [*pd.DataFrame]: a list of dataframes seperated into n slices
    """
    from math import ceil
    n_rows = df.shape[0]
    scope = ceil(n_rows/nslice)
    out = []
    head = 0
    tail = head + scope 
    while True:
        #print(f'{head}->{tail}')
        out.append(df[head:tail])
        head = tail 
        tail = head + scope 
        if len(out) ==  nslice:
            break
    return out 

def set_ids(dct,killmail_id,victim_character_id):
    dct['killmail_id'] = killmail_id
    dct['victim.character_id'] = victim_character_id
    return dct 

def flatten_js(item_sample,killmail_id,victim_character_id):
    return pd.json_normalize(list(map(lambda x: set_ids(x,killmail_id,victim_character_id),eval(item_sample))))

def FLAT_JS(df,hash,out_dct = None,target_column = 'victim.items'):
    out_df = pd.DataFrame()
    for idx,row in tqdm.tqdm(df.iterrows()):
        JS = row[target_column]
        killmail_id = row['killmail_id']
        victim_character_id = row['victim.character_id']
        df_temp = flatten_js(JS,killmail_id,victim_character_id)
        out_df = pd.concat([out_df,df_temp])
    if out_dct is None:
        return out_df
    out_dct[hash] = out_df
    
    
if __name__ == '__main__':
    assert False,'Multiprocessing has no effect reducing computation time,this unit test is abandoned'
    rows_to_keep = range(0,1000)
    df_wars = pd.read_csv('E:\Data\Eve-Online\WARS.csv',index_col=0,
                          skiprows = lambda x: x not in rows_to_keep)
    ###########
    start = datetime.datetime.now()
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    n_jobs = 4

    threads = []
    hash_id = 0
    for df_slice in df_slicer(df_wars,n_jobs):
        hash_id +=1 
        process = Process(target=FLAT_JS, args=(df_slice,hash_id,return_dict))
        threads.append(process)
        process.start()
        
    ####EXECUTE####
    [t.join() for t in threads]
    result = reduce(lambda x,y:pd.concat([x,y]),list(return_dict.values()))
    
    end = datetime.datetime.now()
    print(f'Job with {n_jobs} process took {end-start}')
    result.to_csv('E:\Data\Eve-Online\ITEMS.csv',index=False)