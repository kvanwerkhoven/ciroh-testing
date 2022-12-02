from hydrotools.nwm_client import gcp as nwm
from os import cpu_count
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
from datetime import datetime, timedelta
import warnings

from fpe_lite import admin, files

def ht_dataframe_setup(
    df, 
    config, 
    ht_service
    ) -> pd.DataFrame:
    '''
    set up dataframe in hydrotools format
    (cannot use hydrotools directly to fetch single timesteps of AnA, 
    so copying some chunks of code needed)
    '''

    # Rename
    df = df.rename(columns={'streamflow': 'value'})

    # Reformat crosswalk
    xwalk = ht_service.crosswalk

    # Additional columns
    xwalk['configuration'] = config
    xwalk['measurement_unit'] = 'm3/s'
    xwalk['variable_name'] = 'streamflow'

    # Apply crosswalk metadata
    for col in xwalk:
        df[col] = df['nwm_feature_id'].map(xwalk[col])

    # Categorize
    df['configuration'] = df['configuration'].astype("category")
    df['measurement_unit'] = df['measurement_unit'].astype("category")
    df['variable_name'] = df['variable_name'].astype("category")
    df['usgs_site_code'] = df['usgs_site_code'].astype("category")

    # Sort values
    df = df.sort_values(
        by=['nwm_feature_id', 'value_time'],
        ignore_index=True
        )   
    
    return df
    
    
def get_recent_ana(
    specs,
    ht_service,    
    t0 = datetime(2020, 6, 2, 0),
    bias_period = 10, 
    no_da = False, 
    df = pd.DataFrame()
    ) -> pd.DataFrame:

    '''
    Get ana data for specified 
    '''

    ref_back = t0 - timedelta(days = bias_period)
    max_proc = max((cpu_count() - 2), 1)            

    if no_da:
        ext = "_noda_baseline"
        config = specs.ana_config + "_no_da"
    else:
        ext = "_baseline"
        config = specs.ana_config
                
    # define HDF5 store
    group = admin.get_abbrev(specs.ana_config) + ext
    store = pd.HDFStore(specs.data_dir / (group + ".h5"))
       
    # get list of ana timesteps (only need tm02 per reftime)
    filelist = files.build_one_filelist(specs.domain, 'channel', specs.ana_config,
                                         val_start = ref_back, val_end = t0, 
                                         use_no_da = no_da)
    
    # get the list of valid times of the timesteps needed for the bias period
    df_parsed_filelist = files.parse_nwm_filelist(filelist)
    val_times_needed = set(df_parsed_filelist['val_time'])  
    
    # get list of valid times already in the df
    # and keep only valid times needed in the df
    if not df.empty:
        val_times_in_df = set(df['value_time'])
        df = df[df['value_time'].isin(list(val_times_needed))].copy()              
    else:
        val_times_in_df = set()    

    
    # get the filelist for timesteps not in the dataframe thus need to be fetched    
    val_times_fetch = sorted(list(val_times_needed.difference(val_times_in_df)))
    fetch_file_indices = [list(df_parsed_filelist['val_time']).index(t) for t in val_times_fetch]
    fetch_filelist = [filelist[i] for i in fetch_file_indices]    
    print(f'    # {config} timesteps already in dataframe: {len(filelist) - len(fetch_filelist)}')

    # fetch new data from cache or google cloud 
    # (will usually be a list of 1, except first fcst reftime)
    for file in fetch_filelist:
        
        file_str = "/".join(file.parts)
         
        # ref time this timestep
        ref_ts, t, c = files.parse_nwm_path(file)
                                                                              
        # HDF5 keys this AnA timestep's associated ref_time/issue time
        key = f'/{group}/{config}/DT{ref_ts.strftime("%Y%m%dT%HZ")}'     
    
        # first check the cache
        if key in store:
            print("     reading from cache: ", file_str)
            df_ts = store[key]
            df = pd.concat([df, df_ts])
            df = df.sort_values(
                        by=['nwm_feature_id', 'value_time'],
                        ignore_index=True
                        ) 
        else:
        
            try:
                # Retrieve data from the cloud
                print("     fetching", file_str)
                with ProcessPoolExecutor(
                    max_workers=max_proc) as executor:
                    dataframes = executor.map(
                        ht_service.get_DataFrame, 
                        [file_str],
                        chunksize=1
                        )
                    
                # Concatenate data
                df_ts = pd.concat(dataframes)
                
                # Setup the rest
                df_ts = ht_dataframe_setup(df_ts, config, ht_service)  
                
                # write to cache
                store.put(
                    key = key,
                    value = df_ts,
                    format = 'table',
                )
                
                df = pd.concat([df, df_ts])
                df = df.sort_values(
                            by=['nwm_feature_id', 'value_time'],
                            ignore_index=True 
                            )                
                
            except:
                warnings.warn("Data retreival failed")
      
    return df
    
