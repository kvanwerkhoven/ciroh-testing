''' 
Collection of general administrative utilities to work with NWM data,
    including:
        - defining NWM configuration settings 
        - generating output filenames 
'''

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path


def nwm_version(ref_time):
    '''
    Get NWM version (2.0 or 2.1) corresponding to the reference time     
        - v2.0 assumed for all datetimes prior to 4-20-2021 13z
        - v2.1 begins 4-20 at 14z 
        - v2.2 begins 7-09 at 00z 
        - *code update would be needed for versions prior to 2.0
    '''    
    
    v21_date = datetime(2021, 4, 20, 14, 0, 0)
    v22_date = datetime(2022, 7, 9, 0, 0, 0)
    
    if ref_time >= v22_date:
        version = 2.2
    elif ref_time >= v21_date:
        version = 2.1
    else:
        version = 2.0
        
    return version
    
def nwm_version_dir(data_dir, check_date):
    '''
    Add a subdirectory to the local directory where NWM output netcdf files
    are stored to clearly differentiate files from different NWM versions
    '''

    version = nwm_version(check_date)
    if version == 2.2:
        version_dir = data_dir / 'v2_2'
    elif version == 2.1:
        version_dir = data_dir / 'v2_1'
    else:
        version_dir = data_dir / 'v2_0'
        
    return version_dir
    
    
def get_abbrev(config):
    '''
    Get a short abbrevation for NWM configuration for dataframe column headers
    and/or to use in figure titles or filenames as needed
    '''   
    
    if config == 'latest_ana':
        config = 'analysis_assim'
    
    abbrev = pd.Series(['srf','mrf','stana','exana'],
             index = ["short_range", "medium_range", "analysis_assim", "analysis_assim_extend"])
        
    return abbrev[config]
    
    
def get_config_abbrev(config):

    if config == 'analysis_assim_extend':
        config_abbrev = 'ExtAnA'
        
    elif config == 'analysis_assim':
        config_abbrev = 'StdAnA'
        
    elif config == 'latest_ana':
        config_abbrev = 'LatestAnA'
        
    elif config == 'short_range':
        config_abbrev = 'SRF'
        
    elif config == 'medium_range':
        config_abbrev = 'MRF'      
    
    return config_abbrev

    
def get_column_headers(config_list, metric = "", suffix = ""):
    '''
    Get column headers for evaluation dataframes with config abbrevation
    plus metric name, plus suffix if any
    '''

    col = []
    for i, config in enumerate(config_list):
    
        abbrev = get_abbrev(config)
    
        if metric:
            abbrev = abbrev + "_" + metric
        if suffix:
            abbrev = abbrev + "_" + suffix
            
        col.append(abbrev)

    return col

    
def check_if_forecast(config):
    '''
    Set a flag to indicate if an NWM configuration is a forecast or not (is an AnA). 
    This flag is needed in multiple functions, e.g. to correctly stitch together
    time series relative to a reference time.
    '''

    if config in ['short_range','medium_range','long_range']:
        is_forecast = True
    else:
        is_forecast = False
        
    return is_forecast   
    
    
def check_if_forecast_from_path(nwm_path):
    '''
    check if a nwm path is a file for a forecast or not (AnA)
    '''
    config = nwm_path.name.split(".")[2]
    is_forecast = check_if_forecast(config)
        
    return is_forecast
    
    
def config_specs(config, domain, version, member=1):
    '''
    Build a dataframe of NWM specifications for a defined configuration,
    accounting for differences between domains and versions, includng
        - dir_suffix:       suffix appended to configuration directory, e.g. _hawaii, _puertorico, _mem1
        - var_str_suffix    suffix appended to variable string in filename, e.g. channel_rt_1 
        - duration_hours    simulation period in hours
        - timestep_int      simulation timestep in hours (fraction if < 1 hour)
        - runs_per_day      number of executions per day
        - is_forecast       True if forecast, False if AnA
        - abbrev            short abbrevation for use in column headers and titles
    '''
    
    # if running latest_ana mode, start with standard AnA config
    if config == 'latest_ana':
        config = 'analysis_assim'
    
    # first build the base dataframe of config info for all configs
    # note that for medium range, assume only evaluating 'member 1' for now
    
    if domain == 'conus':
    
        df_config_specs = pd.DataFrame(
            {"dir_suffix" : ["", "_mem1", "", ""],
             "var_str_suffix" : ["","_1","",""],
             "duration_hrs" : [18, 240, 3, 28], 
             "timestep_int" : [1, 1, 1, 1], 
             "runs_per_day" : [24, 4, 24, 1],
             "base_run_hour" : [0, 0, 0, 16],
             "latency" : [1.5, 6, 0.5, 3],
             "is_forecast" : [True, True, False, False],
             "abbrev" : ['srf','mrf','stana','exana']},
            index = ["short_range", "medium_range", "analysis_assim", "analysis_assim_extend"])    
            
        if version < 2.1:
            # in v2.0, medium range time step was 3 hours, changed to 1 hour in v2.1
            df_config_specs.loc['medium_range','timestep_int'] = 3
            
        # for medium range members 2-7, change suffixes and duration 
        if config == 'medium_range' and member > 1:
            df_config_specs.loc['medium_range','duration_hrs'] = 204
            df_config_specs.loc['medium_range','dir_suffix'] = "_mem" + str(member)
            df_config_specs.loc['medium_range','var_str_suffix'] = "_" + str(member)    
            
    elif domain == 'hawaii':
    
        # exit if the configuration requested that does not exist
        if config in ['medium_range','long_range','analysis_assim_extend']:
            raise ValueError(f'Config {config} does not exist for domain {domain}') 
        
        # hawaii short range extends out 60 hours in 2.0 and 48 hours in 2.1
        # hawaii timesteps changes from 1 hour in 2.0 to 15 min in 2.1 (both SRF and AnA)
        # hawaii run interval changes from 6 hours (4 x day) in 2.0 to 12 hours (2 x day) in 2.1
        df_config_specs = pd.DataFrame(
            {"dir_suffix" : ['_' + domain, '_' + domain],
             "var_str_suffix" : ["",""],
             "duration_hrs" : [48, 3], 
             "timestep_int" : [0.25, 0.25], 
             "runs_per_day" : [2, 24],
             "base_run_hour" : [0, 0],
             "latency" : [1.5, 0.5],
             "is_forecast" : [True, False],
             "abbrev" : ['srf','stana']},
            index = ["short_range", "analysis_assim"])
            
        if version == 2.0:
            df_config_specs.loc[:,'timestep_int'] = [1, 1]
            df_config_specs.loc['short_range','duration_hrs'] = 60
            df_config_specs.loc['short_range','runs_per_day'] = 4                
        
    # puerotrico does not exist prior to v2.1
    elif domain == 'puertorico':

        # exit if the configuration requested that does not exist
        if config in ['medium_range','long_range','analysis_assim_extend']:
            raise ValueError(f'Config {config} does not exist for domain {domain}')

        if version < 2.1:
            raise ValueError(f'Domain {domain} does not exist for version {version}')      
            
        df_config_specs = pd.DataFrame(
            {"dir_suffix" : ['_' + domain, '_' + domain],
             "var_str_suffix" : ["",""],
             "duration_hrs" : [48, 3], 
             "timestep_int" : [1, 1], 
             "runs_per_day" : [2, 24],              # runs at 6 hr and 18 hr - code updates needed for this!
             "base_run_hour" : [6, 0],
             "latency" : [1.5, 0.5],
             "is_forecast" : [True, False],
             "abbrev" : ['srf','stana']},
            index = ["short_range", "analysis_assim"])
              
    # reduce dataframe to the row for specified configuration
    df_config_specs = df_config_specs.loc[[config]]
    
    return df_config_specs
    
    
def variable_specs(domain):
    '''
    Build a dataframe of filename specifications that differ by variable 
        dir_prefix:         prefix to the configuration directory, i.e., forcing_short_range
        use_suffix:         turn on or off dir_suffix, defined in config specs     
        var_string:         variable string in filename
        var_out_units       units of data
    '''
        
    # build dataframe of variable group info and processing flags
    df_var_specs = pd.DataFrame(
            {"dir_prefix" : ["forcing_", ""], 
             "use_suffix" : [False, True], 
             "var_string" : ["forcing", "channel_rt"],
             "var_out_units" : ["mm hr-1","cms"]},
            index = ["forcing", "channel"])
 
    # adjustments to base info for hawaii domain and version 
    #(for v2.1 will add other domain specifics, PR, AK)
    if domain == 'hawaii':
        
        # turn on flag to add domain suffix (defined in config specs)
        # for both variables (in conus used for medium term "mem1" suffix, 
        # med term does not exist for Hawaii and suffix instead indicates "hawaii"
        df_var_specs['use_suffix'] = [True, True]                          

    return df_var_specs
    

def synchronize_indexes(df1, df2, meta_columns = [], fill = 'nan'):
    '''
    Synchronize two dataframes so they have the same rows by adding rows
    and filling with missing/emtpy values
    '''
    
    df1 = synchronize_index_left(df1, df2, meta_columns, fill)
    df2 = synchronize_index_left(df2, df1, meta_columns, fill)
               
    return df1, df2

    
def synchronize_index_left(df_append_to, df_check, meta_columns = [], fill = 'nan'):
    '''
    Synchronize one dataframe with another so they have the same rows
    '''
    
    # indexes in df_check that are not in df_append_to
    df_add_indexes = df_check[~df_check.index.isin(df_append_to.index)].copy()

    # set values to NaN if NOT meta data
    for col in df_add_indexes.columns:
        
        if not col in meta_columns:
        
            if df_add_indexes.dtypes[col] == 'object':
                df_add_indexes[col].values[:] = None
            elif df_add_indexes.dtypes[col] in ['bool']:            
                df_add_indexes[col].values[:] = False                
            elif df_add_indexes.dtypes[col] in ['int64','int32']:
                if fill == 'zero':
                    df_add_indexes[col].values[:] = 0 
                else: #nan/missing
                    df_add_indexes[col].values[:] = -999    
            elif df_add_indexes.dtypes[col] in ['float64','float32']:    
                if fill == 'zero':
                    df_add_indexes[col].values[:] = 0.0
                else:
                    df_add_indexes[col].values[:] = np.nan

    # add the missing rows
    df_append_to = pd.concat([df_append_to, df_add_indexes]).sort_index()
    
    return df_append_to
    

def create_dir_if_not_exist(p: Path):
    '''
    Create a directory if does not yet exist
    '''
    if not p.exists():
        print(f"Directory does yet not exist. Creating {p}")
        p.mkdir(parents=True, exist_ok=True)   
        
        
def get_nwm_timestep_valtimes(ts_list):
    '''
    get valid times for nwm timesteps based on reference time and timestep sting
    ts_list is a list of timestep strings of format:  yyyymmddhhz-tmxx or yyyymmddhhz-fxxx
    '''

    valtimes = []

    # parse out reference times
    ref_time_list = [datetime.strptime(t[:10], '%Y%m%d%H') for t in ts_list]
    
    # parse out time step string
    ts_str_list = [t[-4:] for t in ts_list]
    
    # get the valid time based on timestep string
    for i, ts_str in enumerate(ts_str_list):

        if ts_str[0] == "f":
            ts = int(ts_str[1:])
            valtimes.append(ref_time_list[i] + timedelta(hours=ts))
            
        else:
            ts = int(ts_str[2:])
            valtimes.append(ref_time_list[i] - timedelta(hours=ts))
        
    return valtimes
    
    
def get_val_time_range(specs):

    '''
    Assumed for forecast config only:
    Get the bookend start and end valid time associated with a list of reference times
    i.e., first valid time of first ref time and last valid time of last ref time.
    '''

    if not type(specs.ref_time_list) == list:
        raise TypeError('ref_time_list is not a list')
    elif not specs.ref_time_list:
        raise ValueError('ref_time_list is empty')
        
    else:
        ref_start = specs.ref_time_list[0]   
        ref_end = specs.ref_time_list[len(specs.ref_time_list) -1]        
        
        df_config = config_specs(specs.fcst_config, specs.domain, specs.version)
        duration = df_config.loc[specs.fcst_config,"duration_hrs"].item()

        val_start = ref_start
        val_end = ref_end + timedelta(hours = duration)       
        
    return val_start, val_end
    

def get_val_time_range_per_ref_time(specs):

    '''
    Assumed for forecast config only:
    Get the start and end valid time associated with a single reference time
    and adjust if val_start/end defined.
    '''
   
    df_config = config_specs(specs.fcst_config, specs.domain, specs.version)
    duration = df_config.loc[specs.fcst_config,"duration_hrs"].item()

    val_start = specs.ref_time
    val_end = val_start + timedelta(hours = duration)       
    
    if specs.val_start != -999 and specs.val_start > val_start:
        val_start = specs.val_start

    if specs.val_end != -999 and specs.val_end > val_end:
        val_end = specs.val_end       
        
    return val_start, val_end

    
def get_val_time_range_any_config(specs, config):

    '''
    ANY CONFIG:
    Get the bookend start and end valid time associated with a list of reference times
    i.e., first valid time of first ref time and last valid time of last ref time.
    '''

    if not type(specs.ref_time_list) == list:
        raise TypeError('ref_time_list is not a list')
    elif not specs.ref_time_list:
        raise ValueError('ref_time_list is empty')
        
    else:
        ref_start = specs.ref_time_list[0]   
        ref_end = specs.ref_time_list[len(specs.ref_time_list) -1]        
        
        df_config = config_specs(config, specs.domain, specs.version)
        duration = df_config.loc[config,"duration_hrs"].item()

        val_start = ref_start
        val_end = ref_end + timedelta(hours = duration)       
        
    return val_start, val_end
    
    
def get_val_time_range_one_ref_time(specs, config):

    '''
    Get start and end valid time associated with a single reference times
    '''

    ref_start = specs.ref_time         
    
    df_config = config_specs(config, specs.domain, specs.version)
    duration = df_config.loc[config,"duration_hrs"].item()

    val_start = ref_start
    val_end = ref_start + timedelta(hours = duration)       
        
    return val_start, val_end