'''
Script to generate rudimentary bias-corrected NWM forecasts at gage location
'''

from pathlib import Path
from datetime import datetime
import time
import pandas as pd

from fpe_lite import specs, admin
from bias import bias
from hydrotools.nwm_client import gcp as nwm


# local data cache directory, create if it does not exist
data_dir = Path().absolute() / 'cache'                         
admin.create_dir_if_not_exist(data_dir)

# first and last reference times to process (will include all in between)
ref_start = datetime(2022, 5, 30, 16, 0, 0)
ref_end = datetime(2022, 6, 30, 16, 0, 0)

# forecast and AnA configurations
fcst_config = 'short_range'
ana_config = 'analysis_assim'

# bias calculation period - # days prior to each reference time
bias_period = 10

# set up various evaluation spec defaults, generate reference time list
specs = specs.FcstEvalSpecs(locals())

# set up hydrotools data service, initialize cache for baseline forecasts within the service
cache_group = admin.get_abbrev(fcst_config) + "_baseline"
ht_service = nwm.NWMDataService(cache_group = cache_group, cache_path = data_dir / (cache_group + ".h5"))

# set up cache for bias corrected forecasts
bias_group = admin.get_abbrev(fcst_config) + "_biascorr" + str(bias_period)
bias_store = pd.HDFStore(specs.data_dir / (bias_group + ".h5"))


#######  Begin processing

t = time.time()

# initialize AnA dataframes (fcst dataframe set up by hydrotools)
df_ana = pd.DataFrame()
df_ana_noda = pd.DataFrame()

for ref_time in specs.ref_time_list:

    print(f"\n---Processing reference time {ref_time}---")    
    
    t1 = time.time()

    # reference time string in hydrotools format
    ref_str = ref_time.strftime("%Y%m%dT%HZ")
    
    # get forecast for current ref time
    print(f'Fetching {fcst_config} {ref_time}')    
    df_fcst = ht_service.get(
        configuration = fcst_config,
        reference_time = ref_str
        )     
    
    # get standard AnA (is obs data at gages) for defined bias calculation period 
    #  (# days prior to ref time)
    # - first cycle will fetch the full period, each cycle after
    #   update the dataframe only with new timesteps needed
    #   i.e., add 1 (or 6) new timesteps and drop one off the back
    print(f'Fetching/updating past {bias_period} days {ana_config}') 
    df_ana = bias.get_recent_ana(specs, 
                                 ht_service,
                                 ref_time, 
                                 bias_period = bias_period, 
                                 no_da = False,
                                 df = df_ana,
                                 )
    # get standard AnA open loop for bias defined period
    # (unassimilated model output needed for bias calcs)
    config = ana_config + '_no_da'
    print(f'Fetching/updating past {bias_period} days {config}')
    df_ana_noda = bias.get_recent_ana(specs, 
                                 ht_service,
                                 ref_time, 
                                 bias_period = bias_period, 
                                 no_da = True,
                                 df = df_ana_noda,
                                 )
    
    # calculate the bias-corrected forecast
    
    # confirm the ana and no_da data locations/dates are identical 
    compare_columns = ['nwm_feature_id','reference_time','value_time']
    ana_aligned = df_ana_noda[compare_columns].equals(df_ana[compare_columns])
    
    # if aligned, bias correct
    if ana_aligned:
        
        print(f'     Calculating bias corrected forecast')
        
        df_bias = df_ana[compare_columns].copy()
        df_bias['bias'] = df_ana_noda['value'] - df_ana['value']
        df_mean_bias = df_bias.groupby('nwm_feature_id').mean()   

        df_fcst_bias_corr = df_fcst.merge(df_mean_bias, how = 'left', on = 'nwm_feature_id')
        df_fcst_bias_corr['value'] = df_fcst_bias_corr['value'] - df_fcst_bias_corr['bias']
        
        # write to cache        
        bias_store = pd.HDFStore(specs.data_dir / (bias_group + ".h5"))
        key = f'/{bias_group}/{fcst_config}/DT{ref_str}' 
        print(f'     Write to cache: {key}')
        bias_store.put(
                key = key,
                value = df_fcst_bias_corr,
                format = 'table',
        )          

    # if data do not align, skip the reftime - may happen if no_da data did not post, etc.
    else:
        print('data issue, skipping this reference time')