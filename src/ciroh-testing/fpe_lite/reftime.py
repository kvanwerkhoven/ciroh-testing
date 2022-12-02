''' 
Utilities to find/set forecast reference time or list of reference times
based on various operating specifications
'''    
    
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path    
    
from . import admin
    
def get_reftime_list(specs):
    '''
    get the list of reference times in the specified range, accounting for differences in issue frequency
    between configurations and domains, and timing parameter of the evaluation
    '''

    # For near-real-time evaluations, get the start/end reference times that are feasible to evaluate 
    if specs.eval_timing == 'current':
        specs.ref_start, specs.ref_end = get_realtime_eval_daterange(specs)
        
        # If storing results for multiple periods (i.e., 3-day and 5-day for MRF), adjust ref times and get the 
        # adjusted list of ref times
        if specs.store_periods:
            specs.ref_start, specs.ref_end, period_ref_times = adjust_ref_time_limits(specs) 
        
            if specs.verif_config == 'analysis_assim_extend':
                ref_time_list = reftimes_in_range(specs, specs.fcst_config)
            elif specs.verif_config == 'latest_ana':
                # If 'latest_ana', executing a single, most recent evaluation per store_period   
                ref_time_list = get_nearest_ref_times(specs, specs.fcst_config, period_ref_times)
                
        else:
            ref_time_list = reftimes_in_range(specs, specs.fcst_config)

    else:
        # get the list of reference times in the specified range, accounting for differences in issue frequency
        # between configurations and domains
        ref_time_list = reftimes_in_range(specs, specs.fcst_config)

    return ref_time_list    


def reftimes_in_range(specs, config, eval_hr = -999):
    '''
    Generate a list of reference times that exist within a defined range for a specified
    domain and forecast configuration of the NWM
    '''
    
    ref_start = specs.ref_start
    ref_end = specs.ref_end

    if eval_hr >= 0:
        ref_start = ref_start - timedelta(hours = ref_start.hour) + timedelta(hours = eval_hr)
        ref_end = ref_end - timedelta(hours = ref_end.hour) + timedelta(hours = eval_hr)
                                                        
    version = admin.nwm_version(ref_start)
    version_end = admin.nwm_version(ref_end)

    if version != version_end:
        raise ValueError('Date range spans different NWM versions - not yet supported')
         
    # get some needed specs for the config
    df_config = admin.config_specs(config, specs.domain, version)
    runs_per_day = df_config.loc[config,"runs_per_day"].item()
    n_hours = df_config.loc[config,"duration_hrs"].item()     

    # do some more checks if this hour is available - varies by domain and config
    # then build the list
    ref_time_list = build_reftime_list(specs, ref_start, ref_end, config, order = 'ascending')  

    # if ref_time_list returns empty - no forecasts were run on the specified reference time
    # e.g. Hawaii runs only every 6 hours (v2.0)

    if not ref_time_list:
        print(f'\nNo reference times are available to evaluate for datetime: {ref_start}')
        return [], []
    else:
        # if evaluating a specified ref time each day only (as for AAR), pull those out of the list
        if eval_hr >= 0:
            ref_time_list = [ref for ref in ref_time_list if ref.hour == eval_hr]            
            
        min_date = ref_time_list[0]
        max_date = ref_time_list[len(ref_time_list) - 1] + timedelta(hours=n_hours)  
          
    return ref_time_list
       
       
def get_nearest_ref_times(specs, config, ref_time_list, time_direction = 'before'):
    '''
    Generate a list of reference times nearest to a date list in a specified
    direction (before/after) based on domain and forecast configuration of the NWM
    '''
                                                        
    version = admin.nwm_version(ref_time_list[0])
    version_end = admin.nwm_version(ref_time_list[len(ref_time_list)-1])

    if version != version_end:
        raise ValueError('Date range spans different NWM versions - not yet supported')
         
    # get some needed specs for the forecast configuration (SRF, MRF)
    df_config = admin.config_specs(config, specs.domain, version)
    duration = df_config.loc[config,"duration_hrs"].item()
    runs_per_day = df_config.loc[config,"runs_per_day"].item()   
    interval = 24/runs_per_day    
    
    ref_time_list_shifted = []
    for ref_time in ref_time_list:
        
        if time_direction == 'before':
            check_start = ref_time - timedelta(hours=interval)  
            check_end = ref_time
            ref_time_list_check = build_reftime_list(specs, check_start, check_end, config, order = 'ascending')  
            ref_time_shift = max(ref_time_list_check)
            
        elif time_direction == 'after':
            check_start = ref_time
            check_end = ref_time + timedelta(hours=interval)  
            ref_time_list_check = build_reftime_list(specs, check_start, check_end, config, order = 'ascending')  
            ref_time_shift = min(ref_time_list_check)            
        
        ref_time_list_shifted = ref_time_list_shifted + [ref_time_shift]

    return ref_time_list_shifted

    
def get_realtime_eval_daterange(specs):
    '''
    Get the date or date range of forecast reference time(s) of most recent possible 
    forecasts that can be evaluated based on current clock time and when the verifying configuration
    is available
    
    Assumes:
        - NWM Standard AnA are available 2 hours after the reference time
        - NWM Extended AnA (16z ref time) are available at 19z each day
    '''

    # get current clock utc time (top of the last hour) 
    clock_ztime = datetime.utcnow().replace(second=0, microsecond=0, minute=0)
    
    # get the NWM version based on most recently posted SRF
    version = admin.nwm_version(clock_ztime - timedelta(hours=2))
         
    # get some needed specs for the forecast configuration (SRF, MRF)
    df_config = admin.config_specs(specs.fcst_config, specs.domain, version)
    duration = df_config.loc[specs.fcst_config,"duration_hrs"].item()
    runs_per_day = df_config.loc[specs.fcst_config,"runs_per_day"].item()

    if specs.verif_config == 'latest_ana' or specs.verif_config == 'analysis_assim':
        # to update every hour (evaluate most recent possible forecast)
        # get the best available AnA - will be a mix of extended and standard, 
        # depending on clock time   
    
        # evaluate the most recent-possible reftime (clock time - 1 - duration) 
        # (e.g. 18 hrs of the forecast plus 1 hrs due to AnA latency)
        last_reftime = clock_ztime - timedelta(hours=1)
        fcst_reftime = last_reftime - timedelta(hours=duration)
        ref_start = fcst_reftime
        ref_end = fcst_reftime
    
    elif specs.verif_config == 'analysis_assim_extend':        
        # to update as often as possible but using only extended AnA, 
        # get current clock time and check the current hour, 
        # if after 19z clock time, the 16z extended AnA for current date should be available.
        # 
        #   Once today's extended AnA becomes available, any forecast that includes a valid time
        #   between 17z yesterday to 16z today can be now be evaluated.  This includes forecasts
        #   with reference times beginning 23z two days ago (last timestep valid time is 17z yesterday)
        #   through 22z yesterday (last time step is 16z today)
        #   Also rerun the 4 prior reftimes given updated Stage IV in overlapping tm27-24
        # In total, run evaluations for clock time T-48 to T-21 hours
        
        if clock_ztime.hour >= 19:
            # current extAnA available
            last_reftime = clock_ztime.replace(hour=16, minute=0)
        else:
            # use yesterdays extana
            last_reftime = clock_ztime - timedelta(days=1)
            last_reftime = last_reftime.replace(hour=16, minute=0)
            
        ref_start = last_reftime - timedelta(hours=duration + 27)
        ref_end = last_reftime - timedelta(hours=duration)

    print('\nCurrent UTC time: ', clock_ztime)
    print('Last posted reference time: ', last_reftime)
   
    return ref_start, ref_end




def get_recent_reftimes(specs, config, hours_back = 24, use_latency = False):
    '''
    Get a list of reference time(s) for a defined number of hours backward in time to current time,
    list is return in order from most recent to oldest
    '''

    # get current clock utc time (top of the last hour) 
    clock_ztime = datetime.utcnow().replace(second=0, microsecond=0)#, minute=0)    
    
    # get NWM version based on the top of the hour
    version = admin.nwm_version(clock_ztime.replace(minute = 0))
    
    # get NWM config specs
    df_config = admin.config_specs(config, specs.domain, version)
    runs_per_day = df_config.loc[config,"runs_per_day"].item()
    latency = df_config.loc[config,"latency"].item()
    
    interval = 24/runs_per_day

    # get all viable reference times going back defined # hours through current clock time
    # if requested # of hours is < the run interval, go back at least 1 run interval to find
    # a valid reference time
    if hours_back < interval:
        hours_back = interval
    
    if use_latency:
        end_check = (clock_ztime - timedelta(hours=latency)).replace(minute=0)
        hours_back = latency + hours_back
    else:
        end_check = clock_ztime
        
    # check for a valid reference times from start_check and current time
    start_check = (clock_ztime - timedelta(hours=hours_back)).replace(minute=0)
    
    # do some more checks if this hour is available - varies by domain and config
    # then build the list
    ref_time_list = build_reftime_list(specs, start_check, end_check, config, order = 'descending') 

    print('\nCurrent UTC time: ', clock_ztime)
    print('End check hour: ', end_check)
    print('Start check hour: ', start_check)
    print('Most recent viable', config, 'reference time: ', ref_time_list[0])
   
    return ref_time_list    
    

def get_most_recent_ana_valtime(specs):
    '''
    Find the reference time of the most recently-issued NWM AnA simulation with respect
    to current clock time.
    
    Assumes:
        - NWM Standard AnA are available 2 hours after the reference time
        - NWM Extended AnA (16z ref time) are available at 19z each day
    '''

    clock_ztime = datetime.utcnow().replace(second=0, microsecond=0)

    if specs.verif_config == 'latest_ana' or specs.verif_config == 'analysis_assim':
        last_ana_valtime = clock_ztime - timedelta(hours=1)
        last_ana_valtime = last_ana_valtime.replace(minute=0)
        
    elif specs.verif_config == 'analysis_assim_extend':   
        if clock_ztime.hour >= 19:
            last_ana_valtime = clock_ztime.replace(hour=16, minute=0)
        else:
            # use yesterdays extana
            last_ana_valtime = clock_ztime - timedelta(days=1)
            last_ana_valtime = last_ana_valtime.replace(hour=16, minute=0)     

    return last_ana_valtime

def adjust_ref_time_limits(specs): 
    '''
    If the analysis is being performed for less than the full duration (e.g., first 5 days of MRF)
    adjust the last reference time on the list due to different duration
    '''

    # get current clock utc time (top of the last hour) 
    clock_ztime = datetime.utcnow().replace(second=0, microsecond=0)#, minute=0)    
    
    # get NWM version based on the top of the hour
    version = admin.nwm_version(clock_ztime.replace(minute = 0))
    
    # get NWM config specs
    df_config = admin.config_specs(specs.fcst_config, specs.domain, version)
    
    # get duration in days
    duration = df_config.loc[specs.fcst_config,"duration_hrs"].item() / 24
    
    period_ref_end = []
    for period in specs.store_periods:
        
        if period < duration:     
            period_ref_end = period_ref_end + [pd.Timestamp(specs.ref_end) + timedelta(days=(duration - period))]
        else:
            period_ref_end = period_ref_end + [pd.Timestamp(specs.ref_end)]   
            
    adj_ref_end = max(period_ref_end)

    # if the full duration is not being analyzed, adjust the start date accordingly
    adj_ref_start = specs.ref_start
    max_period = max(specs.store_periods)
    if max_period < duration:
        adj_ref_start = specs.ref_start + timedelta(days=(duration - max_period))
            
    return adj_ref_start, adj_ref_end, period_ref_end
   
    
def build_reftime_list(specs, ref_start, ref_end, config, order = 'ascending'):
    '''
    get a list of forecast reference times within date range based on configuration and domain
    since not all configs/domains (med-term, hawaii) are run every hour
    '''
    
    version = admin.nwm_version(ref_start)
    
    # first do some checks
    if ref_end < ref_start:
        raise ValueError('Start date after end date')
        
    if specs.domain == 'hawaii' and config == 'medium_range':
        raise ValueError('Domain and configuration not compatible') 
        
    if version < 2.0 or version > 2.2:
        raise ValueError('Version must be 2.0, 2.1, or 2.2')
        
    df_config = admin.config_specs(config, specs.domain, version)
    runs_per_day = df_config.loc[config,"runs_per_day"].item()
    base_run_hour = df_config.loc[config,"base_run_hour"].item()

    # the reference time interval
    # *Note currently in all cases the NWM always runs at fixed intervals 
    #     beginning at 'base_run_hour'
    # if this changes, will need to update code
    interval = 24/runs_per_day
    
    # if runs_per_day is < 24, check that the starting reftime falls on an hour that exists
    # if not, shift the start time forward to the first existing reference time
    if base_run_hour == 0 and runs_per_day < 24:
        offset = ref_start.hour % interval
        if offset > 0:
            shift_forward = interval - ref_start.hour % interval
            ref_start = ref_start + timedelta(hours=shift_forward)
            
    elif base_run_hour > 0 and runs_per_day == 1:
        
        if ref_start.hour <= base_run_hour:
            ref_start = ref_start.replace(hour = base_run_hour)
        else:
            ref_start = ref_start.replace(hour = base_run_hour) + timedelta(days = 1)
            
    # recheck that start is before the end, if not return empty list
    if ref_end < ref_start:
        return []
                
    # create the list of reference times at the correct interval 
    ref_time_list = pd.date_range(start=ref_start, end=ref_end, freq= str(interval) + 'H').to_list()   
        
    if order == 'descending':
        ref_time_list.reverse()
     
    return ref_time_list
    