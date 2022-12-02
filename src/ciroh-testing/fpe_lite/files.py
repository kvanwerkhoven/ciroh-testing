''' 
Collection of utilities to work with NWM raw data files for evaluations.  
Including generating lists of NWM files needed to build timeseries
'''

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from . import admin, reftime, raw


def get_filelists(specs, variable, i = 0):
    '''
    create a dictionary of filelists for all requested evaluations, break into chunks if necessary
    '''

    config_list = [specs.fcst_config, specs.verif_config]
    
    # when processing only a portion of the forecast, need to pass the partial duration into filelist methods
    # for current eval_timing and latest_ana mode
    if specs.fcst_config == 'medium_range' and specs.verif_config == 'latest_ana' and specs.eval_timing == 'current':
        # for MRF with partial periods defined, there will be one reftime per store period, so reftime[i] matches store_period[i]
        partial_duration = specs.store_periods[i]
    else:
        partial_duration = -999
    
    # now get the list(s) of NWM output files needed for this evaluation
    df_filelists = build_df_filelists(specs.domain, [variable], config_list, [specs.ref_time],
                                            ana_align_to = specs.fcst_config, include_t0 = True, 
                                            partial_duration = partial_duration)
    
    # If reading in more than 24 timesteps (e.g. med range), break filelist into chunks
    # to avoid memory issues (n = chunk size in hours)
    df_filelists, n_chunks = get_filelist_chunks_by_df(df_filelists, n = 24)
    
    # create df_filelist for each chunk and store in dictionary
    dict_filelists = {}
    dict_filelists_keep = {}
    
    if n_chunks > 1:
        for chunk in range(n_chunks):
        
            f = [df_filelists.loc[i,'filelist_chunks'][chunk] for i in df_filelists.index]

            df_filelists_chunk = df_filelists.loc[:,:'variable']
            df_filelists_chunk['filelist'] = f
            
            # check that the dates do not extend beyond the most recent AnA time
            last_ana_valtime = reftime.get_most_recent_ana_valtime(specs)
            max_list_time = parse_nwm_filelist(f[1])['reftime'].max()
            
            # if the date list in this chunk is feasible, keep it
            if max_list_time <= last_ana_valtime:
            
                # key the dictionary from 1 to nchunks (1-based indexing)
                dict_filelists[chunk + 1] = df_filelists_chunk
                
        # remove any pointless chunks to save processing time - e.g. there is no point processing day 4 
        # if not storing it and if day 5 cannot be processed because it goes beyond current clocktime
        included_periods = [p for p in specs.store_periods if p in dict_filelists.keys()]
        if included_periods:
            max_included_period = max(included_periods)
            for key in dict_filelists.keys():
                if key <= max_included_period:
                    dict_filelists_keep[key] = dict_filelists[key]       
        else:
            raise ValueError('None of the requested analysis periods are feasible for this reference time')
            
    else:
        dict_filelists_keep[1] = df_filelists        
       
    return dict_filelists_keep


def get_filelists_fcsts(specs, variable, i = 0):
    '''
    create a dataframe of filelists for all requested evaluations
    '''

    config_list = [specs.fcst_config, specs.verif_config]
    
    # when processing only a portion of the forecast, need to pass the partial duration into filelist methods
    # for current eval_timing and latest_ana mode
    if specs.fcst_config == 'medium_range' and specs.verif_config == 'latest_ana' and specs.eval_timing == 'current':
        # for MRF with partial periods defined, there will be one reftime per store period, so reftime[i] matches store_period[i]
        partial_duration = specs.store_periods[i]
    else:
        partial_duration = -999
    
    # now get the list(s) of NWM output files needed for this evaluation
    df_filelists = build_df_filelists(specs.domain, [variable], config_list, [specs.ref_time],
                                            ana_align_to = specs.fcst_config, include_t0 = True, 
                                            partial_duration = partial_duration)
    
 
def build_df_filelists(domain, var_list, config_list, ref_time_list, val_start = -999, val_end = -999, 
                       ana_align_to = 'none', include_t0 = False, use_no_da = False, partial_duration = -999):
    '''
    Create a dataframe of filelists and configuration specs for multiple configurations
    Dataframe columns:
        - ref_time      reference time
        - config        NWM configuration (or 'latest_ana')
        - variable      variable, channel or forcing
        - filelist      list of all filenames for the ref time and configuration
        - list_start    first datetime in the filelist
        - list_end      last datetime in the filelist
        
    Parameters:
    domain:             NWM geographic domain
    var_list:           list of variables to generate filelists
    config_list:        list of configurations to generate filelists
    ref_time_list:      list of reference times to generate filelists
    val_start:          first valid time to include in the list from each simulation, if -999 include all timesteps
    val_end:            last valid time to include in the list from each simulation, if -999 include all timesteps
    ana_align_to:       if ana config, define which forecast configuration timesteps to align to, if any
    include_t0:         if forecast, include T0 or not
    use_no_da:          if ana, use the no_da version or not
    '''

    # initialize filelist dataframe
    df_filelists = pd.DataFrame()
     
    # loop through lists of reference times, configurations, and variables generating
    # generating filelists 
    print('')
    ind = 0
    for ref_time in ref_time_list:
        for config in config_list:
            for variable in var_list:

                filelist = build_one_filelist(domain, variable, config, ref_time = ref_time, 
                                              val_start = val_start, val_end = val_end, 
                                              ana_align_to = ana_align_to,
                                              include_t0 = include_t0,
                                              use_no_da = use_no_da,
                                              partial_duration = partial_duration)

                # append the list to dataframe if not empty
                if filelist:
                
                    df_filelists = df_filelists.append(pd.DataFrame({'ref_time' : ref_time,
                                                                     'config' : config, 
                                                                     'variable' : variable, 
                                                                     'filelist' : [filelist]},
                                                                      index = [ind]))
                else:
                    print('   ! No timesteps found within the event range for forecast: ',ref_time)
            
                ind += 1
        
    return df_filelists  
    
    
def build_one_filelist(domain, variable, config, ref_time = -999, val_start = -999, val_end = -999,
                       ana_align_to = 'none', include_t0 = False, use_no_da = False, partial_duration = -999):
    '''
    Build a single filelist based on single domain, variable, config, and reftime
    See build_df_filelists for description of parameters
    '''
                       
    # get version based on ref_time
    if ref_time != -999:
        version = admin.nwm_version(ref_time)
    elif val_start != -999:
        version = admin.nwm_version(val_start)
    else:
        raise ValueError(f"no valid date:  ref time = {ref_time} and valid time = {val_start}")
     
    # if config = 'latest_ana' (mix of std and ext ana) use standard ana to get configuration settings
    # but set flag (orig_config) to trigger 'latest_ana' method for AnA timestep selection  
    passed_config = config   
    if config == 'latest_ana':
        config = 'analysis_assim'

    # for Std AnA, flag to use tm02 rather than tm00 when piecing together time series
    # (hard-coding for now until becomes apparent it needs to be an argument)
    use_tm02 = True
        
    # get base dataframe of config and variable info
    df_config = admin.config_specs(config, domain, version)
    df_var = admin.variable_specs(domain)

    # base configuration directory prefix (e.g. 'forcing')
    dir_prefix = df_var.loc[variable, 'dir_prefix']
    
    # variable string used in filename ('forcing' or 'channel_rt' for now)
    var_string_stem = df_var.loc[variable, 'var_string']    

    # base configuration directory suffix (e.g. 'mem1' for medium_range ensemble member 1)    
    dir_suffix = df_config.loc[config, 'dir_suffix']

    # suffix at the end of the variable name (e.g. '1' for medium_range ensemble mem1)
    var_str_suffix = df_config.loc[config, 'var_str_suffix']

    # flag to trigger using suffixes or not (for forcing or hawaii)
    use_suffix = df_var.loc[variable, 'use_suffix']   
    
    # get duration, time interval and whether it is a forecast 
    # (flag for "f" versus "tm")
    is_forecast = df_config.loc[config,"is_forecast"]    
    ts_int = df_config.loc[config,"timestep_int"].item()
    n_hours = df_config.loc[config,"duration_hrs"].item()
    
    if partial_duration > 0:
        n_hours = partial_duration * 24

    # initialize the datedir (and ref_time if none provided)
    if ref_time == -999:
       ref_time = val_start 
    datedir = ref_time.strftime("nwm.%Y%m%d")  # ref date directory   
      
    # Set date range if not defined for AnA, set to
    # to the duration of the duration_config
    if not is_forecast and val_start == -999:
        # for ana, if val_start/end are -999, dates are based on 'ana_align_to' forecast config
        # if 'ana_align_to' is not set, default to short range
        if ana_align_to == 'none':
            ana_align_to = 'short_range'          
        n_hours = admin.config_specs(ana_align_to, domain, version).loc[ana_align_to,"duration_hrs"].item()
        
        if partial_duration > 0:
            n_hours = partial_duration * 24
   
    # if forecast, run the forecast fileparts function   
    if is_forecast:
        df_parts, list_start, list_end = build_forecast_fileparts(ref_time, config, n_hours, ts_int, 
                                                                  include_t0, val_start, val_end)                                                    
    
    # if AnA, fileparts function varies based on whether or not a valid time range is defined (val_start/end = -999)
    else:
        # if no valid time range is defined (val_start/end = -999), dates are selected based on 'ana_align_to' forecast duration
        if val_start == -999:
            # if 'ana_align_to' is not set, default to short range to get duration
            if ana_align_to == 'none':
                ana_align_to = 'short_range'          
                n_hours = admin.config_specs(ana_align_to, domain, version).loc[ana_align_to,"duration_hrs"].item()  
                
                if partial_duration > 0:
                    n_hours = partial_duration * 24

            # get filename parts for an ana config aligned to a specified forecast config and ref time
            df_parts, list_start, list_end = build_ana_fileparts_aligned_to_forecast(ref_time, passed_config, n_hours, ts_int, include_t0, domain)
            
        else:
            # if valid time range was defined, get filename parts for ana config within the valid range
            df_parts, list_start, list_end = build_ana_fileparts_defined_range(val_start, val_end, ts_int, passed_config, domain)
  
    # console messages 
    if val_start != -999:
        print ("Building file list for", config, variable,"| reference time:", ref_time, "| valid time range:", val_start, "to", val_end)
    else:
        print ("Building file list for", config, variable,"| reference time:", ref_time, "| all timesteps")          
       
    # loop through fileparts dataframe, add prefix/suffixes, and build filenames
    # include each file in the list only if it's valid time falls within the requested range (if range defined)
    filelist = []
    count = 0
    for index, row in df_parts.iterrows():
        
        # add prefix and suffixes as needed
        config_dir = dir_prefix + row['config']
        var_string = var_string_stem
        if use_suffix:  
            # do not add the dir suffix for T0 on a forecast (i.e., AnA file, while rest are fcst files)
            if row['config'] == config:
                config_dir = config_dir + dir_suffix
                var_string = var_string_stem + var_str_suffix
            
        # if the valid time of the forecast time step is within the list range, include it
        val_time = row['val_time']
        
        if val_time >= list_start and val_time <= list_end:
            
            # build the filename
            filename_parts = ['nwm',
                               row['ref_hr_string'], 
                               row['config'],
                               var_string,
                               row['ts_hr_string'], 
                               domain,
                              'nc']
                      
            filename = ".".join(filename_parts)
            
            # add the full path to the list
            filelist.append(Path(row['datedir']) / config_dir / filename)
            
            count += 1
                
    # console messages
    if not filelist:
        print('   No timesteps found in range', val_start, 'to', val_end)
    else:
        if val_start != -999:
            print('   ',count, 'timesteps found in range', val_start, 'to', val_end)     
    
    # if using no_da, add no_da string to the filenames
    if use_no_da:
        filelist = add_no_da_to_filelist(filelist)
 
    return filelist
    
   
def build_filelist_by_reftime(domain, variable, config, ref_time, use_no_da = False):
    '''
    Build a nwm filelist of all files associated with a nwm domain, variable, config, and reftime

    Parameters:
    domain:             NWM geographic domain
    variable:           variable - channel or forcing
    config:             NWM configuration ('short_range','medium_range','analysis_assim_extend',
                                           'analysis_assim','latest_ana')
    ref_time:           forecast (or ana) reference time
    use_no_da:          if ana, use the no_da version or not
    '''
                       
    # get version based on ref_time
    version = admin.nwm_version(ref_time)
        
    # get base dataframe of config and variable info
    df_config = admin.config_specs(config, domain, version)
    df_var = admin.variable_specs(domain) 
    
    # get duration, time interval and whether it is a forecast 
    # (flag for "f" versus "tm")
    is_forecast = df_config.loc[config,"is_forecast"]    
    ts_int = df_config.loc[config,"timestep_int"].item()
    n_hours = df_config.loc[config,"duration_hrs"].item()

    # initialize the datedir (and ref_time if none provided)
    datedir = ref_time.strftime("nwm.%Y%m%d")  # ref date directory   
      
    # get a dataframe of the filename parts for the defined nwm config
    df_parts = build_reftime_fileparts(ref_time, config, n_hours, ts_int, is_forecast)                            

    member_str = ""
    if config == 'medium_range':
        member_str = "| member " + str(member) 

    print ("---Building file list for", config, variable,"| reference time:", ref_time, "| all timesteps",  member_str)          
       
    # build the filelist from all the parts
    filelist = build_filelist_from_parts(domain, version, config, variable, df_parts)   
        
    if use_no_da:
        filelist = add_no_da_to_filelist(filelist)
 
    return filelist


def get_filelist_start_end(ref_time, config, n_hours, ts_int, is_forecast = True,
                           include_t0 = False, val_start = -999, val_end = -999):
                       
    '''
    Get the start and end datetimes of a list of files for specific specs
    '''
                            
    # Get the forecast start and end datetime.  
    # First getting list of all files for the forecast, later will keep only those in the valid range 
    # Include T0 (from standard AnA) based on argument

    if is_forecast:
    
        forecast_end = ref_time + timedelta(hours=n_hours)  
        
        if include_t0:
            forecast_start = ref_time           
            n_files = int(n_hours/ts_int + 1)
        else:
            forecast_start = ref_time + timedelta(hours=ts_int)
            n_files = int(n_hours/ts_int)           

    else:
        forecast_start = ref_time - timedelta(hours=n_hours-1)  
        forecast_end = ref_time

    # initialize the start and end dates of the filelist
    # if a range of valid times to use was not defined, include all timesteps of the forecast    
    if val_start == -999:
        list_start = forecast_start
        list_end = forecast_end       
    else:
        list_end = val_end
        list_start = val_start       
        
    print(n_files, ts_int, forecast_start)
        
    return list_start, list_end
 
 
def build_filelist_from_parts(domain, version, config, variable, df_parts, list_start = -999, list_end = -999):
    '''
    Build a list of filenames from a dataframe of the filename parts
    '''

    # get base dataframe of config and variable info
    df_config = admin.config_specs(config, domain, version)
    df_var = admin.variable_specs(domain)

    # base configuration directory prefix (e.g. 'forcing')
    dir_prefix = df_var.loc[variable, 'dir_prefix']
    
    # variable string used in filename ('forcing' or 'channel_rt' for now)
    var_string_stem = df_var.loc[variable, 'var_string']    

    # base configuration directory suffix (e.g. 'mem1' for medium_range ensemble member 1)    
    dir_suffix = df_config.loc[config, 'dir_suffix']

    # suffix at the end of the variable name (e.g. '1' for medium_range ensemble mem1)
    var_str_suffix = df_config.loc[config, 'var_str_suffix']

    # flag to trigger using suffixes or not (for forcing or hawaii)
    use_suffix = df_var.loc[variable, 'use_suffix']   
    
    # if list_start and end are -999, set q

    # loop through fileparts dataframe, add prefix/suffixes, and build filenames
    # include each file in the list only if it's valid time falls within the requested range (if range defined)
    filelist = []
    count = 0
    include_all = True
    if list_start != -999 and list_end != -999:
        include_all = False
        
    for index, row in df_parts.iterrows():
        
        # add prefix and suffixes as needed
        config_dir = dir_prefix + row['config']
        
        var_string = var_string_stem
        if use_suffix:
            config_dir = config_dir + dir_suffix
            var_string = var_string_stem + var_str_suffix
            
        val_time = row['val_time']
               
        # if the valid time of the forecast time step is within the list range, include it
        if include_all or (val_time >= list_start and val_time <= list_end):
            
            # build the filename
            filename_parts = ['nwm',
                               row['ref_hr_string'], 
                               row['config'],
                               var_string,
                               row['ts_hr_string'], 
                               domain,
                              'nc']
                      
            filename = ".".join(filename_parts)
            
            # add the full path to the list
            filelist.append(Path(row['datedir']) / config_dir / filename)
            
            count += 1
 
    return filelist    


def build_reftime_fileparts(ref_time, config, n_hours, ts_int, is_forecast = True, include_t0 = False):
    '''
    Build dataframe of filename parts for a specific reference time
    '''
    
    if include_t0 and is_forecast:
        n_files = int(n_hours/ts_int + 1)
    else:
        n_files = int(n_hours/ts_int)
        
    # initialize the file parts dataframe
    df_parts = pd.DataFrame(
            {"datedir" : np.full(n_files, ref_time.strftime("nwm.%Y%m%d")),
             "ref_hr_string" : np.full(n_files, 't00z'),
             "config" : np.full(n_files, config),
             "ts_hr_string" : np.full(n_files, 'tm00'),
             "val_time" : np.full(n_files, ref_time)},
             index = np.arange(n_files))  
    
    #### Get filename parts for all files associated with the config and reference time
    if is_forecast:
        df_parts = get_forecast_fileparts(ref_time, n_files, df_parts, ts_int, False)   
    else:
        df_parts = get_simple_ana_fileparts(ref_time, n_hours, df_parts, ts_int)
    
    return df_parts    
    
    
def build_forecast_fileparts(ref_time, config, n_hours, ts_int, include_t0, val_start, val_end):
    '''
    Build dataframe of filename parts for a forecast configuration
    '''

    # Get the forecast start and end datetime.  
    # First getting list of all files for the forecast, later will keep only those in the valid range 
    # Include T0 (from standard AnA) based on argument
    if include_t0:
        forecast_start = ref_time
        n_files = int(n_hours/ts_int + 1)
    else:
        forecast_start = ref_time + timedelta(hours=ts_int)
        n_files = int(n_hours/ts_int)
        
    forecast_end = ref_time + timedelta(hours=n_hours)     

    # initialize the start and end dates of the filelist
    list_end = val_end
    list_start = val_start    

    # if a range of valid times to use was not defined, include all timesteps of the forecast
    if val_start == -999:
        list_start = forecast_start
        list_end = forecast_end                  
    
    # check if the end of the forecast is before the end of the valid range
    # (if so, results will be plotted differently in AAR plots)
    if forecast_end < list_end:
        list_end = forecast_end
        
    # check if the start of the forecast is after the start of the valid range
    if ref_time > list_start:
        list_start = ref_time + timedelta(hours=ts_int)
        
    # initialize the file parts dataframe
    df_parts = pd.DataFrame(
            {"datedir" : np.full(n_files, ref_time.strftime("nwm.%Y%m%d")),
             "ref_hr_string" : np.full(n_files, 't00z'),
             "config" : np.full(n_files, config),
             "ts_hr_string" : np.full(n_files, 'tm00'),
             "val_time" : np.full(n_files, ref_time)},
             index = np.arange(n_files))
    
    #### Get filename parts for all files associated with the config and reference time
    df_parts = get_forecast_fileparts(ref_time, n_files, df_parts, ts_int, include_t0)   
    
    return df_parts, list_start, list_end
    

def get_forecast_fileparts(ref_time, n_files, df_parts, ts_int, include_t0):
    '''
    Get filename parts for a forecast configuration
    '''   
 
    # loop through to build each filename
    for i in range(n_files): # (1 through n_files)
               
        if i == 0 and include_t0:
        
            # timestep is always T0 in this case
            ts_hr = 0
        
            # T0 comes from standard AnA tm00
            df_parts.loc[i,'ref_hr_string'] = 't' + ref_time.strftime("%Hz")
            df_parts.loc[i,'config'] = 'analysis_assim'
            df_parts.loc[i,'ts_hr_string'] = 'tm00'
            
            # if ts_int is a fraction (hawaii is 15 min), add minutes to the time string
            if ts_int%1 > 0:
                df_parts.loc[i,'ts_hr_string'] = 'tm0000'
            
        #otherwise, skip i = 0    
        else:
        
            # if T0 was included, will enter the above code rather than here, so i=1 on first entry here
            # otherwise i=0 on first entry here and need to add 1 so i=1.
            if include_t0:
                ts_hr = i*ts_int
            else:
                ts_hr = (i+1)*ts_int

            # update date directory
            df_parts.loc[i,'datedir'] = ref_time.strftime("nwm.%Y%m%d")  
            
            # update the reference time string
            df_parts.loc[i,'ref_hr_string'] = 't' + ref_time.strftime("%Hz") 
            
            # create the forecast time step string
            df_parts.loc[i,'ts_hr_string'] = "f" + str(ts_hr).zfill(3)    
            
            # if ts_int is a fraction (hawaii is 15 min), add minutes to the time string
            if ts_int%1 > 0:
                df_parts.loc[i,'ts_hr_string'] = "f" + str(int(np.floor(ts_hr))).zfill(3) + str(int(ts_hr%1*60)).zfill(2)

        val_time = ref_time + timedelta(hours=ts_hr)
        df_parts.loc[i,'val_time'] = val_time
             
    return df_parts  
    
    
def build_ana_fileparts_aligned_to_forecast(ref_time, config, n_hours, ts_int, include_t0, domain):
    '''
    Build a dataframe of filename parts for an AnA configuration aligned with the valid times of a 
    specified forecast configuration
    '''
    
    #print('n_hours', n_hours)
    
    # get the start datetime of the aligning forecast
    # include t0 or not based on include_t0 argument
    if include_t0:
        forecast_start = ref_time
        n_files = int(n_hours/ts_int + 1)
    else:
        forecast_start = ref_time + timedelta(hours=ts_int)
        n_files = int(n_hours/ts_int)
    
    # get the end datetime of the aligning forecast
    forecast_end = ref_time + timedelta(hours=n_hours)          
    
    # the start and end dates of the filelist is the full forecast duration in this case
    list_start = forecast_start
    list_end = forecast_end    
            
    # initialize the file parts dataframe
    df_parts = pd.DataFrame(
            {"datedir" : np.full(n_files, ref_time.strftime("nwm.%Y%m%d")),
             "ref_hr_string" : np.full(n_files, 't00z'),
             "config" : np.full(n_files, config),
             "ts_hr_string" : np.full(n_files, 'tm00'),
             "val_time" : np.full(n_files, ref_time)},
             index = np.arange(n_files))
    
    # Get filename parts for all AnA files corresponding to the valid time range
    if config == 'latest_ana':
        # if latest_ana and domain is conus, use the latest_ana_fileparts function 
        # (only works for conus, where there is an extended ana configuration)
        if domain == 'conus':
            df_parts = get_latest_ana_fileparts(ref_time, n_hours, ts_int, df_parts, include_t0, domain)
            
        # for other domains use the ana_fileparts function with is_latest flag set to True
        else:
            df_parts = get_ana_fileparts(ref_time, config, n_files, ts_int, df_parts, include_t0, is_latest = True)

    else:
        # for standard and extended-only configs, use the get_ana_fileparts function (is_latest=False by default)
        df_parts = get_ana_fileparts(ref_time, config, n_files, ts_int, df_parts, include_t0)
        
    return df_parts, list_start, list_end
       
    
def build_ana_fileparts_defined_range(val_start, val_end, ts_int, config, domain):
    '''
    Build a dataframe of filename parts for an AnA configuration within a specified
    valid start/end time
    '''

    # for a defined date range, include_t0 is always True
    include_t0 = True    
    
    # n_hours is the hours between and including start/end dates
    delta = val_end - val_start
    n_hours = int(delta.total_seconds() / 3600) + 1
    if ts_int == 1:
        n_files = n_hours
    else:
        n_files = int(n_hours/ts_int + 1)
    
    # the start and end dates of the filelist match the defined range in this case
    list_end = val_end
    list_start = val_start  
            
    # initialize the file parts dataframe
    df_parts = pd.DataFrame(
            {"datedir" : np.full(n_files, val_start.strftime("nwm.%Y%m%d")),
             "ref_hr_string" : np.full(n_files, 't00z'),
             "config" : np.full(n_files, config),
             "ts_hr_string" : np.full(n_files, 'tm00'),
             "val_time" : np.full(n_files, val_start)},
             index = np.arange(n_files))
    
    # Get filename parts for all AnA files corresponding to the valid time range
    if config == 'latest_ana':
        # if latest_ana and domain is conus, use the latest_ana_fileparts function 
        # (only works for conus, where there is an extended ana configuration)
        if domain == 'conus':
            # to make this work, need to subtract 1 from n_hours, it will be added back in this function if 
            # include_t0 = True, which is always is here
            df_parts = get_latest_ana_fileparts(val_start, n_hours-1, ts_int, df_parts, include_t0, domain)
            
        # for other domains use the ana_fileparts function with is_latest flag set to True
        else:
            df_parts = get_ana_fileparts(val_start, config, n_files, ts_int, df_parts, include_t0, is_latest = True)
            
    else:
        # if straightforward AnA (standard or extended) use ana_fileparts
        df_parts = get_ana_fileparts(val_start, config, n_files, ts_int, df_parts, include_t0)
        
    return df_parts, list_start, list_end  
    

def get_ana_fileparts(start_time, config, n_files, ts_int, df_parts, include_t0, use_tm02 = True, is_latest = False):
    '''
    Build a dataframe of filename parts for an AnA configuration based on argument specifications
    '''
    
    #get current clock time to check if "next day AnA" is available yet
    clock_ztime = datetime.utcnow().replace(second=0, microsecond=0, minute=0)    

    for i in range(n_files):
    
        # get ts_hr - the time of this timestep iteration relative to the start time
        # when T0 not included (will only occur when aligning with a forecast), shift ts_hr forward 
        if include_t0:
            ts_hr = i*ts_int 
        else:
            ts_hr = (i+1)*ts_int       
               
        # get the valid time of the timestep for this iteration
        val_time = start_time + timedelta(hours=ts_hr) # calendar date/time of forecast timestep

        if config == "analysis_assim_extend":
            #e-AnA only runs in 16z cycle, get all output from this ref-time, either current or next date
            ref_hr_string = '16z'  
            
            #Valid hours 0-12 --> align with tm16-tm04 in same date directory
            if val_time.hour < 13:  
                datedir = val_time.strftime("nwm.%Y%m%d")
                ts_hr = 16 - val_time.hour            
                
            #Valid hours 13-23 --> align with tm27-tm17 in next date directory    
            else:              
                nextday = (val_time + timedelta(days=1)).replace(second=0, microsecond=0, minute=0)
                datedir = (nextday).strftime("nwm.%Y%m%d")
                ts_hr = 40 - val_time.hour
                              
                # if nextday not yet available, fill in tm03-00 from current day
                if nextday.replace(hour=19) > clock_ztime:
                    datedir = val_time.strftime("nwm.%Y%m%d")
                    ts_hr = 16 - val_time.hour
                    
                    # if the ts_hr becomes negative, use next day - will be missing but keep in the filelist
                    if ts_hr < 0:
                        ts_hr = 40 - val_time.hour
                        datedir = (nextday).strftime("nwm.%Y%m%d")
             
        else:
            #standard AnA runs every cycle, if get_tm02 = True, use tm02 from ref_time + 2, else use tm00 from ref-time
            if use_tm02:
                datedir = (val_time + timedelta(hours=2)).strftime("nwm.%Y%m%d")
                ref_hr_string = (val_time + timedelta(hours=2)).strftime("%Hz")
                ts_hr = 2
                
                val_minutes = val_time.minute
                if val_minutes > 0:
                    ts_min = 60 - val_minutes
                else:
                    ts_min = 0
                
                # if building most recent possible filelist, end with T0
                if is_latest:
                
                    # 2nd to last file is T1 of most recent available reftime
                    if i >= (n_files - (1/ts_int) - 1):                      
                        datedir = (val_time + timedelta(hours=1)).strftime("nwm.%Y%m%d")
                        ref_hr_string = (val_time + timedelta(hours=1)).strftime("%Hz")                      
                        ts_hr = 1
   
                    # last file is T0 of most recent available reftime
                    if i == n_files - 1:
                        datedir = val_time.strftime("nwm.%Y%m%d")
                        ref_hr_string = val_time.strftime("%Hz")
                        ts_hr = 0    
                
                # if valid time is not top of the hour, need to subtract an hour to get the 
                # corresponding AnA timestep string, since they increase going backward in time
                if val_minutes > 0:
                    ts_hr = ts_hr - 1
                
            # if get_tm02 is False, use the t00 from each standard AnA (currently never used)
            # ** this option does not yet work if timestep interval (ts_int) is < 1 hour
            else:
                datedir = val_time.strftime("nwm.%Y%m%d")
                ref_hr_string = val_time.strftime("%Hz")
                ts_hr = 0
                  
        # update filename parts dataframe
        df_parts.loc[i,'datedir'] = datedir
        df_parts.loc[i,'ref_hr_string'] = 't' + ref_hr_string    
        df_parts.loc[i,'ts_hr_string'] = 'tm' + str(ts_hr).zfill(2)
        df_parts.loc[i,'val_time'] = val_time
        
        # if using this function to get the latest AnA, the config is always standard AnA 
        if is_latest:
            df_parts.loc[i,'config'] = 'analysis_assim'
        
        # if ts_int is a fraction (hawaii is 15 min), add minutes to the time string
        if ts_int%1 > 0:
            df_parts.loc[i,'ts_hr_string'] = 'tm' + str(ts_hr).zfill(2) + str(ts_min).zfill(2)
        
    return df_parts
    

def get_latest_ana_fileparts(start_time, n_hours, ts_int, df_parts, include_t0, domain):
    '''
    Build a dataframe of filename parts, determining which AnA configuration (standard or extended) 
    is the best/most recent available that corresponds to each valid time in the most recent evaluatable forecast 
    (e.g. issued 20 hours earlier for short range - 18 hours plus 2 hour latency)
    
    *Note currently only works to combine extended and standard AnA for 1-hour timestep
    '''
    
    # get the start datetime of the aligning forecast
    # include t0 or not based on include_t0 argument
    if include_t0:
        end_time = start_time + timedelta(hours=n_hours)
    else:
        end_time = start_time + timedelta(hours=n_hours)
        start_time = start_time + timedelta(hours=1)
        
    n_hours_back = n_hours
    #start_time = end_time - timedelta(hours=n_hours_back)

    # get the valid times (time on the ground) between start_time/end_time
    # e.g. for comparison to SRF issued 18-hrs prior, get ref_time (18 hours ago) plus 18 hours
    val_times = pd.date_range(start=start_time, end=end_time, freq='H')    

    # get number of datetimes (number of filenames being generated)
    n_files = len(val_times)
    
    # update the val_times column
    df_parts['val_time'] = val_times
    
    # last 3 files are always most recent standard AnA
    config = 'analysis_assim'    
    for i in np.arange(n_files-3,n_files):
    
        df_parts.loc[i,'datedir'] = end_time.strftime("nwm.%Y%m%d")
        df_parts.loc[i,'ref_hr_string'] = 't' + end_time.strftime("%Hz")   
        df_parts.loc[i,'ts_hr_string'] = 'tm' + str(n_files - i - 1).zfill(2)
        df_parts.loc[i,'config'] = config      
        
    # the algorithm below determines which timesteps have an extended AnA value available
    #       for the valid time and which have only standard available
    #       and determines which set of output (which date for extana) the valid time is part of

    # keep track of the timestep hour of extended to know when to switch dates
    ext_ts_hour = 0
    
    # starting with 4th to last timestep (prior to most recent std AnA run), work backwards
    for i in range(n_files-4,-1,-1):

        # ivt short for ith valid time
        ivt = val_times[i]
        vt_date = datetime(ivt.year, ivt.month, ivt.day, 0, 0, 0)
        vt_hour = ivt.hour
        
        # if the valid time hour hits 16, the last run ext-ana is available 
        if config == 'analysis_assim' and vt_hour == 16:
            config = 'analysis_assim_extend'
            ext_ts_hour = 0
               
        df_parts.loc[i,'config'] = config   
            
        # if the valid time is pulling from standard, use tm02 from ref time 2 hours ahead
        # currently hard-coded here to use tm02 only for all timesteps prior to most recent avail std AnA
        if config == 'analysis_assim':
            data_time = ivt + timedelta(hours = 2)
            std_ts_hour = 2
       
            df_parts.loc[i,'ts_hr_string'] = 'tm' + str(np.int(std_ts_hour)).zfill(2)
            
        # if pulling from an extended ana run, figure out which date and timestep
        else:
            if ext_ts_hour < 17:
                data_time = datetime(ivt.year, ivt.month, ivt.day, 16, 0, 0)
            else:
                data_time = datetime(ivt.year, ivt.month, ivt.day+1, 16, 0, 0)              
          
            df_parts.loc[i,'ts_hr_string'] = 'tm' + str(np.int(ext_ts_hour)).zfill(2)

            if ext_ts_hour == 27:
                ext_ts_hour = 4
            else:
                ext_ts_hour += 1    
                
        # update filename parts dataframe 
        df_parts.loc[i,'datedir'] = data_time.strftime("nwm.%Y%m%d")
        df_parts.loc[i,'ref_hr_string'] = 't' + data_time.strftime("%Hz")   
        
                
    return df_parts
   

def get_simple_ana_fileparts(ref_time, n_hours, df_parts, ts_int):
    '''
    straightforward filenames for all timesteps of a given AnA config
    '''   
    
    for i in range(0, n_hours, ts_int): # (0 through n_hours, incrementing by ts_int)

        df_parts.loc[i,'datedir'] = ref_time.strftime("nwm.%Y%m%d")
        df_parts.loc[i,'ref_hr_string'] = 't' + ref_time.strftime("%Hz")     
        
        ts_hr = i

        # fill in ts_hr_string
        df_parts.loc[i,'ts_hr_string'] = "tm" + str(ts_hr).zfill(2)        

    df_parts = df_parts.iloc[::-1]

    return df_parts 
    
    
def add_no_da_to_filelist(filelist):
    '''
    Add the 'no_da' extension to the config directory and filename for a list of filepaths
    '''    
    
    new_filelist = []
    
    if filelist:
        for path in filelist:
        
            parts = path.name.split(".")
            parts[2] = parts[2] + "_no_da"
            new_filename = ".".join(parts)
            
            new_path = path.parent.parent / (path.parent.name + '_no_da') / new_filename
            
            new_filelist.append(new_path)

    return new_filelist 
    
def get_filelist_chunks_by_df(df_filelists, n = 24, include_t0 = True):
    '''
    run the filelist chunker for multiple filelists
    '''
    
    df_filelists_chunks = pd.DataFrame()
    
    for index, row in df_filelists.iterrows():
    
        filelist = row['filelist']
        filelist_chunks, n_chunks = get_filelist_chunks(filelist, n, include_t0 = include_t0)
        
        df_filelists_chunks = df_filelists_chunks.append([pd.DataFrame({'filelist_chunks' : [filelist_chunks]},
                                                                      index = [index])])
    
    df_filelists = pd.concat([df_filelists, df_filelists_chunks], axis = 1)
    
    return df_filelists, n_chunks
    
    
def get_filelist_chunks(filelist, n = 24, include_t0 = True):
    '''
    Split a list of filenames into chunks containing n files each.
    If include_t0 = True, include a 't0' file (last file from prior chunk) in order to
       calculate some necessary metrics, e.g., rate of rise
    '''

    # number of timesteps
    nts = len(filelist)

    if nts > n:
    
        # if include_t0 = True, start chunking at t1 and 
        # append last timestep of prior chunk to each list;
        # first chunk will append the true t0  
        # otherwise start chunking at t0        
        if include_t0:
            filelist_chunks = [filelist[i-1:i+n] if i+n <= nts else filelist[i-1:nts] \
                               for i in range(1,nts,n)]                
        else:        
            filelist_chunks = [filelist[i:i+n] if i+n <= nts else filelist[i:nts] \
                               for i in range(0,nts,n)]
                     
        n_chunks = len(filelist_chunks)
            
    else:
    
        filelist_chunks = [filelist]
        n_chunks = 1

    return filelist_chunks, n_chunks
    
    
def parse_nwm_filelist(filelist):
    '''
    Parse out filename parts from a list of nwm filepaths and store as separate columns in a dataframe
    '''

    date = [path.parents[1].name for path in filelist]
    ref_hr =  [path.name.split(".")[1] for path in filelist]
    reftimes = [datetime.strptime(d, 'nwm.%Y%m%dt%Hz') for d in list(map(str.__add__, date, ref_hr))]
    ts = [path.name.split(".")[-3] for path in filelist]
    config = [path.name.split(".")[2] for path in filelist]
    variable = [path.name.split(".")[3] for path in filelist]
    domain = [path.name.split(".")[-2] for path in filelist]
    
    df = pd.DataFrame({'reftime' : reftimes, "ts" : ts, 'config' : config, 'variable' : variable, 'domain' : domain})   
    
    # add valid times
    df['val_time'] = -999

    for i in range(len(df)):
        ref_time = df.loc[i, 'reftime']
        ts_str = df.loc[i, 'ts']
        if ts_str[0:2] == 'tm':
            hr_add = -1 * int(ts_str[2:])
        else:
            hr_add = int(ts_str[2:])           
        val_time = ref_time + timedelta(hours = hr_add)        
        df.loc[i,'val_time'] = val_time    
    
    return df


def get_alt_ana_filelist(filelist):
    '''
    switch a filelist from one config (e.g. ExtAnA) for another (AnA) for same valid times
    '''
    
    subset = False
    df = parse_nwm_filelist(filelist)
    
    # first check if the filelist contains forecasts or AnA, if not, return empty list
    config = df.loc[0, 'config']
    if config in ['short_range','medium_range']:
        return []
    
    filelist_alt = []
    for i in range(len(df)):
        
        val_time = df.loc[i, 'val_time']
        variable = df.loc[i, 'variable']
        config = df.loc[i, 'config']
        if config == 'analysis_assim_extend':
            alt_config = 'analysis_assim'
        elif config == 'analysis_assim':
            alt_config = 'analysis_assim_extend'
            
        if variable[-6:] == 'subset':
            subset = True
            variable = variable[:-7]

        if variable[:11] == 'channel_rt':
            variable = 'channel'
        n_files = 1
        
        if i == 0:
            version = admin.nwm_version(val_time)
            domain = df.loc[i, 'domain']
                   
        df_parts = pd.DataFrame(
            {"datedir" : np.full(n_files, val_time.strftime("nwm.%Y%m%d")),
             "ref_hr_string" : np.full(n_files, 't00z'),
             "config" : np.full(n_files, alt_config),
             "ts_hr_string" : np.full(n_files, 'tm00'),
             "val_time" : np.full(n_files, val_time)},
             index = np.arange(n_files))    
        
        df_parts = get_ana_fileparts(val_time, alt_config, n_files, 1, df_parts, True)
        filename = build_filelist_from_parts(domain, version, alt_config, variable, df_parts)
        
        if subset:
            filename_add = [raw.get_subset_path(filename[0])]
        else:
            filename_add = filename
        
        filelist_alt = filelist_alt + filename_add
    
    return filelist_alt

    
    
def parse_nwm_path(nwm_path):
    '''
    Parse out filename parts from a single nwm filepath
    '''

    date = nwm_path.parents[1].name
    ref_hr =  nwm_path.name.split(".")[1]
    ref_time = datetime.strptime(date + ref_hr, 'nwm.%Y%m%dt%Hz')
    ts = nwm_path.name.split(".")[-3]
    config = nwm_path.name.split(".")[2]
    
    return ref_time, ts, config
    

def parse_ts_from_path(nwm_path):
    '''
    Parse out timestep number as integer for the file with index i
    '''

    ref_time, ts, config = parse_nwm_path(nwm_path)
    
    if ts[0] == 't':    # tmxx
        t = -1 * int(ts[2:])
    else:               # fxxx
        t = int(ts[1:])

    return t
    
    
def parse_valtime_from_path(nwm_path):
    '''
    Parse out timestep number as integer for the file with index i
    '''

    ref_time, ts, config = parse_nwm_path(nwm_path)
    
    if ts[0] == 't':    # tmxx
        t = -1 * int(ts[2:])
    else:               # fxxx
        t = int(ts[1:])
        
    val_time = ref_time + timedelta(hours = t)        

    return val_time