'''

'''
from pathlib import Path
from datetime import datetime
from . import admin, reftime


class FcstEvalSpecs:
    '''
    Class to store limited forecast evaluation specs
    '''

    def __init__(self, user_pars):
    
        print('\n-------------Setting Eval Specs----------------')
    
        ########## define default spec values ##########
        
        # local system specs
        self.data_dir = Path('C:/default')
        self.in_dir = Path('C:/default')
        self.out_dir = Path('C:/default') 
        
        # domain and timing
        self.domain = 'conus'
        self.ref_start = datetime.utcnow().replace(second=0, microsecond=0, minute=0)
        self.ref_end = self.ref_start
        self.ref_time_list = []
        self.eval_timing = 'past'
        self.fcst_config = 'short_range'
        self.ana_config = 'analysis_assim'
             
        # store defaults for later checks
        defaults = self
   
        ########## overwrite defaults with any run-time defined specs ##########
        
        # loop through the list and reset any that were specified
        for par in dir(self):
            if par in user_pars:
                setattr(self, par, user_pars[par])       
                
        ########## do some checks ##########

        # date checks, set start/end for version check

        if not ('ref_start' in user_pars and 'ref_end' in user_pars):    
            raise NameError('starting reference or valid times must be defined')
            
        else:
            start = self.ref_start
            end = self.ref_end
            
            if end == -999:
                ref_time_list = reftime.get_recent_reftimes(self, self.config, use_latency = True)       
                end = ref_time_list[0]
                self.ref_end = end                
                               
        version = admin.nwm_version(start)
        version_end = admin.nwm_version(end)
        if version != version_end:
            raise ValueError('Date range spans different NWM versions - not yet supported')                     
        if end < start:
            raise ValueError('Start date after end date')

        # domain/config
        if self.domain in ['hawaii','puertorico'] and self.config == 'medium_range':
            raise ValueError('Domain and configuration not compatible')

        ########## set some values ##########    
            
        # set NWM version based on ref time start date
        self.version = version            
            
        # set reference times
        self.ref_time_list = reftime.reftimes_in_range(self, self.fcst_config)

    
    
def reftimes(specs):
    '''
    launch reference time selector
    '''
    
    print('\n-------------Setting Reference Times----------------')

    specs.ref_time_list = reftime.get_reftime_list(specs)

    print('\nSelected reference times: ')
    for ref_time in specs.ref_time_list:
        print(ref_time) 

    return specs
    
 