import os
import pandas as pd

# InfluxDB address (represented as a 'csv' file)
INFLUXDB_ADDR = './influxdb.csv'

def report_execution_time(uuid, kci, resource_id, component_name, 
                          start_time_job, start_time_func, end_time_func):

    ''' Prototype function for sending runtime information to InfluxDB. 
    This function is executed by the 'exec_time' decorator during component's execution.

    NOTE: JUST FOR TESTING IT SIMULATES THE WRITING ON INFLUXDB BY WRITING ON A CSV FILE.
        
        Parameters:
            uuid (str): universally unique identifier associated to a specific file.
            kci (str): kubernetes cluster identifier.
            resource_id (str): identifier of the resource on which the specific application component
                has been deployed.
            component_name (str): user-defined name of the application component.
            start_time_job (float): job creation time including the creation time of the input 
                file in the MinIO storage.
            start_time_func (float): starting time of the main function of the monitored component.
            end_time_func (float): end time of the main function of the monitored component.
    '''


    # Create the entry to be stored into InfluxDB
    entry = {'uuid': [uuid], 'resource_id': [resource_id], 'component_name': [component_name],
            'start_time_job': [start_time_job], 'start_time_func': [start_time_func], 'end_time_func': [end_time_func]}


    if os.path.exists(INFLUXDB_ADDR):
        # csv file already exists, thus it must be updated 
        dataframe = pd.read_csv(INFLUXDB_ADDR)
        dataframe = dataframe.append(pd.DataFrame(entry))
    else:
        # csv does not exist. Let's initialize a new dataframe!
        dataframe = pd.DataFrame(entry)

    dataframe.to_csv(INFLUXDB_ADDR, index=False)