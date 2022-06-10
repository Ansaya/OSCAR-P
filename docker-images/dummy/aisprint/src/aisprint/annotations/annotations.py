import os
import functools
import time
from ..monitoring import report_execution_time

def exec_time(local_time_thr=None, global_time_thr=None, prev_components=None):
    ''' 'exec_time' QoS Annotation. 
    It allows users to define local and/global time constraints.
        Parameteres:
            local_time_thr (float): maximum desired execution time for a single component.
                It is measured in milliseconds.
            global_time_thr (float): maximum desired execution time for a group of component.
                It is measured in milliseconds.
            prev_components (list of str): define the global contraint execution path by explicitly 
                defining the previous components involved. The components are identified 
                by the assigned names.
                It must be != None when global_time_thr != None.
        Execution:
            It provides the runtime information to be saved to the InfluxDB database 
            through the 'to_monitoring_tool' utility function.
    '''
    def decorator_exec_time(func):
        @functools.wraps(func)
        def wrapper_exec_time(*args, **kwargs):
            start_time_func = time.time()
            value = func(*args, **kwargs)
            end_time_func = time.time()
            uuid = os.getenv('UUID')
            kci = os.getenv('KCI')
            resource_id = os.getenv('RESOURCE_ID')
            component_name = os.getenv('COMPONENT_NAME')
            start_time_job = os.getenv('EVENT')
            report_execution_time(uuid, kci, resource_id, component_name, 
                                  start_time_job, start_time_func, end_time_func)
            return value
        return wrapper_exec_time
    return decorator_exec_time

def component_name(name):
    ''' 'component_name' Annotation. 
    It allows users to define the name of the component.
        Parameteres:
            name (str): name that will be associated to the annotated component.
        Execution:
            No additional functionality.
    '''
    def decorator_component_name(func):
        @functools.wraps(func)
        def wrapper_component_name(*args, **kwargs):
            value = func(*args, **kwargs)
            return value
        return wrapper_component_name
    return decorator_component_name

def expected_throughput(rate):
    ''' 'expected_throughput' Annotation. 
    It allows users to define the expected application throughput, 
    defined the number of files ingested per time unit.
        Parameters:
            rate (float): desired #files per time unit.
        Execution:
            No additional functionality.
    '''
    def decorator_expected_throughput(func):
        @functools.wraps(func)
        def wrapper_expected_throughput(*args, **kwargs):
            value = func(*args, **kwargs)
            return value
        return wrapper_expected_throughput
    return decorator_expected_throughput

def partitionable_model(onnx_file):
    ''' 'partitionable_model' QoS Annotation. 
    It allows users to define a partitionable deep-neural-network (DNN) based component. 
    Partitionable components are automatically split into partitions if needed by 
    AI-SPRINT. Users are required to provide the ONNX intermediate representation of the 
    neural model.
        Parameters:
            onnx_file (str): path to the ONNX file representing the used model.
        Execution:
            No additional functionality.
    '''
    def decorator_partitionable_model(func):
        @functools.wraps(func)
        def wrapper_partitionable_model(*args, **kwargs):
            value = func(*args, **kwargs)
            return value
        return wrapper_partitionable_model
    return decorator_partitionable_model

def device_constraints(ram, vram, use_gpu_for):
    '''  'device_constraints' QoS Annotation. 
    It allows users to define additional constraints in the case of a DNN-based component. 
    Constraints regard the minimun required memory (RAM) and GPU memory (RAM). Furthermore,
    it is possible for the user to explicitly define if the GPU must be used only for the 
    execution of the DNN, for pre-processing, for post-processing, or a combination of the three. 
        Parameters:
            ram (float): minimum required RAM.
            vram (float): minimum required VRAM.
            use_gpu_for (list of str with values in {'dnn', 'pre', 'post'}): required use for the GPU 
        Execution:
            No additional functionality.
    '''
    def decorator_device_constraints(func):
        @functools.wraps(func)
        def wrapper_device_constraints(*args, **kwargs):
            value = func(*args, **kwargs)
            return value
        return wrapper_device_constraints
    return decorator_device_constraints

def early_exit_model(onnx_file, early_exits):
    '''  'early_exit_model' QoS Annotation. 
    It allows users to define an early-exit component, i.e., a component using a DNN with early exits. 
        Parameters:
            onnx_file (str): complete path to the ONNX file representing the early-exit DNN.
            early_exits (dict): items of the dictionary are all the early exits of the model, each
                one represented by a dictionary with keys equal to nodes names.
        Execution:
            No additional functionality.
    '''
    def decorator_early_exit_model(func):
        @functools.wraps(func)
        def wrapper_early_exit_model(*args, **kwargs):
            value = func(*args, **kwargs)
            return value
        return wrapper_early_exit_model
    return decorator_early_exit_model

def annotation(annotation_dict):
    ''' Decorator for all-in-one annotation mechanism.
    It allows users to define all the annotations using a single decorator.
    Parameters:
        annotation_dict (dict): dictionary where the keys are the name of the decorators
            and the values are the corresponding parameters.
    '''
    def decorator_annotation(func):
        @functools.wraps(func)
        def wrapper_annotation(*args, **kwargs):
            # In the case the annotated component has a time constraint we need to 
            # implement the same functionality of the original 'exec_time' decorator.
            if 'exec_time' in annotation_dict:
                start_time_func = time.time()
                value = func(*args, **kwargs)
                end_time_func = time.time()
                uuid = os.getenv('UUID')
                kci = os.getenv('KCI')
                resource_id = os.getenv('RESOURCE_ID')
                component_name = os.getenv('COMPONENT_NAME')
                start_time_job = os.getenv('EVENT')
                report_execution_time(uuid, kci, resource_id, component_name, 
                                      start_time_job, start_time_func, end_time_func)
                return value
            value = func(*args, **kwargs)
            return value
        return wrapper_annotation
    return decorator_annotation