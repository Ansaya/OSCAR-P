import os
import sys
import argparse
import ast
import json
import numpy as np

# TODO: decorator.id is the name?
AISPRINT_ANNOTATIONS = ['component_name', 'exec_time', 'expected_throughput', 
                        'partitionable_model', 'device_constraints', 'early_exits_model', 'annotation']


class QoSAnnotationsParser():
    def __init__(self, python_file):
        self.python_file = python_file

    def _find_decorators(self, tree, func_name):

        ''' Get all the decorators of a specific function.

            Return: 
                List of all the decorators of the function.

            Parameters: 
                tree (ast): ast tree of the Python code.
                func_name (str): name of the desired decorated function.
        '''
        
        # Retrieve all the function definitions in the provided Python file represented by the 
        # input ast and filter them by selecting only the function with name 'func_name'.
        func = [p for p in tree.body if (isinstance(p, ast.FunctionDef) and p.name == func_name)]
        
        # Some error checking
        if not func:
            # No 'func_name' functions (not allowed)
            raise RuntimeError("Python file {} does not contain a function named '{}'. ".format(self.python_file, func_name) +
                "Number of '{}' functions expected: 1. Please provide a valid AI-SPRINT's component.".format(func_name))
        elif len(func) > 1:
            # Multiple 'func_name' functions (not allowed)
            raise RuntimeError("Python file '{}' contains multiple functions named '{}'. ".format(self.python_file, func_name) +
                "Number of '{}' functions expected: 1. Please provide a valid AI-SPRINT's component.".format(func_name))
        
        # Get the ast.FunctionDef of the desired 'func_name' function, 
        # which is the first and only element of the list.
        func = func[0]

        # Get the list of all the decorators associated to the function
        return func.decorator_list

    def _get_aisprint_annotations(self, decorators):

        ''' Filter out non-AI-SPRINT annotations. 
            Furthermore, if a single 'annotation' annotation exists, it is used and the others are discarded.

            Return: AI-SPRINT decorators' nodes

            Parameters:
                decorators (ast.FunctionDef.decorator_list): original list of ast decorators.
        '''

        new_decorators = []

        decorators_names = []

        num_decorators = len(decorators)

        for decorator in decorators:
            # TODO: other type could exists? Double check
            if isinstance(decorator, ast.Call):
                # Here we can have 2 kinds of annotations:
                # 1. Attribute: @aisprint.annotations.ANNOTATION
                if isinstance(decorator.func, ast.Attribute):
                    decorator_id = decorator.func.attr
                else:
                    # 2. Direct: @ANNOTATION
                    decorator_id = decorator.func.id
            elif isinstance(decorator, ast.Name):
                decorator_id = decorator.id
                # AI-SPRINT decorators are callable
                if decorator_id in AISPRINT_ANNOTATIONS:
                    raise RuntimeError("Forgot '( )' parenthesis in '{}' annotation.".format(decorator_id))
            
            if decorator_id in AISPRINT_ANNOTATIONS:
                decorators_names.append(decorator_id)

                # If 'annotation' discard the other annotations
                if decorator_id == 'annotation':
                    # TODO: found single-mode annotation (WARNING)
                    new_decorators = [decorator]
                else:
                    new_decorators.append(decorator)
        
        # Check duplicated annotations
        # If it does, then it means that two QoS annotations with the same name
        # have been provided. TODO: Double check if we need this
        unique_annotations, counts = np.unique(decorators_names, return_counts=True)
        if num_decorators > len(unique_annotations):
            raise RuntimeError("Found duplicates of the following annotations: " +
                " ".join(unique_annotations[counts > 1])) 

        return new_decorators, decorators_names
    
    def _get_decorator_arguments(self, decorators):

        ''' Get annotations from single decorator. 

            Return: a Python dictionary with decorator names as keys and decorator arguments as values.

            Parameters:
                decorators (ast.FunctionDef.decorator_list): list of ast decorators.
        '''
        # NOTE: the following ast classification works with Python 3.8
        # it may change in other versions.

        decorator = decorators[0]

        # Initialize the decorator dictionary to produced as output
        return_dict = {}
        
        # Fill the dictionary
        decorators_names = decorator.args[0].keys
        decorators_arguments = decorator.args[0].values
        for decorator_id, decorator_arguments in zip(decorators_names, decorators_arguments):
            # Get annotation name
            decorator_id = decorator_id.value

            # Initialize item as an empty decorator
            return_dict[decorator_id] = {}

            # Save the arguments
            for kw_arg, kw_value in zip(decorator_arguments.keys, decorator_arguments.values):
                # ast.Constant refers
                if isinstance(kw_value, ast.Constant):
                    return_dict[decorator_id][kw_arg.value] = kw_value.value
                elif isinstance(kw_value, ast.List):
                    if kw_value.elts != []:
                        return_dict[decorator_id][kw_arg.value] = [
                            s.value.lstrip() if isinstance(s, str) else s.value for s in kw_value.elts]
                    elif kw_value.elts == []:
                        return_dict[decorator_id][kw_arg.value] = []
            
        return return_dict

    def _get_decorators_arguments(self, decorators):

        ''' Get decorators and their arguments.

            Return: a Python dictionary with decorator names as keys and decorator arguments as values.

            Parameters:
                decorators (ast.FunctionDef.decorator_list): list of ast decorators.
        '''
        # NOTE: the following ast classification works with Python 3.8
        # it may change in other versions.

        # Initialize the decorator dictionary to produced as output
        return_dict = {}
        
        # Fill the dictionary
        for decorator in decorators:
            # Get annotation name
            # Here we can have 2 kinds of annotations:
            # 1. Attribute: @aisprint.annotations.ANNOTATION
            if isinstance(decorator.func, ast.Attribute):
                decorator_id = decorator.func.attr
            else:
                # 2. Direct: @ANNOTATION
                decorator_id = decorator.func.id

            # Initialize item as an empty decorator
            return_dict[decorator_id] = {}

            # Save the arguments
            for kw in decorator.keywords:
                # ast.Constant refers
                if isinstance(kw.value, ast.Constant):
                    return_dict[decorator_id][kw.arg] = kw.value.value
                elif isinstance(kw.value, ast.List):
                    if kw.value.elts != []:
                        return_dict[decorator_id][kw.arg] = [
                            s.value.lstrip() if isinstance(s, str) else s.value for s in kw.value.elts]
                    elif kw.value.elts == []:
                        return_dict[decorator_id][kw.arg] = []
            
        return return_dict

    def parse(self):
        
        with open(self.python_file, 'r') as f:
            tree = ast.parse(f.read()) 

        decorators = self._find_decorators(tree, 'main')

        decorators, decorators_names = self._get_aisprint_annotations(decorators)
        
        if len(decorators) == 1 and 'annotation' in decorators_names:
            decorators_dict = self._get_decorator_arguments(decorators)
        else:
            decorators_dict = self._get_decorators_arguments(decorators)
        
        return decorators_dict