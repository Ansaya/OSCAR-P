from .annotation_validator import AnnotationValidator

class ExecTimeValidator(AnnotationValidator):

    def _check_arguments(self, component_name, arguments):

        # Check 'global_time_thr' is defined -> 'prev_components' is not empty
        # --------------------------------------------------------------------
        if ('global_time_thr' in arguments and 'prev_components' not in arguments):
            raise Exception("Global constraints cannot be defined without providing " +
                "the list of previous components. Please provide a non-empty 'prev_components' argument. ")

        # Check number of arguments
        # -------------------------
        num_arguments = len(list(arguments.keys()))
        if num_arguments > 3:
            raise Exception("Annotation 'exec_time' takes up to 3 arguments ({}).".format(num_arguments))

        # Check allowed arguments 
        # -----------------------
        for arg in arguments.keys():
            if arg not in ['local_time_thr', 'global_time_thr', 'prev_components']:
                raise Exception("Argument {} not in the list of allowed arguments. ".format(arg))
            
        # Check local constraint argument
        # -------------------------------
        if 'local_time_thr' in arguments:
            # Check 'local_time_thr' is a float or int
            if not (isinstance(arguments['local_time_thr'], int) or 
                        isinstance(arguments['local_time_thr'], float)):
                raise Exception("'local_time_thr' argument must be a float or int.")

        # Check global constraint argument
        # -------------------------------
        if 'global_time_thr' in arguments:
            # Check 'global_time_thr' is a float or int
            if not (isinstance(arguments['global_time_thr'], int) or 
                        isinstance(arguments['global_time_thr'], float)):
                raise Exception("'global_time_thr' argument must be a float or int.")

            # If a global constraint is defined, check the correctness of 'prev_components'
            if 'prev_components' in arguments:
                # Check 'prev_components' is a list
                if not isinstance(arguments['prev_components'], list):
                    raise Exception("'prev_components' argument must be a list of strings.")
                # Check 'prev_components' is not empty
                if arguments['prev_components'] == []:
                    raise Exception("'prev_components' cannot be empty if a global constraint is defined.")
                # Check 'prev_components' does not contain the component itself
                if component_name in arguments['prev_components']:
                        raise Exception("'prev_components' cannot contain the component itself.")
                # Check elements in 'prev_components' are strings
                if not all(isinstance(pc, str) for pc in arguments['prev_components']):
                    raise Exception("'prev_components' must be a list containing only strings.")
        
                # Use DAG to check for instance that prev_components are consecutive
                # Check that "prev_components[0] -> prev_components[1]" | must be ordered
                for idx, prev_component in enumerate(arguments['prev_components']):
                    if idx == len(arguments['prev_components']) - 1:
                        next_component = component_name 
                    else:
                        next_component = arguments['prev_components'][idx+1]
                    if not component_name in self.dag[prev_component]['next']:
                        raise Exception(
                            "List of previous components does not match the DAG definition." + 
                            "'{}' and '{}' components are non-consecutive.".format(prev_component, component_name))

    def _check_arguments_validity(self):
        for component_script, annotations in self.annotations.items():
            if 'exec_time' in annotations:
                arguments = annotations['exec_time']
                try:
                    self._check_arguments(annotations['component_name']['name'], arguments)
                except Exception as e:
                    print("An error occurred while parsing 'exec_time' " + 
                          "annotation in the following component: \n{}.\n".format(component_script))
                    raise(e)
    
    def _check_constraints(self):
        # Check prev_components are annotated
        # -----------------------------------
        exec_time_annotated = []
        for _, annotations in self.annotations.items():
            component_name = annotations['component_name']['name']
            if 'exec_time' in annotations:
                exec_time_annotated.append(component_name)
        
        for _, annotations in self.annotations.items():
            if 'exec_time' in annotations and 'global_time_thr' in annotations['exec_time']:
                for component in annotations['exec_time']['prev_components']:
                    if not component in exec_time_annotated:
                        raise Exception(
                            "Component '{}' is involved in a global constraint ".format(component) + 
                            "but no 'exec_time' annotation has been found. Please, annotate the component. ")

    def check_annotation_validity(self):
        super().check_annotation_validity()
        self._check_constraints()