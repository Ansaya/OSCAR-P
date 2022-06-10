from .annotation_validator import AnnotationValidator

class ComponentNameValidator(AnnotationValidator):

    def _check_arguments(self, component_name, arguments):
        # No 'name' argument (arguments is empty)
        if 'name' not in arguments:
            raise Exception("'name' argument required in 'component_name' annotation. ")
        # Check number of arguments
        num_arguments = len(list(arguments.keys()))
        if len(list(arguments.keys())) > 1:
            raise Exception("Annotation 'component_name' takes exactly 1 arguments ({}).".format(num_arguments))
        # Check 'name' is a string
        if not isinstance(arguments['name'], str):
            raise Exception("'name' argument must be a string.")

    def _check_arguments_validity(self):
        for component_script, annotations in self.annotations.items():
            if 'component_name' in annotations:
                arguments = annotations['component_name']
                try:
                    self._check_arguments(annotations['component_name']['name'], arguments)
                except Exception as e:
                    print("\nAn error occurred while parsing 'component_name' " + 
                          "annotation in the following component: \n{}.\n".format(component_script))
                    raise(e)

    def check_annotation_validity(self):
        super().check_annotation_validity()
