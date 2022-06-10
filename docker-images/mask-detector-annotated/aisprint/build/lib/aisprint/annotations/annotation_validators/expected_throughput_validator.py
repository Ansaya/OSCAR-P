from .annotation_validator import AnnotationValidator

class ExpectedThroughputValidator(AnnotationValidator):

    def _check_arguments(self, component_name, arguments):
        # No 'rate' argument (arguments is empty)
        if 'rate' not in arguments:
            raise RuntimeError("'rate' argument required in 'expected_througput' annotation. ")
        # Check number of arguments
        num_arguments = len(list(arguments.keys()))
        if len(list(arguments.keys())) > 1:
            raise RuntimeError("Annotation 'expected_throughput' takes exactly 1 arguments ({}).".format(num_arguments))
        # Check 'rate' is a float or int
        if (not isinstance(arguments['rate'], int) or not isinstance(arguments['rate'], float)):
            raise TypeError("'rate' argument must be a float or int.")

    def _check_arguments_validity(self):
        for component_script, annotations in self.annotations.items():
            if 'expected_thrpughput' in annotations:
                arguments = annotations['expected_throughput']
                try:
                    self._check_arguments(annotations['component_name']['name'], arguments)
                except Exception as e:
                    print("\nAn error occurred while parsing 'component_name' " + 
                          "annotation in the following component: \n{}.\n".format(component_script))
                    raise(e)

    def check_annotation_validity(self):
        super().check_annotation_validity()
