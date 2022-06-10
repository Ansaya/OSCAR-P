from .annotation_validator import AnnotationValidator

class PartitionableModelValidator(AnnotationValidator):

    def _check_arguments(self, component_name, arguments):
        # No 'onnx_file' argument (arguments is empty)
        if 'onnx_file' not in arguments:
            raise RuntimeError("'onnx_file' argument required in 'partitionable_model' annotation. ")
        # Check number of arguments
        num_arguments = len(list(arguments.keys()))
        if len(list(arguments.keys())) > 1:
            raise RuntimeError("Annotation 'partitionable_model' takes exactly 1 arguments ({}).".format(num_arguments))
        # Check 'onnx_file' is a string
        if not isinstance(arguments['onnx_file'], str):
            raise TypeError("'onnx_file' argument must be a string.")

    def _check_arguments_validity(self):
        for component_script, annotations in self.annotations.items():
            if 'partitionable_model' in annotations:
                arguments = annotations['partitionable_model']
                try:
                    self._check_arguments(annotations['component_name']['name'], arguments)
                except Exception as e:
                    print("\nAn error occurred while parsing 'partitionable_model' " + 
                          "annotation in the following component: \n{}.\n".format(component_script))
                    raise(e)

    def check_annotation_validity(self):
        super().check_annotation_validity()
