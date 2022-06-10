import json
from abc import ABC, abstractmethod

class AnnotationValidator(ABC):

    def __init__(self, annotations, dag) -> None:
        super().__init__()
        self.annotations = annotations 
        self.dag = dag
    
    @abstractmethod
    def _check_arguments_validity(self):
        pass

    @abstractmethod
    def check_annotation_validity(self):
        self._check_arguments_validity()