import json
from abc import ABC, abstractmethod

class AnnotationManager(ABC):

    def __init__(self, annotations, application_name):
        self.annotations = annotations 
        self.application_name = application_name

    @abstractmethod
    def process_annotations(self):
        pass

    @abstractmethod
    def generate_configuration_file(self, filename):
        pass