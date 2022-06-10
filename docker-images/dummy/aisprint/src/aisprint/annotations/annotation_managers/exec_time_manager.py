import yaml 

from .annotation_manager import AnnotationManager


class ExecTimeManager(AnnotationManager):

    ''' 'exec_time' annotation manager.

        It provides the methods to read the time constraints from the defined annotations.

        Parameters:
            annotations_file (str): complete path to the parsed annotations' file.
        
        NOTE: the annotations are assumed to be already parsed by the QoSAnnotationsParser, which
        also check errors in the annotations' format.
    '''

    def process_annotations(self):
        ''' Process annotations dictionary to compute the local/global time constraints.
        '''
        local_constraints = {}
        global_constraints = {}
        
        for _, annotations in self.annotations.items():
            if 'exec_time' in annotations.keys():
                current_component = annotations['component_name']['name']
                exec_time_annot = annotations['exec_time']
                # check if it is a local or global constraint
                if 'prev_components' in exec_time_annot.keys(): # global constraint
                    if 'global_time_thr' in exec_time_annot.keys():
                        global_constraints[current_component] = {
                            'components': exec_time_annot['prev_components'] + [current_component],
                            'threshold': exec_time_annot['global_time_thr']
                        }
                    else:
                        # NOTE: Actually this is a 'dead' code, since the parser already provided an error in this case.
                        continue
                    if 'local_time_thr' in exec_time_annot.keys(): # local constraint (on the last component)
                        local_constraints[current_component] = {
                            'threshold': exec_time_annot['local_time_thr']
                        }
                elif 'local_time_thr' in exec_time_annot.keys(): # local constraint
                    local_constraints[current_component] = {
                        'threshold': exec_time_annot['local_time_thr']
                    }

        constraints_dict = {'System': {'name': self.application_name}}
        constraints_dict['System']['LocalConstraints'] = local_constraints
        constraints_dict['System']['GlobalConstraints'] = global_constraints
        return constraints_dict

    def generate_configuration_file(self, filename):
        constraints_dict = self.process_annotations()
        with open(filename, 'w') as f:
            yaml.dump(constraints_dict, f)