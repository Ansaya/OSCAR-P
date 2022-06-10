import os
import shutil

from .annotations.annotations_parser import QoSAnnotationsParser

def parse_dag(dag_file):
    with open(dag_file, 'r') as f:
        dag = f.readlines()
    
    dag_dict = {}

    for idx, pair in enumerate(dag):
        # Get source and target components
        dag_line = pair.split('\n')[0]
        source_c, target_c = pair.split('->')
        source_c = source_c.strip()
        target_c = target_c.strip()

        # Add component in the dag_dict
        if not source_c in dag_dict:
            dag_dict[source_c] = {'next': [target_c]}
        else:
            if not target_c in dag_dict[source_c]['next']:
                dag_dict[source_c]['next'].append(target_c)
        
        if idx == len(dag) - 1:
            dag_dict[target_c] = {'next': []}
    
    # Get total number of components
    num_components = len(dag_dict.keys())

    return dag_dict, num_components
    
def parse_annotations(src_dir):
    # Get the directories of the components
    components_dirs = next(os.walk(src_dir))[1]

    # For each component dir, find annotations in 'main.py' 
    # First check that a 'main.py' exists for each partition
    missing_mains = []
    for component_dir in components_dirs:
        filenames = next(os.walk(os.path.join(src_dir, component_dir)))[2]

        if 'main.py' not in filenames:
            missing_mains.append(component_dir)
    
    if missing_mains != []:
        error_msg = "'main.py' script missing for the following components: "
        for mm in missing_mains:
            error_msg += "{}; ".format(mm)
        raise RuntimeError(error_msg)

    # Parse
    annotations_dict = {}
    for component_dir in components_dirs:
        main_script = os.path.join(src_dir, component_dir, 'main.py')

        qos_annot_parser = QoSAnnotationsParser(main_script) 
        annotations_dict[main_script] = qos_annot_parser.parse()
    
    return annotations_dict

def copy_project_structure(source_dir, destination_dir):
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    shutil.copytree(os.path.join(source_dir, 'src'), 
                    os.path.join(destination_dir, 'src'))
    shutil.copytree(os.path.join(source_dir, 'space4ai-d'), 
                    os.path.join(destination_dir, 'space4ai-d'))
    shutil.copytree(os.path.join(source_dir, 'oscar'), 
                    os.path.join(destination_dir, 'oscar'))
    shutil.copytree(os.path.join(source_dir, 'onnx'), 
                    os.path.join(destination_dir, 'onnx'))
    shutil.copytree(os.path.join(source_dir, 'pycomps'), 
                    os.path.join(destination_dir, 'pycomps'))
    shutil.copytree(os.path.join(source_dir, 'im'), 
                    os.path.join(destination_dir, 'im'))
    shutil.copytree(os.path.join(source_dir, 'ams'), 
                    os.path.join(destination_dir, 'ams'))