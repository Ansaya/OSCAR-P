import os
import argparse 
import json

from .utils import parse_dag, parse_annotations, copy_project_structure
from .annotations import annotation_managers
from .annotations import annotation_validators


AISPRINT_ANNOTATIONS = ['component_name', 'exec_time', 'expected_throughput', 
                        'partitionable_model', 'device_constraints', 'early_exits_model', 'annotation']


def create_aisprint_designs(application_dir, output_dir):

    """ Execute the AI-SPRINT pipeline to create the possible application designs:
        
        1. Read DAG file
        2. Parse AI-SPRINT annotations
        3. Create 'base' design
        4. Run partitioning tool
            a. Find the possible partitions from partitionable and early-exit models
            b. Create the new components' code from the partitions
            c. Create a new design for each partition
        5. Parse AI-SPRINT annotations for each design
            a. Create new annotation.json file for each design
        6. Run annotations managers for each design 
    """

    print("\n")
    print("# --------- #")
    print("# AI-SPRINT #")
    print("# --------- #")
    print("# --------- #")
    print("#   Design  #")
    print("# --------- #")
    print("\n")
    
    # 1) Read DAG file
    # ----------------
    # DAG file has the same name of the application directory
    application_name = os.path.basename(os.path.normpath(application_dir)) 
    dag_file = os.path.join(application_dir, application_name + '.dag')
    
    print("Parsing application DAG in '{}'..".format(dag_file))
    dag_dict, num_components = parse_dag(dag_file)

    print("* Found {} components in the DAG\n".format(num_components))

    # ----------------

    # 2) Parse annotations
    # --------------------
    print("Parsing AI-SPRINT annotations..")
    src_dir = os.path.join(application_dir , 'src')

    # Get the directories of the components
    components_dirs = next(os.walk(src_dir))[1]

    # Check number of directories is equal to the number of components in the DAG
    if len(components_dirs) != num_components:
        raise RuntimeError(
            "Number of components in the DAG does not match the number of directories in 'src'")
    
    # Parse
    annotations_dict = parse_annotations(src_dir) 
    
    # Check all the components have a 'component_name'
    missing_names = []
    for main_script, annotations in annotations_dict.items():
        if 'component_name' not in annotations:
            missing_names.append(main_script) 
    
    if missing_names != []:
        error_msg = "'component_name' is missing in the following scripts"
        for mn in missing_names:
            error_msg += "{}; ".format(mn)
        raise RuntimeError(error_msg)
    
    # Save annotations
    with open(os.path.join(output_dir, 'annotations.json'), 'w') as f:
        json.dump(annotations_dict, f)
    print("* Annotations stored in {}\n".format(os.path.join(output_dir, 'annotations.json')))
    
    # Validate annotations
    print("Validating AI-SPRINT annotations.. ", end=' ')
    for aisprint_annotation in AISPRINT_ANNOTATIONS:
        if aisprint_annotation == 'annotation':
            continue
        validator_module_name = aisprint_annotation + '_validator'
        validator_class_name = "".join([s.capitalize() for s in validator_module_name.split('_')])
        validator_class = getattr(annotation_validators, validator_class_name)
        annotation_validator = validator_class(annotations_dict, dag_dict)
        annotation_validator.check_annotation_validity()
    print("DONE.\n")

    # -------------------

    # 3) Create base design
    # ---------------------
    print("Creating designs.. ")
    copy_project_structure(application_dir, 
                           os.path.join(application_dir, 'aisprint', 'designs', 'base_design'))
    # By default select the base design as 'current design': create a symbolic link 
    source_design = os.path.join(os.path.abspath(application_dir), 'aisprint', 'designs', 'base_design')
    current_design_symlink = os.path.join(os.path.abspath(application_dir), 'current_design')
    os.symlink(source_design, current_design_symlink)
    # ---------------------

    # 4) Run partitioning tool
    # -------------------------
    print("- Finding partitions.. ", end=' ')
    # TODO: 
    # 1. Run partitionable_model manager
    # 2. Run early_exits_model manager
    # 3. Run partitioning tool and create a design replica for each partition
    #    Both for 'patitionable' and 'early exits' models
    print("DONE.\n")
    # ---------------------------
    
    # 5) Run annotation parser for each design
    # ----------------------------------------
    print("Parsing AI-SPRINT annotations in the created designs.. ")
    designs_dir = os.path.join(application_dir, 'aisprint', 'designs')
    design_names = next(os.walk(designs_dir))[1]
    for design_name in design_names:
        design_dir = os.path.join(designs_dir, design_name)
        print("- Parsing annotations in design: {}.. ".format(design_dir), end=' ')
        design_annotations_dict = parse_annotations(os.path.join(design_dir, 'src'))
        # Save annotations
        with open(os.path.join(design_dir, 'annotations.json'), 'w') as f:
            json.dump(design_annotations_dict, f)
        print("DONE.")
    print("DONE.\n")
        
    # ----------------------------------------

    # 6) Run annotation managers for each design
    # ------------------------------------------
    print("Preparing AI-SPRINT application..", end=' ')
    for design_name in design_names:
        design_dir = os.path.join(designs_dir, design_name)
        # Run exec_time managers
        for aisprint_annotation in AISPRINT_ANNOTATIONS:
            if aisprint_annotation == 'annotation':
                continue
            manager_module_name = aisprint_annotation + '_manager'
            manager_class_name = "".join([s.capitalize() for s in manager_module_name.split('_')])
            manager_class = getattr(annotation_managers, manager_class_name)
            annotation_manager = manager_class(annotations_dict, application_name)
            if aisprint_annotation == 'exec_time':
                configuration_file = os.path.join(design_dir, 'ams', 'qos_constraints.yaml')
            else:
                # TODO: initialize other configuration files
                configuration_file = None
            annotation_manager.generate_configuration_file(configuration_file)
    print("DONE.\n")
    # ------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--application_dir", help="Path to the AI-SPRINT application.", required=True)
    parser.add_argument("--output_dir", help="Path to the output folder for the annotations.json file.")
    args = vars(parser.parse_args())

    application_dir = args['application_dir']
    if not args['output_dir']:
        output_dir = application_dir
    
    create_aisprint_designs()