import copy
import os
import time

from termcolor import colored

from infrastructure_manager import create_virtual_infrastructures, adjust_physical_infrastructures_configuration, \
    update_virtual_infrastructures, delete_all_virtual_infrastructures, delete_unused_virtual_infrastructures
from input_files_parser import get_resources, get_run_parameters, get_components_and_images, get_testing_components
from deployment_generator import get_testing_units, get_deployments, reorder_deployments, \
    make_deployments_summary, make_cluster_requirements, manage_deployment_dirs, \
    manage_runs_dir, deployment_has_all_results, get_services_to_test
from lambda_manager import setup_scar, remove_all_lambdas
from run_coordinator import make_oscar_p_input_file, make_services_list, make_oscar_p_input_file_single
from oscarp.utils import auto_mkdir, show_fatal_error, show_warning

import oscarp.oscarp as oscarp
import global_parameters as gp


def main(input_dir, is_dry):

    gp.is_debug = True
    gp.is_dry = is_dry
    gp.set_application_dir(input_dir)

    infrastructure_id_override = "b4b8ae4c-a797-11ed-b873-e65ce69e943c"
    oscarp.executables.init(os.path.join(os.path.dirname(os.path.abspath(__file__)), "./oscarp/executables/"))

    if not gp.is_debug:
        infrastructure_id_override = None

    # get the necessary info from the different input file
    get_resources()  # uses common_config/candidate_resources.yaml
    get_components_and_images()  # uses common_config/candidate_deployments.yaml
    get_run_parameters()  # uses oscarp/run_parameters

    # merge some of those info together (check graph in coordinator.png)
    get_testing_components()
    get_testing_units()

    # create list of deployments, rearrange them as necessary
    get_deployments()
    # deployments = reorder_deployments(deployments, resources)  # todo make sure this works before re-enabling

    # set the stage for the campaign
    gp.make_campaign_dir()
    make_deployments_summary()

    if not gp.is_dry:
        delete_all_virtual_infrastructures()
    gp.virtual_infrastructures = {}
    gp.is_first_launch = True  # todo rename it, this is used by the GUI to print the summary

    # # # # # # # # # # #
    # DEPLOYMENTS LOOP  #
    # # # # # # # # # # #

    for deployment_index in range(manage_deployment_dirs(), len(gp.deployments)):
        print("\nTesting deployment_" + str(deployment_index) + ":")

        gp.set_current_deployment(deployment_index)
        make_cluster_requirements()

        print("Cluster requirements: ", gp.clusters_node_requirements)  # todo keep this and improve it

        base_length = len(list(gp.clusters_node_requirements.items())[0][1]["nodes"])  # todo ugly af, change
        repetitions = gp.run_parameters["run"]["repetitions"]

        """
        if base_length * repetitions < 14:
            if not gp.is_debug:
                show_warning("Not enough runs to generate ML models, increase base runs or repetitions")
        """

        gp.run_parameters["run"]["main_dir"] = gp.application_dir
        gp.run_parameters["run"]["campaign_dir"] = gp.campaign_dir
        gp.run_parameters["run"]["run_name"] = "deployment_" + str(deployment_index)

        make_services_list()

        # Infrastructure Manager
        create_virtual_infrastructures(infrastructure_id_override)  # todo do something with override

        # Lambdas
        setup_scar()

        # # # # # # #
        # RUNS LOOP #
        # # # # # # #

        manage_runs_dir()

        while gp.current_base_index < base_length or deployment_has_all_results(gp.current_deployment_index) is False:

            # set the stage for the current deployment, including creating the deployment directory
            gp.set_current_work_dir("Full_workflow")
            gp.has_active_lambdas = gp.has_lambdas

            if gp.current_base_index < base_length:
                # adjust_physical_infrastructures_configuration()  # todo_rbf
                if gp.is_debug:  # todo this is temporary, replace with dry
                    adjust_physical_infrastructures_configuration()
                # print(colored("! Updating cluster", "magenta"))
                update_virtual_infrastructures()
            else:
                gp.current_base_index = base_length - 1

            gp.is_last_run = (gp.current_base_index + 1 == base_length)
            gp.is_single_service_test = False

            # todo rename main_dir to application_dir
            # todo will get rid of the input file altogether
            make_oscar_p_input_file()
            oscarp.main()

            # # # # # # # # #
            # SERVICES LOOP #
            # # # # # # # # #

            gp.tested_services = []
            # auto_mkdir(gp.current_deployment_dir + "single_services/")
            services_in_deployment, services_to_test = get_services_to_test()
            gp.is_single_service_test = True

            print("\nTesting services of deployment_" + str(deployment_index) + ":")

            for j in range(len(services_in_deployment)):
                s = services_in_deployment[j]
                if s in services_to_test:  # and s[0].split("@")[1] != "AWS Lambda":
                    gp.run_parameters["run"]["campaign_dir"] = gp.current_deployment_dir + "single_services/"
                    # todo line above is wrong, campaign dir should contain the deployment dirs
                    gp.run_parameters["run"]["run_name"] = s
                    # gp.current_deployment_dir += "single_services/" + s + "/"

                    gp.has_active_lambdas = (s.split("@")[1] == "AWS Lambda")
                    gp.set_current_work_dir(s)

                    make_oscar_p_input_file_single(s, service_number=j)
                    oscarp.main()
                    print()

            time.sleep(5)
            gp.current_base_index += 1

    delete_all_virtual_infrastructures()
    if gp.has_lambdas:
        remove_all_lambdas()


if __name__ == '__main__':
    main("Gordon_project", False)
