from oscarp.utils import auto_mkdir, ensure_slash_end

global application_dir, campaign_dir, current_deployment_dir, current_work_dir, runs_dir, results_dir, run_name
global resources, components, images, run_parameters, parallelism
global testing_components, testing_units, deployments
global current_deployment, clusters_node_requirements, virtual_infrastructures
global current_deployment_index, current_base_index, current_run_index, current_services, tested_services
global scar_logs_end_indexes
global is_single_service_test, has_lambdas, has_active_lambdas, is_last_run, is_first_launch
global is_debug

"""
    * application_dir: directory of the whole project
    * campaign_dir: directory containing the directories of the deployments
    * current_deployment_dir: directory of the current deployment (eg. deployment_0/)
    * current_work_dir: directory containing runs/ and results/ (eg. full_workflow/, C1@VM1/)
    * has_lambdas: True if in general the deployment includes a Lambda
    * has_active_lambdas: True if a Lambda is being tested, either in the full workflow or alone
"""


def set_application_dir(directory):
    global application_dir
    application_dir = ensure_slash_end(directory)
    return


def make_campaign_dir():
    global campaign_dir
    campaign_dir = application_dir + "oscarp/" + run_parameters["run"]["campaign_dir"]
    campaign_dir = ensure_slash_end(campaign_dir)
    auto_mkdir(campaign_dir)
    return


def set_current_deployment(deployment_index):
    global current_deployment, current_deployment_dir, current_deployment_index
    current_deployment = deployments[deployment_index]
    current_deployment_index = deployment_index
    current_deployment_dir = campaign_dir + "deployment_" + str(deployment_index)
    current_deployment_dir = ensure_slash_end(current_deployment_dir)
    auto_mkdir(current_deployment_dir)


def set_current_work_dir(name):
    global current_deployment_dir, current_work_dir, runs_dir, results_dir, run_name
    run_name = name
    current_work_dir = ensure_slash_end(current_deployment_dir + run_name)
    auto_mkdir(current_work_dir)
    runs_dir = current_work_dir + "runs/"
    results_dir = current_work_dir + "results/"
