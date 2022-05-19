# this file calls all methods needed for the whole campaign
# tried to keep it as slim as possible for easier reading (failed miserably)

import os
import configparser

from termcolor import colored

from cluster_manager import remove_all_buckets, clean_all_logs, generate_fdl_configuration, apply_fdl_configuration, \
    apply_cluster_configuration, generate_fdl_single_service, remove_all_services, create_bucket, \
    recreate_output_buckets
from input_file_processing import workflow_analyzer, show_workflow, run_scheduler, show_runs, get_cluster_name, \
    get_run_info, get_test_single_components, get_service_by_name
from postprocessing import prepare_runtime_data, plot_runtime_core_graph, make_runtime_core_csv, merge_csv_of_service
from process_logs import make_csv_table
from retrieve_logs import pull_logs
from run_manager import move_files_to_input_bucket, wait_services_completion, move_whole_bucket
from utils import show_error

from MLlibrary import sequence_data_processing


def prepare_cluster():
    remove_all_services()
    remove_all_buckets()
    apply_cluster_configuration(run)
    generate_fdl_configuration(run, cluster_name)
    apply_fdl_configuration()


def start_run_full():
    move_files_to_input_bucket(ordered_services[0]["input"])
    wait_services_completion(ordered_services)


def end_run_full():
    working_dir = os.path.join(campaign_name, run["id"], "full")
    os.mkdir(working_dir)
    pull_logs(working_dir, ordered_services)
    make_csv_table(working_dir, run["services"], run["nodes"])


def test_single_services():
    if not get_test_single_components():
        return

    create_bucket("dead-start")
    is_first_service = True
    for i in range(len(ordered_services)):
        start_run_service(ordered_services[i]["name"], is_first_service)
        end_run_service(get_service_by_name(ordered_services[i]["name"], run["services"]))
        is_first_service = False


def start_run_service(service_name, is_first_service):
    print(colored("\nStarting " + run["id"] + " - " + service_name, "blue"))
    clean_all_logs()
    service = get_service_by_name(service_name, run["services"])

    remove_all_services()
    generate_fdl_single_service(service, cluster_name)
    apply_fdl_configuration()
    recreate_output_buckets(service)

    if is_first_service:
        move_files_to_input_bucket("dead-start")
    else:
        move_whole_bucket(service["input_bucket"], "dead-start")

    wait_services_completion([service])


def end_run_service(service):
    working_dir = os.path.join(campaign_name, run["id"], service["name"])
    os.mkdir(working_dir)
    pull_logs(working_dir, [service])
    make_csv_table(working_dir, [service], run["nodes"])


def final_processing():
    print(colored("\nFinal processing...", "blue"))
    os.mkdir(campaign_name + "/Results")
    process_subfolder("full", ordered_services)
    if get_test_single_components():
        for s in ordered_services:
            process_subfolder(s["name"], [s])
            merge_csv_of_service(campaign_name, s["name"])
    print(colored("Done!", "green"))
    # run_mllibrary()


def process_subfolder(subfolder, services):
    df, adf = prepare_runtime_data(campaign_name, subfolder, repetitions, runs, services)
    plot_runtime_core_graph(campaign_name, subfolder, df, adf)
    make_runtime_core_csv(campaign_name, subfolder, df)


def run_mllibrary():
    print(colored("\nGenerating models...", "blue"))
    base_dir = campaign_name + "/Results/"

    results = os.listdir(base_dir)
    for r in results:
        if ".csv" in r and "runtime" in r:
            filepath = base_dir + r

            # with SFS
            config_file = "MLlibrary/MLlibrary-config-SFS.ini"
            set_mllibrary_config_path(config_file, filepath)
            output_dir = base_dir + r.strip(".csv") + "_model_SFS"
            sequence_data_processor = sequence_data_processing.SequenceDataProcessing(config_file, output=output_dir)
            sequence_data_processor.process()

            # without SFS
            config_file = "MLlibrary/MLlibrary-config-noSFS.ini"
            set_mllibrary_config_path(config_file, filepath)
            output_dir = base_dir + r.strip(".csv") + "_model_noSFS"
            sequence_data_processor = sequence_data_processing.SequenceDataProcessing(config_file, output=output_dir)
            sequence_data_processor.process()
    print(colored("Done!", "green"))


def set_mllibrary_config_path(config_file, filepath):
    parser = configparser.ConfigParser()
    parser.read(config_file)

    parser.set("DataPreparation", "input_path", filepath)

    with open(config_file, "w") as file:
        parser.write(file)


def test():
    config_file = "MLlibrary-config-SFS.ini"
    filepath = "full-test/Results/runtime_core_mask-detector.csv"
    set_mllibrary_config_path(config_file, filepath)
    output_dir = "full-test/Results/runtime_core_mask-detector_model_xx"
    sequence_data_processor = sequence_data_processing.SequenceDataProcessing(config_file, output=output_dir)
    sequence_data_processor.process()
    quit()


# test()

ordered_services = workflow_analyzer()  # ordered list of services, with name and input/output buckets
show_workflow(ordered_services)

base, runs, nodes = run_scheduler()
campaign_name, repetitions, cooldown = get_run_info()
show_runs(base, nodes, repetitions)

campaign_name = "runs-results/" + campaign_name

if os.path.exists(campaign_name) and os.path.isdir(campaign_name):
    show_error("Folder exists. Exiting.")
    quit()

os.mkdir(campaign_name)
os.system("cp input.yaml " + campaign_name + "/input.yaml")


cluster_name = get_cluster_name()
# todo all oscar command should specify on which cluster to execute

for run in runs:
    print(colored("\nStarting " + run["id"], "blue"))
    os.mkdir(os.path.join(campaign_name, run["id"]))  # creates the working directory

    prepare_cluster()
    start_run_full()
    end_run_full()
    test_single_services()

final_processing()
