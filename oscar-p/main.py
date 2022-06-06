# this file calls all methods needed for the whole campaign
# tried to keep it as slim as possible for easier reading (failed miserably)

import os

from termcolor import colored

from cluster_manager import remove_all_buckets, clean_all_logs, generate_fdl_configuration, apply_fdl_configuration, \
    apply_cluster_configuration, generate_fdl_single_service, remove_all_services, create_bucket, \
    recreate_output_buckets
from input_file_processing import workflow_analyzer, show_workflow, run_scheduler, show_runs, get_cluster_name, \
    get_run_info, get_test_single_components, get_service_by_name, get_use_ml_library
from postprocessing import prepare_runtime_data, plot_runtime_core_graphs, make_runtime_core_csv, merge_csv_of_service, \
    make_runtime_core_csv_for_ml, plot_ml_predictions_graphs, save_dataframes, make_statistics
from process_logs import make_csv_table
from retrieve_logs import pull_logs
from run_manager import move_files_to_input_bucket, wait_services_completion, move_whole_bucket
from utils import show_error, auto_mkdir
from mllibrary_manager import run_mllibrary


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
    working_dir = os.path.join(campaign_dir, run["id"], "full")
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
    working_dir = os.path.join(campaign_dir, run["id"], service["name"])
    os.mkdir(working_dir)
    pull_logs(working_dir, [service])
    make_csv_table(working_dir, [service], run["nodes"])


def final_processing():
    print(colored("\nFinal processing...", "blue"))
    results_dir = campaign_dir + "/Results"
    auto_mkdir(results_dir)

    process_subfolder(results_dir, "full", ordered_services)

    if get_test_single_components():
        for s in ordered_services:
            process_subfolder(results_dir, s["name"], [s])
            # merge_csv_of_service(campaign_name, s["name"])
    print(colored("Done!", "green"))
    
    if get_use_ml_library():
        run_mllibrary(results_dir)
        plot_ml_predictions_graphs(results_dir, ordered_services)


def process_subfolder(results_dir, subfolder, services):
    df, adf = prepare_runtime_data(campaign_dir, subfolder, repetitions, runs, services)
    make_statistics(campaign_dir, results_dir, subfolder, services)
    plot_runtime_core_graphs(results_dir, subfolder, df, adf)
    make_runtime_core_csv(results_dir, subfolder, df)
    make_runtime_core_csv_for_ml(results_dir, subfolder, df, adf, "Interpolation")
    make_runtime_core_csv_for_ml(results_dir, subfolder, df, adf, "Extrapolation")
    save_dataframes(results_dir, subfolder, df, adf)


def test():
    final_processing()
    quit()


ordered_services = workflow_analyzer()  # ordered list of services, with name and input/output buckets
show_workflow(ordered_services)

base, runs, nodes = run_scheduler()
campaign_name, repetitions, cooldown = get_run_info()
show_runs(base, nodes, repetitions)

campaign_dir = "runs-results/" + campaign_name

# test()

if os.path.exists(campaign_dir) and os.path.isdir(campaign_dir):
    show_error("Folder exists. Exiting.")
    quit()

os.mkdir(campaign_dir)
os.system("cp input.yaml " + campaign_dir + "/input.yaml")


cluster_name = get_cluster_name()
# todo all oscar command should specify on which cluster to execute

for run in runs:
    print(colored("\nStarting " + run["id"] + " of " + str(len(runs)), "blue"))
    os.mkdir(os.path.join(campaign_dir, run["id"]))  # creates the working directory

    prepare_cluster()
    start_run_full()
    end_run_full()
    test_single_services()

final_processing()
