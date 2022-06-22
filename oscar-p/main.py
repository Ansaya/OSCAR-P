# this file calls all methods needed for the whole campaign
# tried to keep it as slim as possible for easier reading (failed miserably)

import os

from termcolor import colored

from cluster_manager import remove_all_buckets, clean_all_logs, generate_fdl_configuration, apply_cluster_configuration, \
    generate_fdl_single_service, remove_all_services, create_bucket, apply_fdl_configuration_wrapped, \
    recreate_output_buckets, set_default_oscar_cluster
from input_file_processing import show_workflow, run_scheduler, show_runs, get_cluster_name, \
    get_run_info, get_test_single_components, get_service_by_name, get_use_ml_library, get_clusters_info, \
    consistency_check, get_simple_services
from postprocessing import prepare_runtime_data, plot_runtime_core_graphs, make_runtime_core_csv, \
    make_runtime_core_csv_for_ml, plot_ml_predictions_graphs, save_dataframes, make_statistics
from process_logs import make_csv_table
from retrieve_logs import pull_logs
from run_manager import move_files_to_input_bucket, wait_services_completion, move_whole_bucket, download_bucket
from utils import show_error, auto_mkdir, show_warning, delete_directory
# from mllibrary_manager import run_mllibrary


def prepare_clusters():
    remove_all_services(clusters)
    remove_all_buckets(clusters)
    apply_cluster_configuration(run, clusters)
    generate_fdl_configuration(run, clusters)
    apply_fdl_configuration_wrapped(run["services"], clusters)


def start_run_full():
    move_files_to_input_bucket(run["services"][0])
    wait_services_completion(simple_services, clusters)


def end_run_full():
    working_dir = os.path.join(campaign_dir, run["id"], "full")
    os.mkdir(working_dir)
    pull_logs(working_dir, simple_services, clusters)
    make_csv_table(working_dir, run, clusters)
    # download_bucket(campaign_dir + "/Database", "database")


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
    apply_fdl_configuration_wrapped([service])
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


def manage_campaign_dir():
    """
    if the campaign_dir already exists, and a "Results" folder is present, it exits,
    otherwise it finds the last run (i.e. "Run #11"), it deletes it (it may have failed) and resumes from there
    if the campaign_dir doesn't exist it creates it and starts as normal
    :return: the index of the next run to execute; this is the index from the list "runs", not the run id (i.e. run
            with index 0 has id "Run #1")
    """
    if os.path.exists(campaign_dir) and os.path.isdir(campaign_dir):
        folder_list = os.listdir(campaign_dir)
        if "Results" in folder_list:
            show_error("Folder exists, exiting.")
            quit()

        show_warning("Folder exists, resuming...")
        folder_list.remove("input.yaml")

        return len(folder_list)  # todo temporary, use for the long blur-faces then remove

        n = len(folder_list) - 1

        last_run = campaign_dir + "/" + runs[n]["id"]
        delete_directory(last_run)

    else:
        n = 0
        os.mkdir(campaign_dir)
        os.system("cp input.yaml " + campaign_dir + "/input.yaml")

    return n


def test():
    service_name = "mask-detector"
    service = get_service_by_name(service_name, run["services"])
    wait_services_completion([service])
    end_run_service(service)
    quit()


clusters = get_clusters_info()
base, runs = run_scheduler()
simple_services = get_simple_services(runs[0]["services"])
campaign_name, repetitions, cooldown = get_run_info()

consistency_check(simple_services)
show_workflow(simple_services)
show_runs(base, repetitions, clusters)

campaign_dir = "runs-results/" + campaign_name

# run = runs[0]
# test()

s = manage_campaign_dir()


for i in range(s, len(runs)):
    run = runs[i]
    print(colored("\nStarting " + run["id"] + " of " + str(len(runs)), "blue"))
    os.mkdir(os.path.join(campaign_dir, run["id"]))  # creates the working directory
    simple_services = get_simple_services(run["services"])

    prepare_clusters()
    start_run_full()
    end_run_full()
    test_single_services()

final_processing()
