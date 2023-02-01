# this file calls all methods needed for the whole campaign
# tried to keep it as slim as possible for easier reading (failed miserably)

import os

import executables

from termcolor import colored

from cluster_manager import remove_all_buckets, generate_fdl_configuration, \
    remove_all_services, apply_fdl_configuration_wrapped, clean_s3_buckets, upload_input_files_to_storage
from gui import show_runs
from input_file_processing import show_workflow, run_scheduler, get_run_info, \
    get_train_ml_models, get_clusters_info, get_simple_services, get_do_final_processing
from postprocessing import prepare_runtime_data, plot_runtime_core_graphs, make_runtime_core_csv, save_dataframes
from process_logs import make_csv_table, make_done_file
from retrieve_logs import pull_logs, pull_scar_logs, get_data_size
from run_manager import move_input_files_to_input_bucket, wait_services_completion, move_input_files_to_s3_bucket
from mllibrary_manager import run_mllibrary
from utils import auto_mkdir, show_warning, delete_directory

global clusters, runs, run, simple_services, repetitions, current_run_dir, banner_name

import global_parameters as gp


def prepare_clusters():
    remove_all_services(clusters)
    if not gp.is_single_service_test:
        remove_all_buckets(clusters)
        if gp.has_active_lambdas:
            clean_s3_buckets()
    generate_fdl_configuration(run["services"], clusters)
    apply_fdl_configuration_wrapped(run["services"])
    if gp.current_base_index == 0 and not gp.is_single_service_test:
        upload_input_files_to_storage(simple_services)


def start_run_full():
    services = run["services"]
    move_input_files_to_input_bucket(services[0])
    wait_services_completion(services)


def end_run_full(current_run_index):
    pull_logs(current_run_dir, simple_services, clusters)
    pull_scar_logs(current_run_dir, simple_services)
    make_csv_table(current_run_dir, run["services"], clusters, current_run_index)
    # download_bucket(campaign_dir + "/Database", "database")
    if gp.is_single_service_test and gp.current_base_index == 0:  # first run and testing a single service
        get_data_size(simple_services)
    make_done_file(current_run_dir)


def test_single_lambda():
    services = run["services"]
    # print(services[0]["input_bucket"])
    move_input_files_to_s3_bucket(services[0]["input_bucket"])
    wait_services_completion(services)


def final_processing():  # TODO this needs to be moved/called elsewhere
    print(colored("\nFinal processing... " + banner_name, "blue"))
    auto_mkdir(gp.results_dir)

    process_subfolder(simple_services)
    # make_done_file(gp.results_dir)

    if gp.is_single_service_test:
        run_mllibrary()  # urgent generate performance models file
    print(colored("Done!", "green"))
    # print("\n\n")
    return


def process_subfolder(services):
    df, adf = prepare_runtime_data(repetitions, runs, services)
    # make_statistics(campaign_dir, results_dir, subfolder, services)
    plot_runtime_core_graphs(gp.results_dir, gp.run_name, df, adf)
    make_runtime_core_csv(gp.results_dir, gp.run_name, df)

    # no longer needed, the full runtime_core is generated elsewhere and the models are no longer tested
    """
    if get_train_ml_models():
        make_runtime_core_csv_for_ml(results_dir, df, adf, "Interpolation")
        make_runtime_core_csv_for_ml(results_dir, df, adf, "Extrapolation")
    """

    save_dataframes(gp.results_dir, gp.run_name, df, adf)


def manage_campaign_dir():  # todo update comment
    """
    if the campaign_dir already exists, and a "Results" folder is present, it exits,
    otherwise it finds the last run (i.e. "Run #11"), it deletes it (it may have failed) and resumes from there
    if the run_dir doesn't exist it creates it and starts as normal
    :return: the index of the next run to execute; this is the index from the list "runs", not the run id (i.e. run
            with index 0 has id "Run #1")
    """

    if os.path.exists(gp.runs_dir):
        if os.path.exists(
                gp.results_dir):  # if there's a Result folder, the specific deployment has been completely tested
            if os.path.exists(gp.results_dir + "done"):
                show_warning("Run completed, skipping...")
                return -1
            else:  # testing is completed but results are not
                delete_directory(gp.results_dir)
                final_processing()
                return -1

        show_warning("Resuming...")

        folder_list = os.listdir(gp.runs_dir)
        s = len(folder_list)

        """
        if folder_list:  # if not empty
            for i in range(1, len(folder_list) + 1):
                if i > gp.current_run_index:
                    target_dir = gp.runs_dir + "Run #" + str(i)
                    delete_directory(target_dir)
        """

    else:
        os.mkdir(gp.runs_dir)
        return 0

    os.system("cp input.yaml '" + gp.current_deployment_dir + "input.yaml'")
    return s


def main():
    # if clean_buckets is true then I'm testing the full workflow
    global clusters, runs, simple_services, repetitions, current_run_dir
    clusters = get_clusters_info()
    base, runs = run_scheduler()
    simple_services = get_simple_services(runs[0]["services"])
    repetitions, cooldown, stop_at_run = get_run_info()
    current_run_index = stop_at_run - 1

    global banner_name
    if gp.run_name == "Full_workflow":
        banner_name = "(full workflow)"
    else:
        service_name = simple_services[0]["name"]
        banner_name = "(" + service_name + ")"

    s = manage_campaign_dir()
    if s == -1:
        return

    for i in range(s, stop_at_run * repetitions):
        global run
        run = runs[i]

        print(colored("\nStarting " + run["id"] + " of " + str(len(runs)) + " " + banner_name, "blue"))

        show_workflow(simple_services)

        if gp.is_first_launch:
            show_runs(base, repetitions, clusters)
            gp.is_first_launch = False

        current_run_dir = os.path.join(gp.runs_dir, run["id"])
        auto_mkdir(current_run_dir)
        simple_services = get_simple_services(run["services"])

        # if not clean_buckets and len(run["services"]) == 1 and run["services"][0]["cluster"] == "AWS Lambda":
        if gp.is_single_service_test and gp.has_active_lambdas:
            test_single_lambda()
        else:
            prepare_clusters()
            start_run_full()

        end_run_full(current_run_index)

    if gp.is_last_run:
        final_processing()

    return


if __name__ == '__main__':
    main()
