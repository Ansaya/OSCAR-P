# all methods related to starting and finishing the runs go in here

import time
import numpy as np

from termcolor import colored

from input_file_processing import get_mc_alias
from utils import execute_command, get_command_output_wrapped, show_error


# wait between the upload of two files, to emulate arrival rate
# not a boolean to allow adding new distributions
def wait_interval(distribution, inter_upload_time):
    wait = 0
    if distribution == "exponential":
        wait = round(float(max(inter_upload_time, np.random.exponential(inter_upload_time, 1))), 2)
    elif distribution == "deterministic":
        wait = inter_upload_time
    return wait


# move files to the input bucket, starting the workflow execution
def move_files_to_input_bucket(input_bucket):
    print(colored("Moving input files...", "yellow"))
    from input_file_processing import get_workflow_input
    storage_bucket = "storage"

    filename, number_of_files, distribution, inter_upload_time, mc_alias = get_workflow_input()
    stripped_filename, extension = filename.split(".")

    for i in range(0, number_of_files):
        move_file_between_buckets(filename, storage_bucket, stripped_filename + "_" + str(i) + "." + extension, input_bucket)

        wait = wait_interval(distribution, inter_upload_time)
        time.sleep(wait)
    print(colored("Done!", "green"))
    return


def move_file_between_buckets(origin_file, origin_bucket, destination_file, destination_bucket):
    mc_alias = get_mc_alias()
    origin = mc_alias + "/" + origin_bucket + "/" + origin_file
    destination = mc_alias + "/" + destination_bucket + "/" + destination_file
    command = "oscar-p/mc cp " + origin + " " + destination
    execute_command(command)
    return


def move_whole_bucket(origin_bucket, destination_bucket):
    print(colored("Duplicating bucket...", "yellow"))
    mc_alias = get_mc_alias()
    origin = mc_alias + "/" + origin_bucket
    destination = mc_alias + "/" + destination_bucket
    command = "oscar-p/mc cp " + origin + " " + destination + " -r"
    execute_command(command)
    print(colored("Done!", "green"))
    return


# given a service, returns the list of jobs with their status
def get_jobs_list(service_name):
    command = "oscar-p/oscar-cli service logs list " + service_name
    logs_list = get_command_output_wrapped(command)

    if logs_list:
        logs_list.pop(0)

    job_list = []

    for line in logs_list:
        segments = line.split('\t')
        job_name = segments[0]
        job_status = segments[1]

        job_list.append({
            "name": job_name,
            "status": job_status
        })

    return job_list


# todo check if this work as intended
def increment_sleep(sleep):
    max_wait = 300
    sleep += round(sleep / 2)
    if sleep > max_wait:
        return max_wait
    return sleep


# awaits the completion of all the jobs before passing to processing
def wait_services_completion(services):
    sleep_interval = 10

    for service in services:
        completed = False
        service_name = service["name"]
        print(colored("Waiting for service " + service_name + " completion...", "yellow"))
        time.sleep(sleep_interval)

        while not completed:
            job_list = get_jobs_list(service_name)
            if job_list:  # means list not empty
                completed = True
                for j in job_list:
                    if j["status"] != "Succeeded":
                        completed = False
                    if j["status"] == "Failed":
                        show_error("Job " + j["name"] + " failed")
                        completed = False
                        quit()
            if not completed:
                time.sleep(increment_sleep(sleep_interval))
        print(colored("Service " + service_name + " completed!", "green"))
    return
