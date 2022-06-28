# all methods related to starting and finishing the runs go in here

import time
import numpy as np

from termcolor import colored
from tqdm import tqdm

from input_file_processing import get_workflow_input
from cluster_manager import set_default_oscar_cluster_from_alias
from utils import get_command_output_wrapped, show_error, auto_mkdir


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
# todo what happens if file is not there? it should be uploaded by OSCAR-P, add function
def move_input_files_to_input_bucket(service):
    print(colored("Moving input files...", "yellow"))
    minio_alias = service["minio_alias"]
    storage_bucket = minio_alias + "/" + "storage"
    input_bucket = minio_alias + "/" + service["input_bucket"]
    
    filename, number_of_files, distribution, inter_upload_time = get_workflow_input()
    stripped_filename, extension = filename.split(".")

    for i in range(0, number_of_files):
        destination_file = stripped_filename + "_" + str(i) + "." + extension
        move_file_between_buckets(filename, storage_bucket, destination_file, input_bucket)

        wait = wait_interval(distribution, inter_upload_time)
        time.sleep(wait)
    print(colored("Done!", "green"))
    return


def move_input_files_to_dead_start_bucket(service):
    print(colored("Moving input files...", "yellow"))
    minio_alias = service["minio_alias"]
    storage_bucket = minio_alias + "/" + "storage"
    input_bucket = minio_alias + "/" + "dead-start"

    filename, number_of_files, distribution, inter_upload_time = get_workflow_input()
    stripped_filename, extension = filename.split(".")

    for i in tqdm(range(0, number_of_files)):
        destination_file = stripped_filename + "_" + str(i) + "." + extension
        move_file_between_buckets(filename, storage_bucket, destination_file, input_bucket)

        wait = wait_interval(distribution, inter_upload_time)
        time.sleep(wait)
    print(colored("Done!", "green"))
    return


# origin and destination bucket must already include the minio_alias, i.e. "minio-vm/storage"
def move_file_between_buckets(origin_file, origin_bucket, destination_file, destination_bucket):
    origin = origin_bucket + "/" + origin_file
    destination = destination_bucket + "/" + destination_file
    command = "oscar-p/mc cp " + origin + " " + destination
    get_command_output_wrapped(command)
    return


def move_bucket_to_dead_start_bucket(service):
    print(colored("Duplicating bucket...", "yellow"))
    minio_alias = service["minio_alias"]
    origin = minio_alias + "/" + service["input_bucket"]
    destination = minio_alias + "/" + "dead-start"
    set_default_oscar_cluster_from_alias(service["oscarcli_alias"])
    command = "oscar-p/mc cp " + origin + " " + destination + " -r"
    get_command_output_wrapped(command)
    print(colored("Done!", "green"))
    return


# given a service, returns the list of jobs with their status
def get_jobs_list(service_name, oscarcli_alias):

    set_default_oscar_cluster_from_alias(oscarcli_alias)

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
        # cluster = get_active_cluster(service, clusters)
        print(colored("Waiting for service " + service_name + " completion...", "yellow"))
        time.sleep(sleep_interval)
        
        job_list = get_jobs_list(service_name, service["oscarcli_alias"])
        pbar = tqdm(total=len(job_list))
        
        while not completed:
            i = len(job_list)
            if job_list:  # means list not empty
                completed = True
                for j in job_list:
                    if j["status"] != "Succeeded":
                        completed = False
                        i -= 1
                    if j["status"] == "Failed":
                        show_error("Job " + j["name"] + " failed")
                        completed = False
                        quit()
            pbar.n = i
            pbar.refresh()
            if not completed:
                time.sleep(increment_sleep(sleep_interval))
                job_list = get_jobs_list(service_name, service["oscarcli_alias"])
        pbar.close()
        print(colored("Service " + service_name + " completed!", "green"))
    return
    

def download_bucket(destination, bucket_name):
    print(colored("Downloading bucket...", "yellow"))
    mc_alias = get_mc_alias()
    origin = mc_alias + "/" + bucket_name
    auto_mkdir(destination)
    command = "oscar-p/mc cp " + origin + " " + destination + " -r"
    get_command_output_wrapped(command)
    print(colored("Done!", "green"))
    

