# all methods related to starting and finishing the runs go in here

import time
import numpy as np

import executables

from termcolor import colored
from tqdm import tqdm

from input_file_processing import get_workflow_input
from cluster_manager import set_default_oscar_cluster_from_alias
from oscarp.retrieve_logs import get_scar_log
from utils import get_command_output_wrapped, show_error, auto_mkdir

import global_parameters as gp


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
def move_input_files_to_input_bucket(service):  # todo branch, should handle differently lambdas
    print(colored("Moving input files...", "yellow"))
    storage_bucket, filename, batch_size, number_of_batches, distribution, inter_upload_time = get_workflow_input()

    minio_alias = service["storage_provider_alias"]

    # if no filename is specified I just clone the whole storage bucket
    # used for example when testing a single service to clone the output bucket fo the previous service
    if filename is None:
        duplicate_bucket(service, storage_bucket, service["input_bucket"])
        return

    stripped_filename, extension = filename.split(".")
    input_bucket = minio_alias + "/" + service["input_bucket"]
    storage_bucket = minio_alias + "/" + "storage"

    for i in range(0, number_of_batches):
        for j in range(0, batch_size):
            destination_file = stripped_filename + "_" + str(i*batch_size+j) + "." + extension
            move_file_between_buckets(filename, storage_bucket, destination_file, input_bucket)

        wait = wait_interval(distribution, inter_upload_time)
        time.sleep(wait)
    print(colored("Done!", "green"))
    return


"""
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
"""


# origin and destination bucket must already include the minio_alias, i.e. "minio-vm/storage"
def move_file_between_buckets(origin_file, origin_bucket, destination_file, destination_bucket):
    origin = origin_bucket + "/" + origin_file
    destination = destination_bucket + "/" + destination_file
    # command = mc + "cp " + origin + " " + destination
    command = executables.mc.get_command("cp " + origin + " " + destination)
    get_command_output_wrapped(command)
    return


def duplicate_bucket(service, source_bucket, destination_bucket):
    print(colored("Duplicating bucket...", "yellow"))
    minio_alias = service["storage_provider_alias"]
    source = minio_alias + "/" + source_bucket
    destination = minio_alias + "/" + destination_bucket
    set_default_oscar_cluster_from_alias(service["oscarcli_alias"])
    # command = mc + "cp " + source + "/ " + destination + " -r"
    command = executables.mc.get_command("cp " + source + "/ " + destination + " -r")
    get_command_output_wrapped(command)
    print(colored("Done!", "green"))
    return


def move_input_files_to_s3_bucket(input_bucket):
    print(colored("Duplicating bucket...", "yellow"))
    base_bucket = input_bucket.split('/')[0]
    temp_bucket = base_bucket + "/temp_bucket/"
    input_bucket += "/"

    command = "aws s3 cp s3://" + input_bucket + " s3://" + temp_bucket
    get_command_output_wrapped(command)

    command = "aws s3 mv s3://" + input_bucket + " s3://" + temp_bucket + " --recursive"
    get_command_output_wrapped(command)

    command = "aws s3 mv s3://" + temp_bucket + " s3://" + input_bucket + " --recursive"
    get_command_output_wrapped(command)

    print(colored("Done!", "green"))
    return

    command = "aws s3 ls s3://" + input_bucket
    files = get_command_output_wrapped(command)
    for file in tqdm(files[1:]):
        file = file.split()[-1]
        command = "aws s3 cp s3://" + input_bucket + file + " s3://" + temp_bucket + file
        get_command_output_wrapped(command)
        command = "aws s3 cp s3://" + temp_bucket + file + " s3://" + input_bucket + file
        get_command_output_wrapped(command)

    return


# given a service, returns the list of jobs with their status
def get_jobs_list(service_name, oscarcli_alias):

    set_default_oscar_cluster_from_alias(oscarcli_alias)

    # command = oscar_cli + "service logs list " + service_name
    command = executables.oscar_cli.get_command("service logs list " + service_name)
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


def increment_sleep(sleep):
    max_wait = 300
    sleep += round(sleep / 2)
    if sleep > max_wait:
        return max_wait
    return sleep


# awaits the completion of all the jobs before passing to processing
def wait_services_completion(services):
    for service in services:
        if service["is_lambda"]:
            wait_scar_service_completion(service)
        else:
            wait_oscar_service_completion(service)
    return


def wait_oscar_service_completion(service):
    sleep_interval = 10
    completed = False
    service_name = service["name"]
    # cluster = get_active_cluster(service, clusters)
    print(colored("Waiting for service " + service_name + " completion...", "yellow"))

    pbar = tqdm()

    while not completed:
        job_list = get_jobs_list(service_name, service["oscarcli_alias"])
        pbar.total = len(job_list)
        pbar.refresh()
        time.sleep(increment_sleep(sleep_interval))

        i = len(job_list)
        if job_list:  # if list not empty
            completed = True
            for j in job_list:
                if j["status"] != "Succeeded":
                    completed = False
                    i -= 1
                if j["status"] == "Failed":
                    show_error("Job " + j["name"] + " failed")  # todo use fatal error
                    completed = False
                    quit()
        pbar.n = i
        pbar.refresh()

    pbar.close()

    print(colored("Service " + service_name + " completed!", "green"))
    return


# todo this isn't done yet, finish it
def wait_scar_service_completion(service):
    sleep_interval = 10
    completed = False
    service_name = service["name"]
    print(colored("Waiting for service " + service_name + " completion...", "yellow"))

    pbar = tqdm()
    while not completed:
        time.sleep(increment_sleep(sleep_interval))
        log = get_scar_log(service_name)

        total_jobs = 0

        for i in range(0, len(log)):
            line = log[i]
            if "START RequestId" in line:
                total_jobs += 1

        pbar.total = total_jobs
        pbar.refresh()

        completed_jobs = 0

        log = get_scar_log(service_name)

        for i in range(0, len(log)):
            line = log[i]
            if "END RequestId" in line:
                completed_jobs += 1

        pbar.n = completed_jobs

        if completed_jobs == total_jobs:
            completed = True

    pbar.close()
    print(colored("Service " + service_name + " completed!", "green"))
    return

"""
def download_bucket(destination, bucket_name):
    print(colored("Downloading bucket...", "yellow"))
    mc_alias = get_mc_alias()
    origin = mc_alias + "/" + bucket_name
    auto_mkdir(destination)
    command = "oscar-p/mc cp " + origin + " " + destination + " -r"
    get_command_output_wrapped(command)
    print(colored("Done!", "green"))
"""

