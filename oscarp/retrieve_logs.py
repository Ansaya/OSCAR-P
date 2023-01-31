# oscar and kubectl logs are saved as txts and processed in dictionaries saved as pickle files
import csv
import json
import pickle
import os
from tqdm import tqdm

import executables

from termcolor import colored
from datetime import datetime, timedelta

from input_file_processing import get_time_correction
from cluster_manager import set_default_oscar_cluster, get_active_cluster
from utils import configure_ssh_client, get_ssh_output, get_command_output_wrapped, show_debug_info, read_yaml, \
    write_yaml

import global_parameters as gp

global run_name


# todo must comment more everywhere

def pull_logs(name, services, clusters):
    print(colored("Collecting OSCAR logs...", "yellow"))
    global run_name
    run_name = name
    os.system("mkdir \"" + run_name + "/logs_kubectl\"")
    os.system("mkdir \"" + run_name + "/logs_oscar\"")

    for service in services:
        service_name = service["name"]
        cluster, cluster_name = get_active_cluster(service, clusters)
        if cluster["oscarcli_alias"] is not None:  # e.g. it's not Lambda
            # client = configure_ssh_client(cluster)  # todo temporarily (?) disabled
            client = None
            timed_job_list = get_timed_jobs_list(service_name, client, cluster, cluster_name)
            save_timelist_to_file(timed_job_list, service_name)

    print(colored("Done!", "green"))


# for a given service, it saves as text file a copy of the logs from OSCAR and kubectl, and also returns a timelist
# todo explain better in comment
def get_timed_jobs_list(service, client, cluster, cluster_name):
    date_format = "%Y-%m-%d %H:%M:%S"

    # pod_list = get_kubernetes_pod_list(client)  # todo restore

    set_default_oscar_cluster(cluster)

    command = executables.oscar_cli.get_command("service logs list " + service)
    logs_list = get_command_output_wrapped(command)

    if logs_list:
        logs_list.pop(0)

    timed_job_list = {}

    for line in tqdm(logs_list):
        segments = line.split('\t')
        job_name = segments[0]
        job_status = segments[1]
        job_create = datetime.strptime(segments[2], date_format)
        job_start = datetime.strptime(segments[3], date_format)
        job_finish = datetime.strptime(segments[4].rstrip('\n'), date_format)

        if job_status != "Succeeded":
            print(colored("Uh oh, something went wrong somewhere.", "red"))

        pod_node = ""
        pod_creation = ""

        # todo restore? tricky, physical and virtual nodes are accessed in different ways (for now)

        """
        for pod in pod_list:
            if job_name in pod:
                pod_name = pod.split()[1]
                pod_creation, pod_node = get_kubectl_log(client, pod_name)
        """

        bash_script_start, bash_script_end = get_oscar_log(service, job_name)

        timed_job_list[job_name] = {"service": service,
                                    "resource": cluster_name,
                                    # "node": pod_node,
                                    "job_create": job_create,
                                    # "pod_create": pod_creation,
                                    "job_start": job_start,
                                    "bash_script_start": bash_script_start,
                                    "bash_script_end": bash_script_start,
                                    "job_finish": job_finish
                                    }

    return timed_job_list


# retrieve the content of the log of a given job
def get_oscar_log(service_name, job_name):
    # command = oscar_cli + "service logs get " + service_name + " " + job_name
    command = executables.oscar_cli.get_command("service logs get " + service_name + " " + job_name)
    output = get_command_output_wrapped(command)

    # date_format_precise = "%d-%m-%Y %H:%M:%S.%f"
    date_format_precise = "%Y-%m-%d %H:%M:%S,%f"

    time_correction = get_time_correction()

    with open(run_name + "/logs_oscar/" + job_name + ".txt", "w") as file:
        for line in output:
            # nanoseconds after finding the script it is executed
            if "Script file found in '/oscar/config/script.sh'" in line:
                # bash_script_start = line.split(" ")[1].replace("\n", "")
                bash_script_start = line.split(" - ")[0]
                # print(bash_script_start)
                bash_script_start = datetime.strptime(bash_script_start, date_format_precise) \
                                    + timedelta(hours=time_correction)
            # this happens immediately after the bash script exits
            if "Searching for files to upload in folder" in line:
                bash_script_end = line.split(" - ")[0]
                bash_script_end = datetime.strptime(bash_script_end, date_format_precise) \
                                  + timedelta(hours=time_correction)
            file.write(line)

    return bash_script_start, bash_script_end


def get_kubernetes_pod_list(client):
    command = "sudo kubectl get pods -A"
    return get_ssh_output(client, command)


# downloads and save to file the log for the specified pod, returns pod creation time and node
# the pod creation time is the only time not gathered from OSCAR, so it's easier to apply the time correction here
def get_kubectl_log(client, pod_name):
    command = "sudo kubectl describe pods " + pod_name + " -n oscar-svc"
    output = get_ssh_output(client, command)
    with open(run_name + "/logs_kubectl/" + pod_name + ".txt", "w") as file:
        for line in output:
            file.write(line)

    create_time = ""
    node = ""

    time_correction = get_time_correction()
    show_debug_info("Time correction: " + str(time_correction))

    for line in output:
        if "Start Time:" in line:
            create_time = line.split(", ", 1)[1].split(" +")[0]
            create_time = datetime.strptime(create_time, "%d %b %Y %H:%M:%S") + timedelta(hours=time_correction)
        if "Node:" in line:
            node = line.split(" ")[-1].split("/")[0]

    return create_time, node


def pull_scar_logs(name, services):
    """
    pulls the log for every lambda function, start time is the first "storage event found", end is last "uploading file"
    :return:
    """

    if not gp.has_active_lambdas:
        return

    print(colored("Collecting SCAR logs...", "yellow"))

    global run_name  # todo run_name is actually a path to the run dir, fix
    run_name = name

    date_format = "%H:%M:%S,%f"

    command = "scar ls"
    lambda_functions = get_command_output_wrapped(command)
    for service in services:
        service_name = service["name"]
        for function in lambda_functions[3:]:
            function = function.split()[0]
            if service_name == function:  # this is needed when testing a single lambda to download the right log

                log = get_scar_log(function, update_marker=True)

                # todo ? not sure if needed, we'll see

                """
                for line in log:
                    if "REPORT RequestId" in line:
                        print(line.split())
                        print(line.split()[2])  # id
                        print(line.split()[8])  # duration
                """

                for i in range(0, len(log)):
                    line = log[i]
                    if "Storage event found" in line:
                        start_time = line.split()[1]
                        start_time = datetime.strptime(start_time, date_format)
                        break

                for i in range(len(log) - 1, 0, -1):  # recurse backwards
                    line = log[i]
                    if "Uploading file" in line:
                        end_time = line.split()[1]
                        end_time = datetime.strptime(end_time, date_format)
                        break

                timed_job_list = {"lambda_job": {"service": function,
                                                 "resource": "AWS Lambda",
                                                 "job_create": start_time,
                                                 "job_finish": end_time
                                                 }}

                save_timelist_to_file(timed_job_list, function)
    print(colored("Done!", "green"))
    return


def get_scar_log(function, update_marker=False):
    command = "scar log -n " + function
    log = get_command_output_wrapped(command)

    if function in gp.scar_logs_end_indexes.keys():
        index_last_line = gp.scar_logs_end_indexes[function]
        log = log[index_last_line:]

    if update_marker:
        gp.scar_logs_end_indexes[function] = len(log) - 1

        with open(run_name + "/scar_log.txt", "w") as file:
            file.writelines(log)

    return log


# dumps a timelist to file
def save_timelist_to_file(timed_job_list, service_name):
    with open(run_name + "/time_table_" + service_name + ".json", "w") as file:
        json.dump(timed_job_list, file, indent=4, sort_keys=False, default=str)

    with open(run_name + "/time_table_" + service_name + ".pkl", "wb") as file:
        pickle.dump(timed_job_list, file, pickle.HIGHEST_PROTOCOL)
    return


def get_data_size(simple_services):  # missing SCAR support
    name = simple_services[0]["name"]
    cluster = simple_services[0]["cluster"]
    output_bucket = simple_services[0]["output"][2]
    command = executables.mc.get_command("ls minio-%s/%s" % (cluster, output_bucket))
    lines = get_command_output_wrapped(command)

    data_size = 0

    for line in lines:
        size = line.split()[3]
        if "MiB" in size:
            size = float(size.strip("MiB")) * 1000
        elif "GiB" in size:
            size = float(size.strip("GiB")) * 1000000
        elif "KiB" in size:
            size = float(size.strip("KiB"))

        data_size += size

    filepath = gp.application_dir + "components_data_size.yaml"
    components_data_sizes = read_yaml(filepath)

    if name not in components_data_sizes.keys():
        components_data_sizes[name] = data_size
        write_yaml(filepath, components_data_sizes)

    return
