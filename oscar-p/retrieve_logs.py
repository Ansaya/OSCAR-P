# oscar and kubectl logs are saved as txts and processed in dictionaries saved as pickle files

import pickle
import os

from termcolor import colored
from datetime import datetime, timedelta

from utils import configure_ssh_client, get_ssh_output, get_command_output_wrapped

global run_name


# todo must comment more everywhere

def pull_logs(name, services):
    print(colored("Collecting logs...", "yellow"))
    global run_name
    run_name = name
    os.system("mkdir \"" + run_name + "/logs_kubectl\"")
    os.system("mkdir \"" + run_name + "/logs_oscar\"")

    client = configure_ssh_client()

    for service in services:
        service_name = service["name"]
        save_timelist_to_file(get_timed_jobs_list(service_name, client), service_name)

    print(colored("Done!", "green"))


# for a given service, it saves as text file a copy of the logs from OSCAR and kubectl, and also returns a timelist
# todo explain better in comment
def get_timed_jobs_list(service, client):
    date_format = "%Y-%m-%d %H:%M:%S"

    pod_list = get_kubernetes_pod_list(client)

    command = "oscar-p/oscar-cli service logs list " + service
    logs_list = get_command_output_wrapped(command)

    if logs_list:
        logs_list.pop(0)

    timed_job_list = {}

    for line in logs_list:
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

        for pod in pod_list:
            if job_name in pod:
                pod_name = pod.split()[1]
                pod_creation, pod_node = get_kubectl_log(client, pod_name)

        bash_script_start, bash_script_end = get_oscar_log(service, job_name)

        timed_job_list[job_name] = {"service": service,
                                    "node": pod_node,
                                    "job_create": job_create,
                                    "pod_create": pod_creation,
                                    "job_start": job_start,
                                    "bash_script_start": bash_script_start,
                                    "bash_script_end": bash_script_start,
                                    "job_finish": job_finish
                                    }

    return timed_job_list


# retrieve the content of the log of a given job
# todo change fixed delta to time correction from input file
# todo change bash_script_start and end to equivalent lines
def get_oscar_log(service_name, job_name):
    command = "oscar-p/oscar-cli service logs get " + service_name + " " + job_name
    output = get_command_output_wrapped(command)

    date_format_precise = "%d-%m-%Y %H:%M:%S.%f"

    with open(run_name + "/logs_oscar/" + job_name + ".txt", "w") as file:
        for line in output:
            if "#bash_script_start" in line:
                bash_script_start = line.split(": ")[1].replace("\n", "")
                bash_script_start = datetime.strptime(bash_script_start, date_format_precise) + timedelta(hours=1)
            if "#bash_script_end" in line:
                bash_script_end = line.split(": ")[1].replace("\n", "")
                bash_script_end = datetime.strptime(bash_script_end, date_format_precise) + timedelta(hours=1)
            file.write(line)

    return bash_script_start, bash_script_end


def get_kubernetes_pod_list(client):
    command = "sudo kubectl get pods -A"
    return get_ssh_output(client, command)


# downloads and save to file the log for the specified pod, returns pod creation time and node
def get_kubectl_log(client, pod_name):
    command = "sudo kubectl describe pods " + pod_name + " -n oscar-svc"
    output = get_ssh_output(client, command)
    with open(run_name + "/logs_kubectl/" + pod_name + ".txt", "w") as file:
        for line in output:
            file.write(line)

    create_time = ""
    node = ""

    for line in output:
        if "Start Time:" in line:
            create_time = line.split(", ", 1)[1].split(" +")[0]
            create_time = datetime.strptime(create_time, "%d %b %Y %H:%M:%S") + timedelta(hours=1)
        if "Node:" in line:
            node = line.split(" ")[-1].split("/")[0]

    return create_time, node


# dumps a timelist to file
def save_timelist_to_file(timed_job_list, service_name):
    with open(run_name + "/time_table_" + service_name + ".pkl", "wb") as file:
        pickle.dump(timed_job_list, file, pickle.HIGHEST_PROTOCOL)
    return
