# all methods related to the inital cluster configuration (before run start)

import yaml

from termcolor import colored
from input_file_processing import get_mc_alias, get_debug
from utils import execute_command, configure_ssh_client, get_ssh_output, get_command_output_wrapped, show_debug_info, \
    make_debug_info


def remove_all_services():
    print(colored("Removing services...", "yellow"))
    command = "oscar-p/oscar-cli service ls"
    services = get_command_output_wrapped(command)
    services.pop(0)
    for service in services:
        service = service.split()[0]
        command = "oscar-p/oscar-cli service remove " + service
        execute_command(command)
    print(colored("Done!", "green"))


def remove_all_buckets():
    print(colored("Removing buckets...", "yellow"))
    mc_alias = get_mc_alias()
    command = "oscar-p/mc ls " + mc_alias
    buckets = get_command_output_wrapped(command)
    for bucket in buckets:
        bucket = bucket.split()[-1]
        if bucket != "storage/":
            bucket = mc_alias + "/" + bucket
            command = "oscar-p/mc rb " + bucket + " --force"
            execute_command(command)
    print(colored("Done!", "green"))
    return


def create_bucket(bucket):
    print(colored("Recreating bucket " + bucket + "...", "yellow"))
    mc_alias = get_mc_alias()
    command = "oscar-p/mc mb " + mc_alias + "/" + bucket
    execute_command(command)
    print(colored("Done!", "green"))
    return


def recreate_bucket(bucket):
    mc_alias = get_mc_alias()
    bucket = mc_alias + "/" + bucket
    command = "oscar-p/mc rb " + bucket + " --force"
    execute_command(command)
    command = "oscar-p/mc mb " + bucket
    execute_command(command)
    return


def recreate_output_buckets(service):
    for b in service["output_buckets"]:
        recreate_bucket(b["path"])
    return


# clean all the logs of oscar
def clean_all_logs():
    command = "oscar-p/oscar-cli service ls"
    services = get_command_output_wrapped(command)
    services.pop(0)
    for service in services:
        service = service.split()[0]
        print(colored("Cleaning logs of service " + service + "...", "yellow"))
        command = "oscar-p/oscar-cli service logs remove " + service + " --all"
        execute_command(command)
        print(colored("Done!", "green"))

    return


def make_fdl_buckets_list(buckets):
    bucket_list = []
    for b in buckets:
        prefix = []
        for p in b["prefix"]:
            prefix.append(p)

        suffix = []
        for s in b["suffix"]:
            suffix.append(s)

        bucket_list.append({
            "path": b["path"],
            "storage_provider": "minio.default",
            "prefix": prefix,
            "suffix": suffix,
        })
    return bucket_list


# generate the FDL file used to prepare OSCAR, starting from the input yaml
def generate_fdl_configuration(run, cluster_name):
    with open(r'oscar-p/FDL_configuration.yaml', 'w') as file:

        services = []
        for s in run["services"]:
            service = {cluster_name: {
                "cpu": s["cpu"],
                "memory": s["memory"] + "Mi",
                "image": s["image"],
                "name": s["name"],
                "script": s["script"],
                "input": [{
                    "path": s["input_bucket"],
                    "storage_provider": "minio.default"
                }],
                "output": make_fdl_buckets_list(s["output_buckets"])
            }
            }
            services.append(service)

        fdl_config = {"functions": {"oscar": services}}
        yaml.dump(fdl_config, file)


# generate the FDL file used to prepare OSCAR, including only the specified service
def generate_fdl_single_service(service, cluster_name):
    with open(r'oscar-p/FDL_configuration.yaml', 'w') as file:
        services = [{cluster_name: {
                "cpu": service["cpu"],
                "memory": service["memory"] + "Mi",
                "image": service["image"],
                "name": service["name"],
                "script": service["script"],
                "input": [{
                    "path": "dead-start",
                    "storage_provider": "minio.default"
                }],
                "output": make_fdl_buckets_list(service["output_buckets"])
            }
            }]

        fdl_config = {"functions": {"oscar": services}}
        yaml.dump(fdl_config, file)


def apply_fdl_configuration():
    print(colored("Adjusting OSCAR configuration...", "yellow"))
    command = "oscar-p/oscar-cli apply oscar-p/FDL_configuration.yaml"
    get_command_output_wrapped(command)
    print(colored("Done!", "green"))
    return


# returns a list of nodes, with status = off if cordoned or on otherwise
# doesn't return master nodes
def get_node_list(client):
    lines = get_ssh_output(client, "sudo kubectl get nodes")

    lines.pop(0)

    node_list = []

    for line in lines:
        node_name = line.split()[0]
        node_status = line.split()[1]
        node_role = line.split()[2]
        if "SchedulingDisabled" in node_status:
            node_status = "off"
        else:
            node_status = "on"

        if "master" not in node_role:
            node_list.append({
                "name": node_name,
                "status": node_status,
            })

    return node_list


# cordons or uncordons the nodes of the cluster to obtain the number requested for the current run
def apply_cluster_configuration(run):
    print(colored("Adjusting cluster configuration...", "yellow"))
    client = configure_ssh_client()
    node_list = get_node_list(client)
    requested_number_of_nodes = run["nodes"]

    show_debug_info(make_debug_info(["Cluster configuration BEFORE:"] + node_list))

    for i in range(1, len(node_list) + 1):
        node = node_list[i - 1]
        if i <= requested_number_of_nodes and node["status"] == "off":
            get_ssh_output(client, "sudo kubectl uncordon " + node["name"])
        if i > requested_number_of_nodes and node["status"] == "on":
            get_ssh_output(client, "sudo kubectl cordon " + node["name"])

    show_debug_info(make_debug_info(["Cluster configuration AFTER:"] + get_node_list(client)))

    print(colored("Done!", "green"))
    quit()
    return
