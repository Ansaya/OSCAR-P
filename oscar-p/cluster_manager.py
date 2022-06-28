# all methods related to the inital cluster configuration (before run start)

import yaml
import json
import os

from termcolor import colored
from input_file_processing import get_debug, get_closest_parallelism_level
from utils import configure_ssh_client, get_ssh_output, get_command_output_wrapped, show_debug_info, \
    make_debug_info, show_warning


def remove_all_services(clusters):
    print(colored("Removing services...", "yellow"))
    for c in clusters:
        cluster = clusters[c]
        set_default_oscar_cluster(cluster)
        command = "oscar-p/oscar-cli service ls"
        services = get_command_output_wrapped(command)
        services.pop(0)
        for service in services:
            service = service.split()[0]
            command = "oscar-p/oscar-cli service remove " + service
            get_command_output_wrapped(command)
            print("Removed service " + service + " from cluster " + c)
    print(colored("Done!", "green"))


def remove_all_buckets(clusters):
    print(colored("Removing buckets...", "yellow"))
    for c in clusters:
        cluster = clusters[c]
        minio_alias = cluster["minio_alias"]
        command = "oscar-p/mc ls " + minio_alias
        buckets = get_command_output_wrapped(command)
        for bucket in buckets:
            bucket = bucket.split()[-1]
            if bucket != "storage/":
                bucket = minio_alias + "/" + bucket
                command = "oscar-p/mc rb " + bucket + " --force"
                get_command_output_wrapped(command)
                print("Removed bucket " + bucket + " from cluster " + c)
    print(colored("Done!", "green"))
    return


def create_bucket(bucket, cluster_name, clusters):
    cluster = clusters[cluster_name]
    print(colored("Creating bucket " + bucket + " on cluster " + cluster_name + "...", "yellow"))
    minio_alias = cluster["minio_alias"]
    command = "oscar-p/mc mb " + minio_alias + "/" + bucket
    get_command_output_wrapped(command)
    print(colored("Done!", "green"))
    return


def recreate_bucket(bucket, minio_alias):
    bucket = minio_alias + "/" + bucket
    command = "oscar-p/mc rb " + bucket + " --force"
    get_command_output_wrapped(command)
    command = "oscar-p/mc mb " + bucket
    get_command_output_wrapped(command)
    return


def recreate_output_buckets(service):
    for b in service["output_buckets"]:
        recreate_bucket(b["path"], b["minio_alias"])
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
        get_command_output_wrapped(command)
        print(colored("Done!", "green"))

    return


def make_fdl_buckets_list(buckets):
    bucket_list = []

    for b in buckets:
        storage_provider = "minio." + b["minio_alias"]

        prefix = []
        for p in b["prefix"]:
            prefix.append(p)

        suffix = []
        for s in b["suffix"]:
            suffix.append(s)

        bucket_list.append({
            "path": b["path"],
            "storage_provider": storage_provider,
            "prefix": prefix,
            "suffix": suffix,
        })
    return bucket_list


# generate the FDL file used to prepare OSCAR, starting from the input yaml
def generate_fdl_configuration(services, clusters):
    with open(r'oscar-p/FDL_configuration.yaml', 'w') as file:

        fdl_services = []
        for current_service in services:

            oscarcli_alias = current_service["oscarcli_alias"]

            fdl_service = {oscarcli_alias: {
                "cpu": current_service["cpu"],
                "memory": current_service["memory"] + "Mi",
                "image": current_service["image"],
                "name": current_service["name"],
                "script": current_service["script"],
                "input": [{
                    "path": current_service["input_bucket"],
                    "storage_provider": "minio.default"
                }],
                "output": make_fdl_buckets_list(current_service["output_buckets"])
                }
            }
            fdl_services.append(fdl_service)

        fdl_config = {"functions": {"oscar": fdl_services},
                      "storage_providers": {"minio": generate_fdl_storage_providers(clusters)}}
        yaml.dump(fdl_config, file)


# generate the FDL file used to prepare OSCAR, including only the specified service
def generate_fdl_single_service(service, clusters):
    original_input_bucket = service["input_bucket"]
    service["input_bucket"] = "dead-start"
    generate_fdl_configuration([service], clusters)
    service["input_bucket"] = original_input_bucket


def generate_fdl_storage_providers(clusters):
    home_dir = os.path.expanduser('~')

    with open(home_dir + "/.mc/config.json") as file:
        minio_config = json.load(file)

    providers = {}

    for c in clusters:
        cluster_minio_alias = clusters[c]["minio_alias"]
        cluster_info = minio_config["aliases"][cluster_minio_alias]

        providers[cluster_minio_alias] = {
            "endpoint": cluster_info["url"],
            "access_key": cluster_info["accessKey"],
            "secret_key": cluster_info["secretKey"],
            "region": "us-east-1"
        }

    return providers


def _apply_fdl_configuration():
    command = "oscar-p/oscar-cli apply oscar-p/FDL_configuration.yaml"
    get_command_output_wrapped(command)
    return


def apply_fdl_configuration_wrapped(services):
    """

    :param services:
    :return:
    """

    print(colored("Adjusting OSCAR configuration...", "yellow"))
    while True:
        _apply_fdl_configuration()
        if verify_correct_fdl_deployment(services):
            break
    print(colored("Done!", "green"))


def verify_correct_fdl_deployment(services):
    """
    after applying the FDL file, makes sure that all the required services are deployed
    """

    print(colored("Checking correct deployment...", "yellow"))

    for s in services:
        deployed_services = get_deployed_services(s["oscarcli_alias"])

        if s["name"] not in deployed_services:
            show_warning("Service " + s["name"] + " not deployed")
            return False
        else:
            print("Service " + s["name"] + " deployed on cluster " + s["cluster"])

    return True


def get_deployed_services(oscarcli_alias):
    set_default_oscar_cluster_from_alias(oscarcli_alias)
    command = "oscar-p/oscar-cli service list"
    output = get_command_output_wrapped(command)

    deployed_services = []

    output.pop(0)

    for o in output:
        deployed_services.append(o.split('\t')[0])

    return deployed_services


def get_active_cluster(service, clusters):
    cluster_name = service["cluster"]
    cluster = clusters[cluster_name]
    return cluster


def set_default_oscar_cluster(cluster):
    set_default_oscar_cluster_from_alias(cluster["oscarcli_alias"])


def set_default_oscar_cluster_from_alias(oscarcli_alias):
    command = "./oscar-p/oscar-cli cluster default -s " + oscarcli_alias
    get_command_output_wrapped(command)


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

        # doesn't include master node
        if "master" not in node_role:
            node_list.append({
                "name": node_name,
                "status": node_status,
            })

    return node_list


# cordons or un-cordons the nodes of the cluster to obtain the number requested for the current run
def apply_cluster_configuration(run, clusters):
    print(colored("Adjusting clusters configuration...", "yellow"))

    for c in clusters:
        cluster = clusters[c]

        closest_parallelism = get_closest_parallelism_level(run["parallelism"],
                                                            cluster["possible_parallelism"], c, False)

        client = configure_ssh_client(cluster)
        node_list = get_node_list(client)

        requested_number_of_nodes = cluster["possible_parallelism"][closest_parallelism][1]

        show_debug_info(make_debug_info(["Cluster configuration BEFORE:"] + node_list))

        for i in range(1, len(node_list) + 1):
            node = node_list[i - 1]
            if i <= requested_number_of_nodes and node["status"] == "off":
                get_ssh_output(client, "sudo kubectl uncordon " + node["name"])
            if i > requested_number_of_nodes and node["status"] == "on":
                get_ssh_output(client, "sudo kubectl cordon " + node["name"])

        show_debug_info(make_debug_info(["Cluster configuration AFTER:"] + get_node_list(client)))

    print(colored("Done!", "green"))
    return
