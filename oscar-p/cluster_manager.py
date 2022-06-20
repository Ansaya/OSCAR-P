# all methods related to the inital cluster configuration (before run start)

import yaml
import json
import os

from termcolor import colored
from input_file_processing import get_mc_alias, get_debug, get_closest_parallelism_level
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
    print(colored("Done!", "green"))


def remove_all_buckets(clusters):
    print(colored("Removing buckets...", "yellow"))
    for c in clusters:
        cluster = clusters[c]
        minio_alias =  cluster["minio_alias"]
        command = "oscar-p/mc ls " + minio_alias
        buckets = get_command_output_wrapped(command)
        for bucket in buckets:
            bucket = bucket.split()[-1]
            if bucket != "storage/":
                bucket = minio_alias + "/" + bucket
                command = "oscar-p/mc rb " + bucket + " --force"
                get_command_output_wrapped(command)
    print(colored("Done!", "green"))
    return


def create_bucket(bucket):
    print(colored("Recreating bucket " + bucket + "...", "yellow"))
    mc_alias = get_mc_alias()
    command = "oscar-p/mc mb " + mc_alias + "/" + bucket
    get_command_output_wrapped(command)
    print(colored("Done!", "green"))
    return


def recreate_bucket(bucket):
    mc_alias = get_mc_alias()
    bucket = mc_alias + "/" + bucket
    command = "oscar-p/mc rb " + bucket + " --force"
    get_command_output_wrapped(command)
    command = "oscar-p/mc mb " + bucket
    get_command_output_wrapped(command)
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
        get_command_output_wrapped(command)
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
def generate_fdl_configuration(run, clusters):
    with open(r'oscar-p/FDL_configuration.yaml', 'w') as file:

        services = []
        for s in run["services"]:

            cluster_name = s["cluster"]
            oscarcli_alias = clusters[cluster_name]["oscarcli_alias"]

            service = {oscarcli_alias: {
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

        fdl_config = {"functions": {"oscar": services}, 
                      "storage_providers": {"minio": generate_fdl_storage_providers(clusters)}}
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

        fdl_config = {"functions": {"oscar": services}, 
                      "storage_providers": {"minio": generate_fdl_storage_providers()}}
        yaml.dump(fdl_config, file)


def generate_fdl_storage_providers(clusters):
    home_dir = os.path.expanduser('~')
    
    with open(home_dir + "/.mc/config.json") as file:
        minio_config = json.load(file)
        
    providers = {}
        
    for c in clusters:        
        cluster_minio_alias = clusters[c]["minio_alias"]
        cluster_info = minio_config["aliases"][cluster_minio_alias]
        
        providers[c] = {
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
    command = "oscar-p/oscar-cli service list"
    output = get_command_output_wrapped(command)
    
    deployed_services = []

    output.pop(0)
    
    for o in output:
        deployed_services.append(o.split('\t')[0])
    
    for s in services:
        if s["name"] not in deployed_services:
            show_warning("Service " + s["name"] + " not deployed")
            return False
        else:
            print("Service " + s["name"] + " deployed")
    
    return True


def set_default_oscar_cluster(cluster):
    oscarcli_alias = cluster["oscarcli_alias"]
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


# cordons or uncordons the nodes of the cluster to obtain the number requested for the current run
def apply_cluster_configuration(run, clusters):
    print(colored("Adjusting clusters configuration...", "yellow"))
    
    for c in clusters:
        cluster = clusters[c]
        
        closest_parallelism = get_closest_parallelism_level(run["parallelism"], cluster["possible_parallelism"], c, False)
        
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
