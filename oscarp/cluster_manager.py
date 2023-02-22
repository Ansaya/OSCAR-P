# all methods related to the initial cluster configuration (before run start)
import time

import yaml
import json
import os
import random

from tqdm import tqdm

import executables

from termcolor import colored
from datetime import date

from input_file_processing import get_debug, get_closest_parallelism_level, get_workflow_input
from utils import configure_ssh_client, get_ssh_output, get_command_output_wrapped, show_debug_info, \
    make_debug_info, show_warning, show_fatal_error, read_json

import global_parameters as gp


def remove_all_services(clusters):
    print(colored("Removing services...", "yellow"))
    for c in clusters:
        if c != "AWS Lambda":
            cluster = clusters[c]
            set_default_oscar_cluster(cluster)
            command = executables.oscar_cli.get_command("service ls")
            services = get_command_output_wrapped(command)
            services.pop(0)
            for service in services:
                service = service.split()[0]
                command = executables.oscar_cli.get_command("service remove " + service)
                get_command_output_wrapped(command)
                print("Removed service " + service + " from cluster " + c)
    print(colored("Done!", "green"))


def remove_all_buckets(clusters):
    print(colored("Removing buckets...", "yellow"))
    for c in clusters:
        if c != "AWS Lambda":
            cluster = clusters[c]
            minio_alias = cluster["storage_provider_alias"]
            command = executables.mc.get_command("ls " + minio_alias)
            buckets = get_command_output_wrapped(command)
            for bucket in buckets:
                bucket = bucket.split()[-1]
                # if bucket != "storage/":  todo reenable
                if "storage" not in bucket:
                    bucket = minio_alias + "/" + bucket
                    command = executables.mc.get_command("rb " + bucket + " --force")
                    get_command_output_wrapped(command)
                    print("Removed bucket " + bucket + " from cluster " + c)
    print(colored("Done!", "green"))
    return


def clean_s3_buckets():
    print(colored("Cleaning S3 buckets...", "yellow"))
    command = "aws s3 ls"
    buckets = get_command_output_wrapped(command)
    for b in buckets:
        b = b.split()[2]
        if "scar-bucket" in b or "test" in b:
            command = "aws s3 ls s3://" + b + "/"
            folders = get_command_output_wrapped(command)
            for f in folders:
                f = f.split()[1]
                command = "aws s3 ls s3://" + b + "/" + f
                files = get_command_output_wrapped(command)
                for file in files[1:]:
                    command = "aws s3 rm s3://" + b + "/" + f + file.split()[-1]
                    get_command_output_wrapped(command)

            print("Cleaned bucket " + b)
    print(colored("Done!", "green"))
    return


def create_bucket(bucket, cluster_name, clusters):
    cluster = clusters[cluster_name]
    print(colored("Creating bucket " + bucket + " on cluster " + cluster_name + "...", "yellow"))
    minio_alias = cluster["minio_alias"]
    command = executables.mc.get_command("mb " + minio_alias + "/" + bucket)
    get_command_output_wrapped(command)
    print(colored("Done!", "green"))
    return


def recreate_bucket(bucket, minio_alias):
    bucket = minio_alias + "/" + bucket
    command = executables.mc.get_command("rb " + bucket + " --force")
    get_command_output_wrapped(command)
    command = executables.mc.get_command("mb " + bucket)
    get_command_output_wrapped(command)
    return


def recreate_output_buckets(service):
    for b in service["output_buckets"]:
        recreate_bucket(b["path"], b["minio_alias"])
    return


# clean all the logs of oscar
def clean_all_logs():
    command = executables.oscar_cli.get_command("service ls")
    services = get_command_output_wrapped(command)
    services.pop(0)
    for service in services:
        service = service.split()[0]
        print(colored("Cleaning logs of service " + service + "...", "yellow"))
        command = executables.oscar_cli.get_command("service logs remove " + service + " --all")
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
    with open("FDL_configuration.yaml", "w") as file:

        fdl_services = []

        for current_service in services:

            script_path = executables.script  # todo will replace with script inside images (?)
            # script_path = "/opt/script.sh"

            fdl_service = {
                "name": current_service["name"],
                "input": [{"path": "", "storage_provider": ""}],
                "output": [{"path": "", "storage_provider": ""}]
            }

            # urgent test service-wise cpu and memory limits (YuniKorn)

            if not current_service["is_lambda"]:
                fdl_service["input"][0]["storage_provider"] = "minio.default"
                fdl_service["input"][0]["path"] = current_service["input_bucket"]
                fdl_service["output"][0]["storage_provider"] = current_service["output_bucket"][0] + "." + current_service["output_bucket"][1]
                fdl_service["output"][0]["path"] = current_service["output_bucket"][2]
                fdl_service["cpu"] = float(current_service["cpu"])
                fdl_service["memory"] = current_service["memory"] + "Mi"
                fdl_service["total_cpu"] = float(current_service["cpu"]) * current_service["parallelism"]
                fdl_service["total_memory"] = str(int(current_service["memory"]) * current_service["parallelism"]) + "Mi"
                fdl_service["image"] = current_service["image"]
                fdl_service["script"] = script_path
                oscarcli_alias = current_service["oscarcli_alias"]
                fdl_service = {oscarcli_alias: fdl_service}
                fdl_services.append(fdl_service)

        fdl_config = {"functions": {},
                      "storage_providers": generate_fdl_storage_providers()}

        fdl_config["functions"]["oscar"] = fdl_services

        yaml.dump(fdl_config, file)
        return


# generate the FDL file used to prepare OSCAR, including only the specified service
def generate_fdl_single_service(service, clusters):
    original_input_bucket = service["input_bucket"]
    service["input_bucket"] = "dead-start"
    generate_fdl_configuration([service], clusters)
    service["input_bucket"] = original_input_bucket


def generate_fdl_storage_providers():
    region = "us-east-1"  # todo should this be hardcoded like this?

    providers = {
        "minio": {}
    }

    if gp.is_debug:
        minio_aliases = read_json(os.path.join("/home/scrapjack/.mc/", "config.json"))["aliases"]
    else:
        minio_aliases = read_json(os.path.join("/root/.mc/", "config.json"))["aliases"]  # todo RBF

    for alias in minio_aliases:
        if "minio" in alias:
            providers["minio"][alias] = {
                "access_key": minio_aliases[alias]["accessKey"],
                "endpoint": minio_aliases[alias]["url"],
                "secret_key": minio_aliases[alias]["secretKey"],
                "region": region
            }

    auth_file = gp.application_dir + "aisprint/deployments/base/im/auth.dat"

    if os.path.exists(auth_file):
        with open(auth_file, "r") as file:
            lines = file.readlines()
            for line in lines:
                if "id = ec2" in line:
                    for segment in line.split("; "):
                        if "username" in segment:
                            access_key = segment.strip("username = ")
                        elif "password" in segment:
                            secret_key = segment.strip("password = ")

        providers["s3"] = {}
        providers["s3"]["aws"] = {
            "access_key": access_key,
            "region": region,
            "secret_key": secret_key
        }

    return providers


def apply_fdl_configuration_wrapped(services):
    """

    :param services:
    :return:
    """

    print(colored("Adjusting OSCAR configuration...", "yellow"))
    _apply_fdl_configuration_oscar()
    verify_correct_oscar_deployment(services)
    print(colored("Done!", "green"))
    return


def _apply_fdl_configuration_oscar():
    command = executables.oscar_cli.get_command("apply " + "FDL_configuration.yaml")
    get_command_output_wrapped(command)
    return


def verify_correct_oscar_deployment(services):
    """
    after applying the FDL file, makes sure that all the required services are deployed
    """

    print(colored("Checking correct OSCAR deployment...", "yellow"))

    for s in services:
        if not s["is_lambda"]:
            deployed_services = get_deployed_services(s["oscarcli_alias"])

            if s["name"] not in deployed_services:
                show_fatal_error("Service " + s["name"] + " not deployed")
            else:
                print("Service " + s["name"] + " deployed on cluster " + s["cluster"])

    return


def get_deployed_services(oscarcli_alias):
    set_default_oscar_cluster_from_alias(oscarcli_alias)
    command = executables.oscar_cli.get_command("service list")
    output = get_command_output_wrapped(command)

    deployed_services = []

    output.pop(0)

    for o in output:
        deployed_services.append(o.split('\t')[0])

    return deployed_services


def get_active_cluster(service, clusters):
    cluster_name = service["cluster"]
    cluster = clusters[cluster_name]
    return cluster, cluster_name


def set_default_oscar_cluster(cluster):
    set_default_oscar_cluster_from_alias(cluster["oscarcli_alias"])


def set_default_oscar_cluster_from_alias(oscarcli_alias):
    command = executables.oscar_cli.get_command("cluster default -s " + oscarcli_alias)
    get_command_output_wrapped(command)


def upload_input_files_to_storage(simple_services):
    storage_bucket, filename, _, _, _, _ = get_workflow_input()
    if filename is None:
        return

    cluster = simple_services[0]["cluster"]

    storage_bucket_found = False
    command = executables.mc.get_command("ls minio-%s/" % cluster)
    lines = get_command_output_wrapped(command)
    for line in lines:
        if storage_bucket in line:
            storage_bucket_found = True
            break

    if not storage_bucket_found:
        command = executables.mc.get_command("mb minio-%s/%s" % (cluster, storage_bucket))
        get_command_output_wrapped(command)

    command = executables.mc.get_command("ls minio-%s/%s" % (cluster, storage_bucket))
    lines = get_command_output_wrapped(command)
    for line in lines:
        if filename in line:
            return

    input_file_path = gp.application_dir + "oscarp/input_files/" + filename
    command = executables.mc.get_command("cp %s minio-%s/%s" % (input_file_path, cluster, storage_bucket))
    print(colored("Uploading input file to %s storage bucket" % cluster, "yellow"))
    get_command_output_wrapped(command)
    print(colored("Done!", "green"))
