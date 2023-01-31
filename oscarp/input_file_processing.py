# contains the methods related to the processing of the input file

import yaml

from termcolor import colored

from utils import get_valid_input, show_warning, show_error, show_fatal_error


def consistency_check(services):
    """
    makes sure that all services are linked, i.e. at least one output bucket of the current service matches
    the input bucket of the next service
    """

    for i in range(len(services) - 1):
        match_found = False
        current_service = services[i]
        next_service = services[i + 1]
        for output_bucket in current_service["outputs"]:
            if output_bucket == next_service["input"]:
                match_found = True
                break
        if not match_found:
            show_error("Workflow inconsistent")
            quit()

    return


def get_simple_services(services):
    """
    makes a list of the services with only very basic info, such as the service name and its buckets
    :return: list of services with only basic info, such as the service name and its buckets
    """

    simple_services = []
    for s in services:
        service = {
            "name": s["name"],
            "cluster": s["cluster"],
            "input": s["input_bucket"],
            "output": s["output_bucket"],
        }
        simple_services.append(service)

    return simple_services


# can be used both for the ordered services list and for the services under run
def get_service_by_name(name, services):
    for s in services:
        if s["name"] == name:
            return s


def get_buckets_name_from_list(bucket_list):
    buckets_names = []
    for b in bucket_list:
        buckets_names.append(b["path"])

    return buckets_names


# given a list of ordered services it prints the workflow
def show_workflow(services):
    print("Workflow:\n\tstorage -> " + services[0]["input"])

    for s in services:
        input_bucket = s["input"].split('/')[-1]
        output_bucket = s["output"][2].split('/')[-1]
        print("\t" + colored(input_bucket) + " -> |" + colored(s["name"], "blue") + "| -> " + colored(output_bucket))

    return


# receives a variable (i.e. mem array for a service), a counter and a boolean variable for "continue"
# if variable is a list and the counter is lower than its length (i.e. list has length 3 and counter is 1) it means
#   another run should be scheduled, so cont is set to 1
# if variable isn't a list, or it is but we reached its end, it returns the last available value and doesn't change cont
def var_process(x, i, cont):
    if type(x) is list:
        if len(x) > i + 1:
            x = x[i]
            cont = 1
        else:
            x = x[-1]
    return x, cont


# todo complete function
# starting from the input file it creates a sequence of run that the script will then cycle on
# "base" is a basic skeleton list of runs (if a run must be repeated twice, it shows only once in base)
# "runs" is the full list of runs, including repetitions (implemented to add state saving later on)
# "nodes" is just the number of nodes
def run_scheduler():
    with open("input.yaml", "r") as file:
        script_config = yaml.load(file, Loader=yaml.FullLoader)["configuration"]
        repetitions = script_config["run"]["repetitions"]
        # parallelism = script_config["run"]["parallelism"]
        services = script_config["services"]

    base = base_scheduler_parallel(services)
    runs = base_to_runs(base, repetitions)

    return base, runs


"""

    base = []
    runs = []
    i = 0

    nodes = script_config["worker_nodes"]["nodes"]
    if type(nodes) is not list:
        nodes = [nodes]

    # this creates the "base" list, meaning the skeleton list of the runs that are then repeated multiple times or
    # on different nodes but with the same structure
    while True:
        cont = 0
        services = []

        for s in script_config["services"]:
            name = list(s.keys())[0]
            cpu, cont = var_process(s[name]["cpu"], i, cont)
            memory, cont = var_process(s[name]["memory_mb"], i, cont)
            service = {
                "name": name,
                "cpu": str(cpu),
                "memory": str(memory),
                "image": s[name]["image"],
                "script": s[name]["script"],
                "input_bucket": s[name]["input_bucket"],
                "output_buckets": s[name]["output_buckets"]
                }
            services.append(service)
        run = {
            "id": "Run #" + str(i+1),
            "nodes": nodes[0],
            "services": services
            }
        base.append(run)
        if cont == 0:
            break
        else:
            i += 1

        # the base structure is repeated for every node in the list
        # and the result is repeated x times, where is x is specified number of repetitions
        j = 1
        for i in range(repetitions):
            for n in nodes:
                for elem in base:
                    run = elem.copy()
                    run["id"] = "Run #" + str(j)
                    run["nodes"] = n
                    j += 1
                    runs.append(run)

    return base, runs, nodes
"""


# given the hardware structure of the cluster, returns a list of the possible achievable parallelism levels
# i.e. on a cluster with 2 nodes with 4 cores each, parallelism levels of 5 or 7 are unachievable
def get_possible_parallelisms(total_nodes, max_cores, max_memory):
    possible_parallelism = {}
    for c in range(1, int(max_cores) + 1):
        for n in range(1, int(total_nodes) + 1):
            p = c * n
            cores = max_cores / c
            mem = int((max_memory * 0.8) / c)
            cores = round(cores, 1) - 0.1
            possible_parallelism[p] = (cores, n, mem)
    return possible_parallelism


# todo finish comments
def base_scheduler_parallel(services):
    """
    schedules the base runs by using the "parallelism" array
    :param services:
    :return:
    """

    clusters = get_clusters_info()
    base = []

    first_service = list(services[0].keys())[0]  # only needed to get parallelism length
    parallelism_length = len(services[0][first_service]["parallelism"])

    for x in range(parallelism_length):

        configured_services = []  # services configured correctly for the given parallelism level

        for s in services:
            # collects all necessary info
            service_name = list(s.keys())[0]
            current_service = s[service_name]
            current_cluster_name = current_service["cluster"]
            current_cluster = clusters[current_cluster_name]

            """
            # possible_parallelism = current_cluster["possible_parallelism"]

            # checks if the requested parallelism level is achievable on the selected cluster
            # if not it returns the closest available level
            # closest_p = get_closest_parallelism_level(p, possible_parallelism, current_service["cluster"], True)

            # after choosing a parallelism level we gather the node configuration for the selected cluster
            cpu = possible_parallelism[closest_p][0]
            memory = possible_parallelism[closest_p][2]
            """

            cpu = current_service["cpu"]
            memory = current_service["memory"]
            parallelism = current_service["parallelism"][x]

            # puts everything in a dictionary
            new_service = {
                "name": service_name,
                "cpu": str(cpu),
                "memory": str(memory),
                "image": current_service["image"],
                "parallelism": parallelism,
                "cluster": current_cluster_name,
                "oscarcli_alias": current_cluster["oscarcli_alias"],
                "storage_provider": current_cluster["storage_provider"],
                "storage_provider_alias": current_cluster["storage_provider_alias"],
                "input_bucket": current_service["input_bucket"],
                "output_bucket": current_service["output_bucket"],
                "is_lambda": current_service["is_lambda"]
            }
            configured_services.append(new_service)

        # makes sure that the output bucket provider of one service will match the input bucket provider of the next
        i = -1
        for i in range(len(configured_services) - 1):
            current_service = configured_services[i]
            next_service = configured_services[i + 1]

            # output_bucket = current_service["output_bucket"]
            current_service["output_bucket"] = (next_service["storage_provider"],
                                                next_service["storage_provider_alias"],
                                                next_service["input_bucket"])  # provider.alias/bucket, eg. minio.minio-vm/bucket0

        # for the last service the bucket provider is always the default one
        current_service = configured_services[i + 1]
        output_bucket = current_service["output_bucket"]
        current_service["output_bucket"] = (current_service["storage_provider"], "default", output_bucket)

        # id will be added later on
        run = {
            "id": "",
            "services": configured_services
        }
        base.append(run)

    return base


# todo comment
def base_to_runs(base, repetitions):
    runs = []

    for i in range(len(base)):
        b = base[i]
        b["id"] = "Run #" + str(i + 1)
        for j in range(repetitions):
            run_id = i * repetitions + j + 1
            r = b.copy()
            r["id"] = "Run #" + str(run_id)
            runs.append(r)

    return runs


"""
def get_correct_docker_image(service, architecture, service_name):
    if architecture == "ARM64":
        if "image_ARM64" in service.keys():
            image = service["image_ARM64"]
        else:
            show_fatal_error("Unable to find an ARM64 image for service " + service_name)
    elif architecture == "x86":
        if "image_x86" in service.keys():
            image = service["image_x86"]
        else:
            show_fatal_error("Unable to find an x86 image for service " + service_name)
    else:
        show_fatal_error("Unknown architecture " + architecture)
    return image
"""


def get_closest_parallelism_level(requested_parallelism, possible_parallelism, cluster_name, verbose):
    """
    checks if the requested parallelism level is achievable on the selected cluster, if not it returns the closest
        available level
    :param requested_parallelism: requested parallelism level
    :param possible_parallelism: list of possible parallelism levels
    :param cluster_name: name of the cluster
    :param verbose: boolean, if true shows a warning when an exact match is not possible
    :return: selected parallelism level
    """

    if requested_parallelism not in possible_parallelism.keys():
        closest_parallelism = min(possible_parallelism, key=lambda x: abs(x - requested_parallelism))
        if verbose:
            show_warning("A parallelism of " + str(requested_parallelism) + " is not achievable on cluster "
                         + cluster_name + ", using " + str(closest_parallelism) + " instead")
        return closest_parallelism
    else:
        return requested_parallelism


# todo comment
def get_clusters_info():
    with open("input.yaml", "r") as file:
        script_config = yaml.load(file, Loader=yaml.FullLoader)["configuration"]

    all_clusters = {}

    for cluster in script_config["clusters"]:
        name = list(cluster.keys())[0]
        cluster = cluster[name]
        all_clusters[name] = cluster

    return all_clusters


###############
#   GETTERS   #
###############

# todo make sure that useless stuff is removed

"""
def get_cluster_name():
    with open("input.yaml", "r") as file:
        cluster_name = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["cluster"]["name"]
        return cluster_name
"""


def get_workflow_input():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["input_files"]
        return i["storage_bucket"], i["filename"], i["batch_size"], i["number_of_batches"], i["distribution"], i["inter_upload_time"]


def get_cluster_ssh_info():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["cluster"]
        return i["ip_address"], i["port"], i["username"]


def get_run_info():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["run"]
        return i["repetitions"], i["cooldown_time"], i["stop_at_run"]


def get_time_correction():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["other"]
        return i["time_correction"]


"""
def get_test_single_components():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["other"]
        return i["test_single_components"]
"""


def get_debug():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["other"]
        return i["debug"]


def get_do_final_processing():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["run"]
        return i["final_processing"]


def get_train_ml_models():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["ml_library"]
        return i["train_models"]


def get_train_ml_interpolation_models():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["ml_library"]
        return i["train_interpolation_models"]


def get_interpolation_values():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["ml_library"]
        return i["interpolation_values"]


def get_extrapolation_values():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["ml_library"]
        return i["extrapolation_values"]
