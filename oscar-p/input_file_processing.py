# contains the methods related to the processing of the input file

import yaml

from termcolor import colored

from utils import get_valid_input, show_warning, show_error, show_fatal_error


def consistency_check(services):
    """
    receives the list of services, we should only have one empty "father" field (the first service)
    :param services: ordered list of services, each a dictionary containing name of the service (as string),
            input bucket (as string) and output buckets (list of strings)
    :returns: True if workflow is consistent, False otherwise
    """
    root_found = False  # root being the node with "father" field empty

    for service in services:
        if service["father"] == "":
            if not root_found:
                root_found = True
            else:
                return False
    return True


# can be used both for the ordered services list and for the services under run
def get_service_by_name(name, services):
    for s in services:
        if s["name"] == name:
            return s


def find_root(services):
    for s in services:
        if s["father"] == "":
            return s


# reorders the services based on the identified workflow
def reorder_services(services):

    # first it orders the services
    ordered_services = [find_root(services)]

    i = 0

    while True:
        s = ordered_services[i]
        for c in s["children"]:
            child = get_service_by_name(c, services)
            if child not in ordered_services:
                ordered_services.append(child)
        i += 1

        if i == len(ordered_services):  # if list empty break
            break

    # then it deletes the useless fields
    for s in ordered_services:
        s.pop("father")
        s.pop("children")

    return ordered_services


def get_output_buckets_name(bucket_list):
    buckets_names = []
    for b in bucket_list:
        buckets_names.append(b["path"])

    return buckets_names


# given an input file, it tries to find a valid workflow by looking at the input and output bucket of every service
# it then returns the list of the ordered services
def workflow_analyzer():

    # first creates a list of services with their input and output buckets
    with open("demo.yaml", "r") as file:
        script_config = yaml.load(file, Loader=yaml.FullLoader)["configuration"]
        services = []
        for s in script_config["services"]:
            name = list(s.keys())[0]
            service = {
                "name": name,
                "input": s[name]["input_bucket"],
                "outputs": get_output_buckets_name(s[name]["output_buckets"]),
                "father": "",
                "children": []
                }
            services.append(service)

    # then tries to find the father and children service, for every service, by looking at the buckets
    for s in services:
        for t in services:
            for bucket in t["outputs"]:
                if s["input"] == bucket:
                    s["father"] = t["name"]
                    t["children"].append(s["name"])

    # and checks if the workflow is consistent
    if not consistency_check(services):  # if returns False, hence not consistent
        show_error("Workflow inconsistent")
        quit()

    # finally, reorder the services based on the identified workflow and returns it
    return reorder_services(services)


# given a list of ordered services it prints the workflow
def show_workflow(services):

    print("Workflow:\n\tstorage -> " + services[0]["input"])

    for s in services:
        input_bucket = s["input"]
        for output_bucket in s["outputs"]:
            print("\t" + colored(input_bucket) + " -> |" + colored(s["name"], "blue") + "| -> " + colored(output_bucket))
            input_bucket = " " * len(input_bucket)

    return


# receives a variable (i.e. mem array for a service), a counter and a boolean variable for "continue"
# if variable is a list and the counter is lower than its length (i.e. list has length 3 and counter is 1) it means
#   another run should be scheduled, so cont is set to 1
# if variable isn't a list, or it is but we reached its end, it returns the last available value and doesn't change cont
def var_process(x, i, cont):
    if type(x) is list:
        if len(x) > i+1:
            x = x[i]
            cont = 1
        else:
            x = x[-1]
    return x, cont


# todo streamline function
# starting from the input file it creates a sequence of run that the script will then cycle on
# "base" is a basic skeleton list of runs (if a run must be repeated twice, it shows only once in base)
# "runs" is the full list of runs, including repetitions (implemented to add state saving later on)
# "nodes" is just the number of nodes
def run_scheduler():
    with open("demo.yaml", "r") as file:
        script_config = yaml.load(file, Loader=yaml.FullLoader)["configuration"]
        repetitions = script_config["run"]["repetitions"]
        parallelism = script_config["run"]["parallelism"]
        services = script_config["services"]

    base = cycle_through_clusters(services, parallelism)
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


def cycle_through_clusters(services, parallelism):
    """
    generates a list of the runs, considering all the clusters selected for the services one by one
    :return:
    """

    clusters = get_clusters_info()
    base = []

    i = 0
    while True:
        cont = 0

        # generates a temporary list of services, each with only one cluster and not a list
        # makes the job of configuring each service for each run a lot easier
        temp_services = []

        for s in services:
            name = list(s.keys())[0]
            cluster, cont = var_process(s[name]["cluster"], i, cont)
            # next op makes a copy of the service that can be modified without messing up the original
            t = {name: s[name].copy()}
            t[name]["cluster"] = cluster

            temp_services.append(t)

        # now that we have the temp_services list with a single cluster, we can call the actual run scheduler(s)
        # if the parallelism field is not empty it overrides the other fields
        if not isinstance(parallelism, type(None)):
            base += base_scheduler_parallel(clusters, parallelism, temp_services)
        else:
            return

        if cont == 0:
            break
        else:
            i += 1

    return base


# given the hardware structure of the cluster, returns a list of the possible achievable parallelism levels
# i.e. on a cluster with 2 nodes with 4 cores each, parallelism levels of 5 or 7 are unachievable
def get_possible_parallelisms(total_nodes, max_cores, max_memory):
    possible_parallelism = {}
    for c in range(1, int(max_cores) + 1):
        for n in range(1, int(total_nodes) + 1):
            p = c * n
            cores = max_cores / c
            mem = int((max_memory - 128) / c)
            cores = round(cores, 1) - 0.1
            possible_parallelism[p] = (cores, n, mem)
    return possible_parallelism


# todo finish comments
def base_scheduler_parallel(clusters, parallelism, services):
    """
    schedules the base runs by using the "parallelism" array
    :param clusters:
    :param parallelism:
    :param services:
    :return:
    """

    base = []

    for p in parallelism:

        configured_services = []  # services configured correctly for the given parallelism level

        for s in services:
            # collects all necessary info
            service_name = list(s.keys())[0]
            current_service = s[service_name]
            current_cluster_name = current_service["cluster"]
            current_cluster = clusters[current_cluster_name]
            possible_parallelism = current_cluster["possible_parallelism"]
            minio_alias = current_cluster["minio_alias"]
            cluster_architecture = current_cluster["architecture"]

            # checks if the requested parallelism level is achievable on the selected cluster
            # if not it returns the closest available level
            p = get_closest_parallelism_level(p, possible_parallelism, current_service["cluster"], True)

            # after choosing a parallelism level we gather the node configuration for the selected cluster
            cpu = possible_parallelism[p][0]
            memory = possible_parallelism[p][2]

            # we pick the correct docker image for the selected cluster architecture
            image = get_correct_docker_image(current_service, cluster_architecture, service_name)

            # puts everything in a dictionary
            new_service = {
                "name": service_name,
                "cpu": str(cpu),
                "memory": str(memory),
                "image": image,
                "cluster": current_cluster_name,
                "script": current_service["script"],
                "minio_alias": minio_alias,
                "input_bucket": current_service["input_bucket"],
                "output_buckets": current_service["output_buckets"]
                }
            configured_services.append(new_service)

        # id will be added later on
        run = {
            "id": "",
            "parallelism": p,
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
            run_id = i*repetitions + j + 1
            r = b.copy()
            r["id"] = "Run #" + str(run_id)
            runs.append(r)

    return runs


# todo comment
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


def get_closest_parallelism_level(requested_parallelism, possible_parallelism, cluster_name, verbose):
    """
    checks if the requested parallelism level is achievable on the selected cluster, if not it returns the closest
        available level
    :param requested_parallelism: requested parallelism level
    :param possible_parallelism: list of possible parallelism levels
    :param cluster_name: name of the cluster
    :param verbose: boolean, if true shows warning when exact match is not possible
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
    with open("demo.yaml", "r") as file:
        script_config = yaml.load(file, Loader=yaml.FullLoader)["configuration"]

    layers = script_config["layers"]
    all_clusters = {}

    for layer in layers:
        name = list(layer.keys())[0]
        layer = layer[name]
        clusters = layer["clusters"]

        for cluster in clusters:
            name = list(cluster.keys())[0]
            cluster = cluster[name]
            all_clusters[name] = cluster
            all_clusters[name]["possible_parallelism"] = get_possible_parallelisms(cluster["total_nodes"],
                                                                                   cluster["max_cpu_cores"],
                                                                                   cluster["max_memory_mb"])

    return all_clusters
    

# given two runs, it shows the difference in cpu and memory settings
def runs_diff_services(run1, run2):

    print("\t\tServices:")
    for i in range(len(run1["services"])):
        s1 = run1["services"][i]
        s2 = run2["services"][i]
        service_name = "{:<20}".format(s1["name"])

        output = ""
        if s1["cpu"] != s2["cpu"]:
            output += "cpu: " + colored(s1["cpu"], "green") + " -> " + colored(s2["cpu"], "green")
            output += ", memory: " + colored(s1["memory"], "green") + " mb -> " + colored(s2["memory"], "green") + " mb"

        if output != "":
            print("\t\t\t" + colored(service_name, "blue") + output)
        else:
            print("\t\t\t" + colored(service_name, "blue") + colored("unchanged", "green"))

        output = ""
        if s1["cluster"] != s2["cluster"]:
            output += "cluster: " + colored(s1["cluster"], "green") + " -> " + colored(s2["cluster"], "green")

        if output != "":
            print("\t\t\t" + colored(service_name, "blue") + output)


def runs_diff_clusters(last_parallelism, run, clusters, mentioned_clusters):
    current_parallelism = run["parallelism"]

    print("\t\tClusters:")
    for cluster_name in mentioned_clusters:
        cluster = clusters[cluster_name]
        old_closest_parallelism = get_closest_parallelism_level(last_parallelism,
                                                                cluster["possible_parallelism"], cluster_name, False)
        new_closest_parallelism = get_closest_parallelism_level(current_parallelism,
                                                                cluster["possible_parallelism"], cluster_name, False)
        old_nodes = cluster["possible_parallelism"][old_closest_parallelism][1]
        new_nodes = cluster["possible_parallelism"][new_closest_parallelism][1]
        cluster_name = "{:<20}".format(cluster_name)

        output = ""
        if old_nodes != new_nodes:
            output += "cluster: " + colored(old_nodes, "green") + " -> " + colored(new_nodes, "green")

        if output != "":
            print("\t\t\t" + colored(cluster_name, "blue") + output)
        else:
            print("\t\t\t" + colored(cluster_name, "blue") + colored("unchanged", "green"))

    return current_parallelism


def show_all_services(run):
    print("\t" + run["id"] + " - Parallelism level: " + str(run["parallelism"]))

    print("\t\tServices:")
    mentioned_clusters = []
    for s in run["services"]:
        service_name = "{:<20}".format(s["name"])
        print("\t\t\t" + colored(service_name, "blue")
              + "cpu: " + colored(s["cpu"], "green")
              + " , memory: " + colored(s["memory"], "green") + " mb"
              + " , cluster: " + colored(s["cluster"], "green"))

        mentioned_clusters.append(s["cluster"])

    return mentioned_clusters


def show_all_clusters(run, clusters, mentioned_clusters):
    print("\t\tClusters:")
    requested_parallelism = run["parallelism"]

    for cluster_name in mentioned_clusters:
        cluster = clusters[cluster_name]
        closest_parallelism = get_closest_parallelism_level(requested_parallelism,
                                                            cluster["possible_parallelism"], cluster_name, False)
        nodes = cluster["possible_parallelism"][closest_parallelism][1]
        cluster_name = "{:<20}".format(cluster_name)
        print("\t\t\t" + colored(cluster_name, "blue") + "nodes: " + colored(str(nodes), "green"))

    return requested_parallelism


# todo add total number of runs
def show_runs(base, repetitions, clusters):
    print("\nScheduler:")
    mentioned_clusters = show_all_services(base[0])
    last_parallelism = show_all_clusters(base[0], clusters, mentioned_clusters)
    print()

    for i in range(1, len(base)):
        print("\t" + base[i]["id"] + " - Parallelism level: " + str(base[i]["parallelism"]))
        runs_diff_services(base[i-1], base[i])
        last_parallelism = runs_diff_clusters(last_parallelism, base[i], clusters, mentioned_clusters)
        print()

    print("\n\tRepeated " + colored(str(repetitions), "green") + " time(s), "
          + str(len(base) * repetitions) + " runs in total")

    # todo re-enable this after testing!
    # value = get_valid_input("Do you want to proceed? (y/n)\t", ["y", "n"])
    # print()  # just for spacing
    value = "y"

    if value == "n":
        print(colored("Exiting...", "red"))
        quit()


###############
#   GETTERS   #
###############

def get_cluster_name():
    with open("input.yaml", "r") as file:
        cluster_name = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["cluster"]["name"]
        return cluster_name


def get_workflow_input():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["input_files"]
        return i["filename"], i["number_of_files"], i["distribution"], i["inter_upload_time"], i["mc_alias"]


def get_mc_alias():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["input_files"]
        return i["mc_alias"]


def get_cluster_ssh_info():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["cluster"]
        return i["ip_address"], i["port"], i["username"]


def get_run_info():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["run"]
        return i["name"], i["repetitions"], i["cooldown_time"]


def get_worker_nodes_info():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["worker_nodes"]
        return i["total_nodes"], i["max_cpu_cores"], i["max_memory_mb"]


def get_time_correction():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["other"]
        return i["time_correction"]


def get_test_single_components():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["other"]
        return i["test_single_components"]


def get_debug():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["other"]
        return i["debug"]


def get_use_ml_library():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["ml_library"]
        return i["use"]


def get_interpolation_values():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["ml_library"]
        return i["interpolation_values"]


def get_extrapolation_values():
    with open("input.yaml", "r") as file:
        i = yaml.load(file, Loader=yaml.FullLoader)["configuration"]["ml_library"]
        return i["extrapolation_values"]
