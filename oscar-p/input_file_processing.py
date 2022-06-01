# contains the methods related to the processing of the input file

import yaml

from termcolor import colored

from utils import get_valid_input, show_warning, show_error


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
    with open("input.yaml", "r") as file:
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


# receives a variable (ie mem array for a service), a counter and a boolean variable for "continue"
# if variable is a list and counter is lower than its lenght it means another run should be scheduled, cont is set to 1
# if variable is not a list, or it is, and we reached its end, it returns the last value and doesn't change cont
def var_process(x, i, cont):
    if type(x) is list:
        if len(x) > i+1:
            x = x[i]
            cont = 1
        else:
            x = x[-1]
    return x, cont


# starting from the input file it creates a sequence of run that the script will then cycle on
# "base" is a basic skeleton list of runs (if a run must be repeated twice, it shows only once in base)
# "runs" is the full list of runs, including repetitions (implemented to add state saving later on)
# "nodes" is just the number of nodes
def run_scheduler():
    with open("input.yaml", "r") as file:
        script_config = yaml.load(file, Loader=yaml.FullLoader)["configuration"]
        repetitions = script_config["run"]["repetitions"]
        parallelism = script_config["run"]["parallelism"]

        # if parallelism field is not empty it overrides the other fields
        if not isinstance(parallelism, type(None)):
            base = run_scheduler_parallel(parallelism, script_config["services"])
            runs = []
            j = 1
            for i in range(repetitions):
                for elem in base:
                    run = elem.copy()
                    run["id"] = "Run #" + str(j)
                    j += 1
                    runs.append(run)
            return base, runs, 0

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


# given the hardware structure of the cluster, returns a list of the possible achievable parallelism levels
# i.e. on a cluster with 2 nodes with 4 cores each, parallelism levels of 5 or 7 are unachievable
def get_possible_parallelisms(total_nodes, max_cores):
    possible_parallelism = {}
    for c in range(1, int(max_cores) + 1):
        for n in range(1, int(total_nodes) + 1):
            p = c * n
            cores = max_cores / c
            cores = round(cores, 1) - 0.1
            possible_parallelism[p] = (cores, n)
    return possible_parallelism


# alternative to run_scheduler that generates the list of runs automatically based on the parallelism array
# change how this behaves, use parallelism by default and overrides with fields from service if not empty
def run_scheduler_parallel(parallelism, services):
    total_nodes, max_cores, max_memory_mb = get_worker_nodes_info()
    possible_parallelism = get_possible_parallelisms(total_nodes, max_cores)

    i = 0
    runs = []
    for p in parallelism:
        if p not in possible_parallelism.keys():
            show_warning("A parallelism of " + str(p) + " is not achieavable on this cluster")
        else:
            container_services = []

            for s in services:
                name = list(s.keys())[0]
                cpu = possible_parallelism[p][0]
                memory, _ = var_process(s[name]["memory_mb"], 0, 0)

                s = {
                    "name": name,
                    "cpu": str(cpu),
                    "memory": str(memory),
                    "image": s[name]["image"],
                    "script": s[name]["script"],
                    "input_bucket": s[name]["input_bucket"],
                    "output_buckets": s[name]["output_buckets"]
                }
                container_services.append(s)
            run = {
                "id": "Run #" + str(i + 1),
                "nodes": possible_parallelism[p][1],
                "services": container_services
            }
            i += 1
            runs.append(run)

    return runs


def new_run_scheduler():
    total_nodes, max_cores, max_memory_mb = get_worker_nodes_info()
    possible_parallelism = get_possible_parallelisms(total_nodes, max_cores)
    
    # todo move this to getters functions
    with open("input.yaml", "r") as file:
        script_config = yaml.load(file, Loader=yaml.FullLoader)["configuration"]
        repetitions = script_config["run"]["repetitions"]
        parallelism = script_config["run"]["parallelism"]
        services_list = script_config["services"]
        
    i = 0
    runs = []
    for p in parallelism:
        if p not in possible_parallelism.keys():
            show_error("A parallelism of " + str(p) + " is not achieavable on this cluster")
        else:
            for s in services_list:
                
                name = list(s.keys())[0]
                cpu = s[name]["cpu"]
                print("cpu", cpu)
                # if field is not empty ...
                if not isinstance(cpu, type(None)):
                    print(name, "not empty")
                    
                name = list(s.keys())[0]
                cpu = possible_parallelism[p][0]
                memory, _ = var_process(s[name]["memory_mb"], 0, 0)

                s = {
                    "name": name,
                    "cpu": str(cpu),
                    "memory": str(memory),
                    "image": s[name]["image"],
                    "script": s[name]["script"],
                    "input_bucket": s[name]["input_bucket"],
                    "output_buckets": s[name]["output_buckets"]
                }
                container_services.append(s)
            run = {
                "id": "Run #" + str(i + 1),
                "nodes": possible_parallelism[p][1],
                "services": container_services
            }
            i += 1
            runs.append(run)
        break

    return runs
    

def set_services_for_run(services_list, parallelism_cores, parallelism_nodes):
    """
    :param services_list: list of services as red from input.yaml file
    """
    
    for s in services_list:
        name = list(s.keys())[0]  # needed to fetch the service name, ignore it
        cpu = s[name]["cpu"]
        
        # if field is not empty uses this value
        if not isinstance(cpu, type(None)):
            cpu = s[name]["cpu"]  # todo remove, only temporary so that my neurons don't suicide when reading this
        # todo need to use var process here too since it may receive an array
        
        # otherwise use the one calculated to achieve required parallelism
        else:
            cpu = parallelism_cores
            
        memory, _ = var_process(s[name]["memory_mb"], 0, 0)

        s = {
            "name": name,
            "cpu": str(cpu),
            "memory": str(memory),
            "image": s[name]["image"],
            "script": s[name]["script"],
            "input_bucket": s[name]["input_bucket"],
            "output_buckets": s[name]["output_buckets"]
        }
        container_services.append(s)
    
    return
    


# given two runs, it shows the difference in cpu and memory settings
def runs_diff(run1, run2):
    if run1["nodes"] != run2["nodes"]:
        print("\t\t" + colored(str(run1["nodes"]), "green") + " -> " + colored(str(run2["nodes"]), "green") + " node(s)")
    for i in range(len(run1["services"])):
        s1 = run1["services"][i]
        s2 = run2["services"][i]
        service_name = "{:<20}".format(s1["name"])
        if s1["cpu"] != s2["cpu"]:
            print("\t\t" + colored(service_name, "blue") + "cpu: " + colored(s1["cpu"], "green") + " -> " + colored(s2["cpu"], "green"))
        if s1["memory"] != s2["memory"]:
            print("\t\t" + colored(service_name, "blue") + "memory: " + colored(s1["memory"], "green") + " mb -> " + colored(s2["memory"], "green") + " mb")
            
            
def show_all(run):
    print("\t" + run["id"])
    print("\t\t" + colored(str(run["nodes"]), "green") + " node(s)")
    for s in run["services"]:
        service_name = "{:<20}".format(s["name"])
        print("\t\t" + colored(service_name, "blue") + "cpu: " + colored(s["cpu"], "green") + " , memory: " + colored(s["memory"], "green") + " mb")


# todo add total number of runs
def show_runs(base, nodes, repetitions):
    print("\nScheduler:")
    show_all(base[0])
    
    for i in range(1, len(base)):
        print("\t" + base[i]["id"])
        runs_diff(base[i-1], base[i])

    if isinstance(nodes, type([])):
        print("\n\tRepeated " + colored(str(repetitions), "green") + " time(s) on " + ", ".join([colored(str(n), "green") for n in nodes]) + " nodes")
    else:
        print("\n\tRepeated " + colored(str(repetitions), "green") + " time(s)")

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
