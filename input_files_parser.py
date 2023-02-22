import yaml

from oscarp.utils import show_error, show_fatal_error

import global_parameters as gp


def get_resources():

    physical_nodes = _get_physical_nodes(gp.application_dir)

    with open(gp.application_dir + "common_config/candidate_resources.yaml") as file:
        candidate_resources = yaml.load(file, Loader=yaml.Loader)

    resources = {}
    for _, nd in candidate_resources["System"]["NetworkDomains"].items():
        if "ComputationalLayers" in nd:
            for cl_name, cl in nd["ComputationalLayers"].items():
                for resource in list(cl["Resources"].values()):
                    name = resource["name"]
                    processor = list(resource["processors"].values())[0]

                    resources[name] = {
                        "is_physical": False,
                        "oscarcli_alias": "oscar-" + name,
                        "storage_provider": "minio",
                        "storage_provider_alias": "minio-" + name,
                        "ssh": None,
                        "total_nodes": resource["totalNodes"],
                        "max_cpu_cores": float(processor["computingUnits"]),
                        "max_memory_mb": resource["memorySize"],
                        "execution_layer": cl["number"]  # needed for modified candidate deployments file
                    }

    # todo change this, if the node is physical it is written in candidate_resources.yaml
    # todo then look for matches in physical_nodes.yaml, if not found throw fatal error

    # add info if node is physical
    for name, resource in resources.items():
        if name in list(physical_nodes.keys()):
            resource["is_physical"] = True  # urgent, fix, check in candidate_resource instead
            resource["ssh"] = physical_nodes[name]["ssh"]

    # add fake cluster for Lambda/SCAR
    resources["AWS Lambda"] = {
        "is_physical": False,
        "oscarcli_alias": None,
        "storage_provider": "s3",
        "storage_provider_alias": "aws",
        "ssh": None,
        "total_nodes": 1,
        "max_cpu_cores": 1.0 * 1000,
        "max_memory_mb": 4096 * 1000,
    }

    gp.resources = resources


def _get_physical_nodes(application_dir):
    with open(application_dir + "common_config/physical_nodes.yaml") as file:
        physical_nodes_file = yaml.load(file, Loader=yaml.Loader)

    physical_nodes = {}
    for _, cl in physical_nodes_file["ComputationalLayers"].items():
        for _, resource in cl["Resources"].items():
            name = resource["name"]
            fn = resource["fe_node"]
            physical_nodes[name] = {"ssh": "%s@%s/%s" % (fn["ssh_user"], fn["public_ip"], fn["ssh_port"])}

    return physical_nodes


def get_components_and_images():  # todo check this method when have time
    with open(gp.application_dir + "common_config/candidate_deployments.yaml") as file:
        candidate_deployments = yaml.load(file, Loader=yaml.Loader)

    components = {}
    images = {}

    for component_key in candidate_deployments["Components"]:  # component1, component1_partitionX_1...
        component = candidate_deployments["Components"][component_key]
        name = component["name"]
        short_id = shorten_key(component_key)  # C1
        component_resources = []

        if "partition" not in name:
            for container_key in component["Containers"]:
                container = component["Containers"][container_key]
                resources = container["candidateExecutionResources"]
                component_resources += resources

                for r in resources:
                    full_id = short_id + "@" + r  # C1@VM1
                    images[full_id] = {
                        "image": container["image"],
                        "cpu": container["computingUnits"],
                        "memory": container["memorySize"]
                    }

            components[component_key] = {
                "name": name,
                "resources": component_resources
            }

        else:
            target, matches = get_correct_partition(component)
            target_string = "partition" + target

            for m in matches:
                for container_key in component["Containers"]:
                    container = component["Containers"][container_key]
                    image = container["image"]
                    resources = container["candidateExecutionResources"]
                    component_resources += resources

                    match_string = "partition" + str(m)

                    for r in resources:
                        full_id = short_id.replace(target, m) + "@" + r  # C1@VM1
                        images[full_id] = {
                            "image": container["image"].replace(target, m),
                            "cpu": container["computingUnits"],
                            "memory": container["memorySize"]
                        }

                components[component_key.replace(target, m)] = {
                    "name": name.replace(target_string, match_string),
                    "resources": component_resources
                }

    gp.components = components
    gp.images = images


def get_correct_partition(component):
    with open(gp.application_dir + "aisprint/designs/component_partitions.yaml") as file:
        component_partitions = yaml.load(file, Loader=yaml.Loader)["components"]

    name, partition = component["name"].split("_partition")

    partitions = component_partitions[name]["partitions"]
    matches = []
    for p in partitions:
        if p[-1] == partition[-1]:
            matches.append(p[-3])

    return partition[0], matches


def get_run_parameters():
    with open(gp.application_dir + "oscarp/run_parameters.yaml") as file:
        run_parameters = yaml.load(file, Loader=yaml.Loader)

    testing_parameters = run_parameters["parallelism"]
    del(run_parameters["parallelism"])
    length = len(list(testing_parameters.values())[0])

    for value in list(testing_parameters.values()):
        if len(value) != length:
            show_fatal_error("Parallelism lists of different length in file \"run_parameters.yaml\"")

    gp.run_parameters = run_parameters
    gp.testing_parameters = testing_parameters


# merges the components with the testing parameters, as to have everything in one place
def get_testing_components():
    testing_components = {}

    for c in gp.components.keys():
        short_id = shorten_key(c)

        if "partition" in c:
            a, _, b = c.split("_")
            x = a + "_partitionX_" + b
            parallelism = gp.testing_parameters[x]
        else:
            parallelism = gp.testing_parameters[c]

        for r in gp.components[c]["resources"]:
            full_id = short_id + "@" + r
            testing_components[full_id] = {
                "name": gp.components[c]["name"],
                "parallelism": parallelism,
            }

    gp.testing_components = testing_components


def shorten_key(name):
    if "partition" in name:
        c, p, i = name.split("_")
        c = c.strip("component")
        p = p.strip("partition")
        return "C" + str(c) + "P" + str(p) + "." + str(i)
    else:
        c = name.strip("component")
        return "C" + str(c)
