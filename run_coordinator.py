import yaml
import copy
import random

from datetime import date

import global_parameters as gp


def old_make_oscar_p_input_file(deployment, service_number=0, is_single_service=False):

    services = []
    clusters = []
    clusters_dict = {}

    run_parameters = copy.deepcopy(gp.run_parameters)

    i = service_number

    if is_single_service:
        run_parameters["run"]["final_processing"] = True
    else:
        run_parameters["run"]["final_processing"] = False

    run_parameters["run"]["stop_at_run"] = gp.current_base_index + 1

    if is_single_service:
        run_parameters["input_files"]["storage_bucket"] = "bucket" + str(i)
        run_parameters["input_files"]["filename"] = None
    else:  # if it's full workflow
        # run_parameters["run"]["repetitions"] = 1  # todo keep?
        pass

    for s in deployment:  # single components in deployments
        name = gp.testing_components[s]["name"].replace('_', '-')
        r = s.split("@")[1]

        is_lambda = (r == "AWS Lambda")

        if is_single_service:  # lambdas are never tested as single services
            input_bucket = "temp" + str(i)
            output_bucket = "trash" + str(i)
        else:
            input_bucket = "bucket" + str(i)
            output_bucket = "bucket" + str(i + 1)

        services.append({name: {
            "cluster": r,
            "image": gp.images[s]["image"],
            "input_bucket": input_bucket,
            "output_bucket": output_bucket,
            "cpu": gp.images[s]["cpu"],
            "memory": gp.images[s]["memory"],
            "parallelism": gp.testing_components[s]["parallelism"],
            "is_lambda": is_lambda
        }})

        # easier to use a dict here to avoid duplications
        if r not in clusters_dict.keys():
            clusters_dict[r] = gp.resources[r]

        i += 1

    # converts the dict to a list
    for c in clusters_dict.keys():
        clusters_dict[c]["nodes"] = gp.clusters_node_requirements[c]["nodes"]
        clusters.append({c: clusters_dict[c]})

    oscar_p_input_file = {
        "services": services,
        "clusters": clusters
    }

    # oscar_p_input_file = oscar_p_input_file | run_parameters
    oscar_p_input_file.update(run_parameters)

    oscar_p_input_file = {"configuration": oscar_p_input_file}

    with open("input.yaml", "w") as file:
        yaml.dump(oscar_p_input_file, file, sort_keys=False)

    # input(">_")


def make_oscar_p_input_file():

    run_parameters = copy.deepcopy(gp.run_parameters)
    # run_parameters["run"]["final_processing"] = is_last_run
    run_parameters["run"]["stop_at_run"] = gp.current_base_index + 1

    clusters_dict = {}
    for unit in gp.current_deployment:
        r = unit.split("@")[1]

        # easier to use a dict here to avoid duplications
        if r not in clusters_dict.keys():
            clusters_dict[r] = gp.resources[r]

    # converts the dict to a list
    clusters = []
    for c in clusters_dict.keys():
        clusters_dict[c]["nodes"] = gp.clusters_node_requirements[c]["nodes"]
        clusters.append({c: clusters_dict[c]})

    # converts the services list to the correct format
    services = []
    for s in gp.current_services:
        services.append({s["name"]: s})

    oscar_p_input_file = {
        "services": services,
        "clusters": clusters
    }

    # oscar_p_input_file = oscar_p_input_file | run_parameters
    oscar_p_input_file.update(run_parameters)

    oscar_p_input_file = {"configuration": oscar_p_input_file}

    with open("input.yaml", "w") as file:  # todo this will have to be written inside current_deployment_dir instead
        yaml.dump(oscar_p_input_file, file, sort_keys=False)

    return


def make_oscar_p_input_file_single(service, service_number=0):

    services = []
    clusters = []
    clusters_dict = {}

    run_parameters = copy.deepcopy(gp.run_parameters)

    i = service_number

    # run_parameters["run"]["final_processing"] = is_last_run
    run_parameters["run"]["stop_at_run"] = gp.current_base_index + 1

    run_parameters["input_files"]["storage_bucket"] = "bucket" + str(i)
    run_parameters["input_files"]["filename"] = None

    name = gp.testing_components[service]["name"].replace('_', '-')
    r = service.split("@")[1]

    is_lambda = (r == "AWS Lambda")

    if is_lambda:
        input_bucket = gp.current_services[service_number]["input_bucket"]
    else:
        input_bucket = "temp" + str(i)

    output_bucket = "trash" + str(i)

    services.append({name: {
        "cluster": r,
        "image": gp.images[service]["image"],
        "input_bucket": input_bucket,
        "output_bucket": output_bucket,
        "cpu": gp.images[service]["cpu"],
        "memory": gp.images[service]["memory"],
        "parallelism": gp.testing_components[service]["parallelism"],
        "is_lambda": is_lambda
    }})

    # easier to use a dict here to avoid duplications
    if r not in clusters_dict.keys():
        clusters_dict[r] = gp.resources[r]

    i += 1

    # converts the dict to a list
    for c in clusters_dict.keys():
        clusters_dict[c]["nodes"] = gp.clusters_node_requirements[c]["nodes"]
        clusters.append({c: clusters_dict[c]})

    oscar_p_input_file = {
        "services": services,
        "clusters": clusters
    }

    # oscar_p_input_file = oscar_p_input_file | run_parameters
    oscar_p_input_file.update(run_parameters)

    oscar_p_input_file = {"configuration": oscar_p_input_file}

    with open("input.yaml", "w") as file:
        yaml.dump(oscar_p_input_file, file, sort_keys=False)

    return


def make_services_list():

    gp.current_services = []
    gp.has_lambdas = False

    today = date.today().strftime("%d%m%y")
    s3_main_bucket_name = "scar-bucket-"
    s3_main_bucket_name += today + "-" + str(random.randint(100, 999))
    s3_main_bucket_name = "scar-bucket-130123-509"

    i = 0

    for unit in gp.current_deployment:  # single components in deployments
        name = gp.testing_components[unit]["name"].replace('_', '-')
        r = unit.split("@")[1]

        is_lambda = (r == "AWS Lambda")

        input_bucket = "bucket" + str(i)
        output_bucket = "bucket" + str(i + 1)

        if is_lambda:
            input_bucket = s3_main_bucket_name + "/" + input_bucket
            output_bucket = s3_main_bucket_name + "/" + output_bucket
            gp.has_lambdas = True

        gp.current_services.append({
            "name": name,
            "cluster": r,
            "image": gp.images[unit]["image"],
            "input_bucket": input_bucket,
            "output_bucket": output_bucket,
            "cpu": gp.images[unit]["cpu"],
            "memory": gp.images[unit]["memory"],
            "parallelism": gp.testing_components[unit]["parallelism"],
            "is_lambda": is_lambda
        })

        i += 1