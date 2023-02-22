import time
import requests
import yaml

from toscarizer.utils import (DEPLOYMENTS_FILE,
                              RESOURCES_FILE,
                              BASE_DAG_FILE,
                              PHYSICAL_NODES_FILE,
                              CONTAINERS_FILE,
                              parse_dag,
                              parse_resources)

from toscarizer.im_tosca import gen_tosca_yamls

from termcolor import colored
from oscarp.utils import configure_ssh_client, get_ssh_output, auto_mkdir, show_fatal_error, get_command_output_wrapped, \
    delete_file

import global_parameters as gp

import oscarp.oscarp as oscarp


# # # # # # # # # # # #
# Virtual infrastructures
# # # # # # # # # # # #

def create_virtual_infrastructures(infrastructure_id_override):
    if gp.is_dry:
        return

    # virtual is the list of the infrastructures that need to be deployed
    # an infrastructure is added as long as: 1_ it's not AWS lambda, 2_ it's not physical, 3_ it's not already deployed
    virtual = []

    for c in gp.current_deployment:
        c, r = c.split('@')
        if r != "AWS Lambda" and not gp.resources[r]["is_physical"] and r not in gp.virtual_infrastructures.keys():
            virtual.append((c, r))

    if virtual:
        print(colored("Deploying virtual infrastructures...", "yellow"))
    else:
        return

    auto_mkdir(gp.current_deployment_dir + ".toscarizer")
    to_deploy = _generate_modified_candidate_files(virtual)
    tosca_files = generate_tosca()

    cleaned_tosca_files = clean_and_rename_tosca_files(to_deploy, tosca_files)

    delete_unused_virtual_infrastructures()

    for tosca_file in cleaned_tosca_files:
        resource = tosca_file.split('/')[-1].split('.')[0]
        inf_url = deploy_virtual_infrastructure(tosca_file, resource, None)
        gp.virtual_infrastructures[resource] = {
            "inf_url": inf_url,
            "inf_id": inf_url.split('/')[-1],
            "tosca_file_path": tosca_file
        }

    wait_for_infrastructure_deployment()
    configure_executables()  # todo this needs to be moved, since it'll be done for physical too

    print(colored("Done", "green"))
    return


def _generate_modified_candidate_files(virtual,):
    with open("%s%s" % (gp.application_dir, DEPLOYMENTS_FILE), 'r') as f:
        components = yaml.safe_load(f)["Components"]

    to_deploy = {}

    for c, r in virtual:
        c = c.replace("C", "component")

        if r not in to_deploy.keys():
            to_deploy[r] = components[c]["name"]

            for container in components[c]["Containers"].values():
                if r in container["candidateExecutionResources"]:  # found the right container
                    components[c]["Containers"]["container1"] = container
                    components[c]["Containers"]["container1"]["candidateExecutionResources"] = [r]
                    components[c]["candidateExecutionLayers"] = [gp.resources[r]["execution_layer"]]
                    break

    with open("%s.toscarizer/modified_candidate_deployments.yaml" % gp.current_deployment_dir, 'w') as file:
        yaml.dump({"Components": components}, file, sort_keys=False)

    return to_deploy


def generate_tosca():
    elastic = 0
    auth_data = None

    dag = parse_dag("%s%s" % (gp.application_dir, BASE_DAG_FILE))
    deployments_file = "%s/.toscarizer/modified_candidate_deployments.yaml" % gp.current_deployment_dir
    resources_file = "%s%s" % (gp.application_dir, RESOURCES_FILE)

    toscas = gen_tosca_yamls(dag, resources_file, deployments_file,
                             "%s%s" % (gp.application_dir, PHYSICAL_NODES_FILE), elastic, auth_data)

    tosca_files = []

    for cl, tosca in toscas.items():
        tosca_file = "%s.toscarizer/%s.yaml" % (gp.current_deployment_dir, cl)
        tosca_files.append(tosca_file)
        with open(tosca_file, 'w+') as f:
            yaml.safe_dump(tosca, f, indent=2)
        # print(colored("TOSCA file has been generated for component %s." % cl, "green"))

    return tosca_files


def clean_and_rename_tosca(tosca_file, component_name, resource, node_requirements):
    # domain_name = "polimi-aisprint.click"
    domain_name = gp.run_parameters["other"]["domain_name"]

    with open(tosca_file, 'r') as file:
        content = yaml.safe_load(file)

    content["topology_template"]["inputs"]["domain_name"]["default"] = domain_name

    content["topology_template"]["node_templates"]["oscar"]["properties"]["yunikorn_enable"] = True
    content["topology_template"]["node_templates"]["lrms_front_end"]["properties"]["install_yunikorn"] = True

    del (content["topology_template"]["node_templates"]["oscar_service_" + component_name])
    del (content["topology_template"]["outputs"]["oscar_service_url"])
    del (content["topology_template"]["outputs"]["oscar_service_cred"])
    content["topology_template"]["node_templates"]["wn_resource1"]["capabilities"]["scalable"]["properties"]["count"] = \
        node_requirements[resource]["nodes"][0]

    filename = gp.current_deployment_dir + ".toscarizer/" + resource + ".yaml"

    with open(filename, 'w') as file:
        yaml.dump(content, file, sort_keys=False)

    return filename


def clean_and_rename_tosca_files(to_deploy, tosca_files):

    cleaned_tosca_files = []

    for tosca_file in tosca_files:
        for r, c in to_deploy.items():
            if c in tosca_file:
                filename = clean_and_rename_tosca(tosca_file, c, r, gp.clusters_node_requirements)
                cleaned_tosca_files.append(filename)

    for tosca_file in tosca_files:
        if tosca_file not in cleaned_tosca_files:
            delete_file(tosca_file)

    return cleaned_tosca_files


def deploy_virtual_infrastructure(tosca_file, resource, inf_id):
    im_url = "https://appsgrycap.i3m.upv.es:31443/im/"  # todo change before final (?)
    tosca_dir = "%s/aisprint/deployments/base/im" % gp.application_dir

    im_auth = "%s/auth.dat" % tosca_dir

    with open(im_auth, 'r') as f:
        auth_data = f.read().replace("\n", "\\n")

    headers = {"Authorization": auth_data, 'Content-Type': 'text/yaml'}

    with open(tosca_file, 'rb') as f:
        data = f.read()
        # print(data)
    try:
        if inf_id is None:
            resp = requests.request("POST", "%s/infrastructures" % im_url, headers=headers, data=data)
            print(colored("Resource %s is being deployed..." % resource, "yellow"))
        else:
            resp = requests.request("POST", "%s/infrastructures/%s" % (im_url, inf_id), headers=headers, data=data)
            print(colored("Resource %s is being updated..." % resource, "yellow"))
        return resp.text
    except Exception as ex:
        print("Exception:")
        print(str(ex))
        return


def wait_for_infrastructure_deployment(is_update=False):
    if not is_update:
        print(colored("Waiting for infrastructure deployment (this may take up to 15 minutes)...", "yellow"))
        time.sleep(60*8)
    else:
        print(colored("Waiting for infrastructure update (this should take only a few minutes)...", "yellow"))
    completed = False
    while not completed:
        completed = True
        time.sleep(30)
        for resource in gp.virtual_infrastructures.keys():
            inf_url = gp.virtual_infrastructures[resource]["inf_url"]
            state = get_state(inf_url, gp.application_dir)
            if state != "configured":
                completed = False

    if not is_update:
        time.sleep(30)
    return


def get_state(inf_url, application_dir):
    tosca_dir = "%s/aisprint/deployments/base/im" % application_dir

    im_auth = "%s/auth.dat" % tosca_dir

    with open(im_auth, 'r') as f:
        auth_data = f.read().replace("\n", "\\n")

    headers = {"Authorization": auth_data, "Content-Type": "application/json"}
    try:
        resp = requests.get("%s/state" % inf_url, verify=True, headers=headers)
        success = resp.status_code == 200
        if success:
            return resp.json()["state"]["state"]
        else:
            return resp.text
    except Exception as ex:
        print(str(ex))
        return str(ex)


def configure_executables():  # todo this will also have to include physical infrastructures
    for resource in gp.virtual_infrastructures.keys():
        # get outputs from IM
        inf_url = gp.virtual_infrastructures[resource]["inf_url"]
        outputs = get_outputs(inf_url)

        # set for oscar_cli
        oscar_endpoint = outputs["oscarui_endpoint"]
        oscar_password = outputs["oscar_password"]
        command = "cluster add oscar-%s %s oscar %s" % (resource, oscar_endpoint, oscar_password)
        command = oscarp.executables.oscar_cli.get_command(command)
        get_command_output_wrapped(command)

        #set for mc
        minio_endpoint = outputs["minio_endpoint"]
        minio_password = outputs["minio_password"]
        command = "alias set minio-%s %s minio %s" % (resource, minio_endpoint, minio_password)
        command = oscarp.executables.mc.get_command(command)
        get_command_output_wrapped(command)

    return


def get_outputs(inf_url):
    # inf_url = "https://appsgrycap.i3m.upv.es:31443/im/infrastructures/cfbbef3c-8109-11ed-b108-9620e04d497d"

    tosca_dir = "%s/aisprint/deployments/base/im" % gp.application_dir
    im_auth = "%s/auth.dat" % tosca_dir

    with open(im_auth, 'r') as f:
        auth_data = f.read().replace("\n", "\\n")

    headers = {"Authorization": auth_data, "Accept": "application/json"}

    try:
        resp = requests.get("%s/outputs" % inf_url, headers=headers, verify=True)
        return resp.json()["outputs"]
    except Exception as ex:
        show_fatal_error("Error: %s" % str(ex))

    return None


def update_virtual_infrastructures():
    if gp.is_dry:
        return

    if not gp.virtual_infrastructures:  # if empty return
        return

    if gp.current_base_index == 0:  # if it's the first run everything is already configured
        return

    for resource in gp.virtual_infrastructures.keys():
        tosca_file_path = gp.virtual_infrastructures[resource]["tosca_file_path"]
        inf_id = gp.virtual_infrastructures[resource]["inf_id"]

        # returns True if number of nodes actually changed
        if update_tosca(tosca_file_path, resource, gp.clusters_node_requirements, gp.current_base_index):
            deploy_virtual_infrastructure(tosca_file_path, resource, inf_id)

    wait_for_infrastructure_deployment(is_update=True)


def update_tosca(tosca_file, resource, node_requirements, current_base_run_index):
    with open(tosca_file, 'r') as file:
        content = yaml.safe_load(file)

    old_value = content["topology_template"]["node_templates"]["wn_resource1"]["capabilities"]["scalable"]["properties"]["count"]
    new_value = node_requirements[resource]["nodes"][current_base_run_index]

    if old_value == new_value:
        return False

    content["topology_template"]["node_templates"]["wn_resource1"]["capabilities"]["scalable"]["properties"]["count"] = \
        new_value

    with open(tosca_file, 'w') as file:
        yaml.dump(content, file, sort_keys=False)

    return True


def delete_virtual_infrastructure(inf_url):
    # inf_url = "https://appsgrycap.i3m.upv.es:31443/im/infrastructures/5aad513e-8120-11ed-9ece-ca414e79af22"

    tosca_dir = "%s/aisprint/deployments/base/im" % gp.application_dir
    im_auth = "%s/auth.dat" % tosca_dir

    with open(im_auth, 'r') as f:
        auth_data = f.read().replace("\n", "\\n")

    headers = {"Authorization": auth_data, "Accept": "application/json"}

    try:
        resp = requests.delete(inf_url, headers=headers, verify=True)
        print(resp)
    except Exception as ex:
        show_fatal_error(str(ex))


def delete_unused_virtual_infrastructures():

    resources_in_use = []

    for c in gp.current_deployment:
        c, r = c.split('@')
        resources_in_use.append(r)

    for r in gp.virtual_infrastructures.keys():
        if r not in resources_in_use:
            inf_url = gp.virtual_infrastructures[r]["inf_url"]
            delete_virtual_infrastructure(inf_url)


def get_all_infrastructures():
    im_url = "https://appsgrycap.i3m.upv.es:31443/im/"  # todo change before final (?)

    tosca_dir = "%s/aisprint/deployments/base/im" % gp.application_dir
    im_auth = "%s/auth.dat" % tosca_dir

    with open(im_auth, 'r') as f:
        auth_data = f.read().replace("\n", "\\n")

    headers = {"Authorization": auth_data}

    try:
        resp = requests.request("GET", "%s/infrastructures" % im_url, headers = headers)
        return resp.text.split("\n")
    except Exception as ex:
        print(str(ex))
        return False, str(ex)


def delete_all_virtual_infrastructures():

    infrastructures = get_all_infrastructures()

    if infrastructures == ['']:  # if list is empty
        return
    else:
        print(colored("Deleting old infrastructures...", "yellow"))
        for inf_url in infrastructures:
            delete_virtual_infrastructure(inf_url)


# # # # # # # # # # # #
# Physical infrastructures
# # # # # # # # # # # #

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
def adjust_physical_infrastructures_configuration():

    has_printed_message = False

    for r in gp.clusters_node_requirements:
        cluster = gp.resources[r]

        if cluster["ssh"] is not None:

            if not has_printed_message:
                print(colored("Adjusting physical infrastructures configuration...", "yellow"))
                has_printed_message = True

            client = configure_ssh_client(cluster)
            node_list = get_node_list(client)

            requested_number_of_nodes = gp.clusters_node_requirements[r]["nodes"][gp.current_base_index]

            # show_debug_info(make_debug_info(["Cluster configuration BEFORE:"] + node_list))

            for i in range(1, len(node_list) + 1):
                node = node_list[i - 1]
                if i <= requested_number_of_nodes and node["status"] == "off":
                    get_ssh_output(client, "sudo kubectl uncordon " + node["name"])
                if i > requested_number_of_nodes and node["status"] == "on":
                    get_ssh_output(client, "sudo kubectl cordon " + node["name"])

            # show_debug_info(make_debug_info(["Cluster configuration AFTER:"] + get_node_list(client)))

    if has_printed_message:
        print(colored("Done!", "green"))

    return
