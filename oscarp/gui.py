# this file includes functions that print out content
import os.path

from termcolor import colored

from input_file_processing import get_debug, get_closest_parallelism_level
from utils import append_string_to_file, strip_ansi_from_string, create_new_file

import global_parameters as gp


# given two runs, it shows the difference in cpu and memory settings
def runs_diff_services(run1, run2):

    print("\t\tServices:")
    append_to_deployment_summary("\t\tServices:")
    for i in range(len(run1["services"])):
        s1 = run1["services"][i]
        s2 = run2["services"][i]
        service_name = "{:<35}".format(s1["name"])

        output = ""
        if s1["parallelism"] != s2["parallelism"]:
            output += "parallelism: " + colored(s1["parallelism"], "green") + " -> " + colored(s2["parallelism"], "green")

        if output != "":
            show_and_save_to_summary("\t\t\t" + colored(service_name, "blue") + output)
        else:
            show_and_save_to_summary("\t\t\t" + colored(service_name, "blue") + colored("unchanged", "green"))


def runs_diff_clusters(clusters, index):

    show_and_save_to_summary("\t\tClusters:")
    for cluster_name in clusters:
        if cluster_name != "AWS Lambda":
            cluster = clusters[cluster_name]

            old_nodes = cluster["nodes"][index - 1]
            new_nodes = cluster["nodes"][index]
            cluster_name = "{:<20}".format(cluster_name)

            output = ""
            if old_nodes != new_nodes:
                output += "nodes: " + colored(old_nodes, "green") + " -> " + colored(new_nodes, "green")

            if output != "":
                show_and_save_to_summary("\t\t\t" + colored(cluster_name, "blue") + output)
            else:
                show_and_save_to_summary("\t\t\t" + colored(cluster_name, "blue") + colored("unchanged", "green"))

    return


def show_all_services(run):
    show_and_save_to_summary("\t" + run["id"])
    show_and_save_to_summary("\t\tServices:")

    for s in run["services"]:
        service_name = "{:<35}".format(s["name"])
        show_and_save_to_summary("\t\t\t" + colored(service_name, "blue")
              + "cpu: " + colored(s["cpu"], "green")
              + " , memory: " + colored(s["memory"], "green") + " mb"
              + " , parallelism: " + colored(s["parallelism"], "green")
              + " , cluster: " + colored(s["cluster"], "green"))

    return


def show_all_clusters(clusters, index):
    show_and_save_to_summary("\t\tClusters:")

    for cluster_name in clusters:
        if cluster_name != "AWS Lambda":
            cluster = clusters[cluster_name]

            nodes = cluster["nodes"][0]
            cluster_name = "{:<20}".format(cluster_name)
            show_and_save_to_summary("\t\t\t" + colored(cluster_name, "blue") + "nodes: " + colored(str(nodes), "green"))

    return


def show_runs(base, repetitions, clusters):
    summary_filepath = os.path.join(gp.current_deployment_dir, "campaign_summary.txt")
    create_new_file(summary_filepath)
    show_and_save_to_summary("\nScheduler:")
    show_all_services(base[0])

    show_all_clusters(clusters, 0)
    show_and_save_to_summary("")

    for i in range(1, len(base)):
        show_and_save_to_summary("\t" + base[i]["id"])
        runs_diff_services(base[i-1], base[i])
        runs_diff_clusters(clusters, i)
        show_and_save_to_summary("")

    show_and_save_to_summary("\n\tRepeated " + colored(str(repetitions), "green") + " time(s), "
          + str(len(base) * repetitions) + " runs in total")

    # todo re-enable this after testing!
    # value = get_valid_input("Do you want to proceed? (y/n)\t", ["y", "n"])
    print()  # just for spacing
    value = "y"

    if value == "n":
        print(colored("Exiting...", "red"))
        quit()


def show_and_save_to_summary(string):
    print(string)
    append_to_deployment_summary(strip_ansi_from_string(string))


def append_to_deployment_summary(string):
    summary_filepath = os.path.join(gp.current_deployment_dir, "campaign_summary.txt")
    append_string_to_file(string, summary_filepath)
