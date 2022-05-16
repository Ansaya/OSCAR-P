# processing that take place after the run are done

import math
import pickle
import os

import plotly.express as px
import pandas as pd


# returns the time of the creation of the first job in a given timelist
def get_first_job(timelist):
    start_list = []

    for k in timelist.keys():
        start_list.append(timelist[k]["job_create"])

    first = start_list[0]

    for s in start_list:
        if s < first:
            first = s
    return first


# returns the time of the completion of the last job in a given timelist
def get_last_job(timelist):
    finish_list = []

    for k in timelist.keys():
        finish_list.append(timelist[k]["job_finish"])

    last = finish_list[0]

    for f in finish_list:
        if f > last:
            last = f
    return last


# given the directory of a specific run and the list of services, it returns the runtime of that run
# the list of services is ordered hence not needing to many checks
def calculate_runtime(working_dir, services):

    first, last = 0, 0
    for service in services:
        service_name = service["name"]
        with open(working_dir + "/time_table_" + service_name + ".pkl", "rb") as file:
            timelist = pickle.load(file)

        if len(timelist) != 0:
            if first == 0:
                first = get_first_job(timelist)
                last = get_last_job(timelist)
            else:
                last = get_last_job(timelist)

    runtime = (last - first).total_seconds()
    return runtime


# receives a list of values, returns the average
def calculate_average(values):
    average = 0
    for v in values:
        average += v

    return average / len(values)


# todo for now considers only cores
def calculate_parallelism(cores, max_cores, memory, max_memory, nodes):
    cores = round(cores)
    parallelism = (max_cores / cores) * nodes
    return parallelism


# by checking the resources of every service in a run, it returns the parallelism of the "most parallel" service
# todo for now considers only cores
def calculate_maximum_parallelism(run):
    from input_file_processing import get_worker_nodes_info
    _, max_cores, max_memory = get_worker_nodes_info()

    smallest_core = 9999

    for s in run["services"]:
        service_cores = float(s["cpu"])
        if service_cores < smallest_core:  # the smaller the number of cores used by service, the greater the parallelism
            smallest_core = service_cores

    return calculate_parallelism(smallest_core, max_cores, 0, 0, run["nodes"])


# todo explain
def prepare_runtime_data(campaign_name, subfolder, repetitions, runs, services):

    runtimes = []
    parallelism = []
    run = []

    averaged_runtimes = []
    averaged_parallelism = []

    base_length = int(len(runs) / repetitions)

    for i in range(base_length):
        runtimes_to_average = []
        for j in range(repetitions):
            working_dir = os.path.join(campaign_name, runs[i+(j*base_length)]["id"], subfolder)
            runtime = calculate_runtime(working_dir, services)
            runtimes_to_average.append(runtime)
            runtimes.append(runtime)
            parallelism.append(calculate_maximum_parallelism(runs[i]))
            run.append("Run #" + str(j+1))
        average_runtime = calculate_average(runtimes_to_average)
        average_runtime = round(average_runtime, 3)

        averaged_runtimes.append(average_runtime)
        averaged_parallelism.append(calculate_maximum_parallelism(runs[i]))

    # this one is for the CSV
    data = {
        "runtime": runtimes,
        "parallelism": parallelism,
        "runs": run
    }

    # this other one is for the graph
    averaged_data = {
        "runtime": averaged_runtimes,
        "parallelism": averaged_parallelism
    }

    df = pd.DataFrame(data=data)
    df.sort_values(by=['parallelism'], inplace=True)

    adf = pd.DataFrame(data=averaged_data)
    adf.sort_values(by=['parallelism'], inplace=True)

    return df, adf


def plot_runtime_core_graph(campaign_name, subfolder, data, averaged_data):
    df = pd.DataFrame(data)
    fig = px.scatter(df, x="parallelism", y="runtime", color="runs", title=campaign_name + "_" + subfolder, labels={"x": "Cores", "y": "Runtime (seconds)"})
    fig.add_scatter(x=averaged_data["parallelism"], y=averaged_data["runtime"], name="Average", mode="lines")
    fig.write_image(campaign_name + "/Results/" + "runtime_core_" + subfolder + ".png")


def make_runtime_core_csv(campaign_name, subfolder, data):
    filename = campaign_name + "/Results/" + "runtime_core_" + subfolder + ".csv"
    header = "runtime,cores,log(cores),number_of_videos\n"
    with open(filename, "w") as file:
        file.write(header)
        for i in range(len(data)):
            core = data["parallelism"][i]
            log_core = round(math.log10(int(core)), 5)
            runtime = data["runtime"][i]
            file.write(str(runtime) + "," + str(core) + "," + str(log_core) + "," + "10" + "\n")
    # print("Done!")
    return


def read_csv_header(filepath):
    with open(filepath, "r") as file:
        lines = file.readlines()
    return lines[0]


def read_csv_content(filepath):
    with open(filepath, "r") as file:
        lines = file.readlines()
    return lines[1:]


def merge_csv_of_service(campaign_name, service_name):
    runs = []
    for x in os.listdir(campaign_name):
        if "Run #" in x:
            runs.append(x)

    filename = service_name + "_jobs.csv"
    header = read_csv_header(os.path.join(campaign_name, runs[0], service_name, filename))

    merged_csv = [header]
    for run in runs:
        merged_csv += read_csv_content(os.path.join(campaign_name, run, service_name, filename))

    filename = "Results/merged_" + service_name + ".csv"
    filepath = os.path.join(campaign_name, filename)

    with open(filepath, "w") as file:
        for line in merged_csv:
            file.write(line)

    return

