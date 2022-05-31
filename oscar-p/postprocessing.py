# processing that take place after the run are done

import math
import pickle
import os

import plotly.express as px
import pandas as pd
import numpy as np

from input_file_processing import get_interpolation_values, get_extrapolation_values
from utils import show_error, list_of_strings_to_file, auto_mkdir, csv_to_list_of_dict


def get_first_job(timelist):
    """
    returns the time of the creation of the first job in a given timelist
    :param timelist: dictionary containing a list of jobs, each job itself a dictionary containing the following info:
            service, node, job_create, pod_create, job_start, bash_script_start, bash_script_end, job_finish;
            timelists are created by function "get_timed_jobs_list" in file "retrieve_logs.py"
    :return: the time of the creation (job_create) of the first job as datetime
    """

    start_list = []

    for k in timelist.keys():
        start_list.append(timelist[k]["job_create"])

    first = start_list[0]

    for s in start_list:
        if s < first:
            first = s
    return first


def get_last_job(timelist):
    """
    returns the time of the completion of the last job in a given timelist
    :param timelist: dictionary containing a list of jobs, each job itself a dictionary containing the following info:
            service, node, job_create, pod_create, job_start, bash_script_start, bash_script_end, job_finish;
            timelists are created by function "get_timed_jobs_list" in file "retrieve_logs.py"
    :return: the time of the completion (job_finish) of the last job as datetime
    """

    finish_list = []

    for k in timelist.keys():
        finish_list.append(timelist[k]["job_finish"])

    last = finish_list[0]

    for f in finish_list:
        if f > last:
            last = f
    return last


def calculate_runtime(working_dir, services):
    """
    given the directory of a specific run (i.e. "Run #1/") and the list of services, it returns the runtime of that run;
    the list of services is ordered, so it doesn't need too many checks
    :param working_dir: directory of a specific run (i.e. "Run #1/")
    :param services: ordered list of services, each a dictionary containing name of the service (as string),
            input bucket (as string) and output buckets (list of strings)
    :return: runtime of the run in seconds
    """

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


def calculate_average(values):
    """
    receives a list of values, returns the average
    :param values: list of values
    :return: average
    """

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
# todo for now it considers only cores
def calculate_maximum_parallelism(run):
    from input_file_processing import get_worker_nodes_info
    _, max_cores, max_memory = get_worker_nodes_info()

    smallest_core = 9999

    for s in run["services"]:
        service_cores = float(s["cpu"])
        # the smaller the number of cores used by service, the greater the parallelism level
        if service_cores < smallest_core:
            smallest_core = service_cores

    return calculate_parallelism(smallest_core, max_cores, 0, 0, run["nodes"])


def prepare_runtime_data(campaign_name, subfolder, repetitions, runs, services):
    """
    outputs two runtime/parallelism dataframes:
    * the first contains every run (including repetitions)
    * the second one instead averages the repeated runs
    :return: two runtime/parallelism dataframes, complete and averaged
    """

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

    # this one is both for the CSV and for the graph
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

    # dataframe
    df = pd.DataFrame(data=data)
    df.sort_values(by=['parallelism'], inplace=True)

    # averaged_dataframe
    adf = pd.DataFrame(data=averaged_data)
    adf.sort_values(by=['parallelism'], inplace=True)

    return df, adf


def plot_runtime_core_graphs(results_dir, subfolder, df, adf):
    """
    plots a graph with all the runs and a line for the average
    :param results_dir: path to the "Results" folder of the considered campaign
    :param subfolder: specifies whether we're considering the full workflow ("full") or a service
    :param df: dataframe containing the runtime/parallelism data for every run (including repetitions)
    :param adf: dataframe containing the runtime/parallelism data for every run (repeated runs are averaged)
    """

    graphs_dir = results_dir + "/Graphs/"
    auto_mkdir(graphs_dir)
    title = subfolder
    if subfolder == "full":
        title = "Full workflow"
    fig = px.scatter(df, x="parallelism", y="runtime", color="runs", title=title, template="simple_white",
                     labels={"parallelism": "Parallelism", "runtime": "Runtime (seconds)", "runs": "Runs"})
    fig.add_scatter(x=adf["parallelism"], y=adf["runtime"], name="Average", mode="lines")
    fig.write_image(graphs_dir + "runtime_core_" + subfolder + ".png")


def make_runtime_core_csv(results_dir, subfolder, df):
    """
    writes the dataframe as CSV that can be fed to the ML library
    :param results_dir: path to the "Results" folder of the considered campaign
    :param subfolder: specifies whether we're considering the full workflow ("full") or a service
    :param df: dataframe containing the runtime/parallelism data for every run (including repetitions)
    """

    csvs_dir = results_dir + "/CSVs/"
    auto_mkdir(csvs_dir)
    filename = csvs_dir + "runtime_core_" + subfolder + ".csv"
    header = "runtime,cores,log(cores)\n"
    with open(filename, "w") as file:
        file.write(header)
        for i in range(len(df)):
            core = df["parallelism"][i]
            log_core = round(math.log10(int(core)), 5)
            runtime = df["runtime"][i]
            file.write(str(runtime) + "," + str(core) + "," + str(log_core) + "\n")


def plot_ml_predictions_graphs(results_dir, services):
    """
    collects the required data needed to plot the ml graphs, such as test values, dataframes and predictions
    :param results_dir: path to the "Results" folder of the considered campaign
    :param services: ordered list of services, each a dictionary containing name of the service (as string),
            input bucket (as string) and output buckets (list of strings)
    """
    graphs_dir = results_dir + "/Graphs/"

    # first let's make a list of the "subfolders" we'll need to process
    subfolders_list = ["full"]

    for s in services:
        subfolders_list.append(s["name"])

    interpolation_test_values = get_interpolation_values()
    extrapolation_test_values = get_extrapolation_values()

    for s in subfolders_list:
        # load the required dataframes
        df, adf = load_dataframes(results_dir, s)

        # gets required info for current subfolder
        interpolation, extrapolation = find_best_predictions(results_dir, s)

        # plots interpolation and extrapolation graphs
        plot_ml_predictions_graph(graphs_dir, s, interpolation_test_values, interpolation, df, adf, "interpolation")
        plot_ml_predictions_graph(graphs_dir, s, extrapolation_test_values, extrapolation, df, adf, "extrapolation")


def plot_ml_predictions_graph(graphs_dir, subfolder, test_values, dictionary, df, adf, operation):
    """
    plots a graph with all the runs, a line for the average and red marks for the predictions
    :param graphs_dir: directory containing the graphs of the considered campaign (i.e. "Results/Graphs")
    :param subfolder: specifies whether we're considering the full workflow ("full") or a service
    :param test_values: array with the "parallelism" values used to make the predictions on
    :param dictionary: contains info on the prediction, such as the best model, its mape, and the predicted values
    :param df: dataframe containing the runtime/parallelism data for every run (including repetitions)
    :param adf: dataframe containing the runtime/parallelism data for every run (repeated runs are averaged)
    :param operation: string, is either "interpolation" or "extrapolation", only needed to save the image
    """

    if subfolder == "full":
        title = "Full workflow - " + dictionary["best_model"] + " - MAPE " + dictionary["mape"]
    else:
        title = subfolder + " - " + dictionary["best_model"] + " - MAPE " + dictionary["mape"]

    fig = px.scatter(df, x="parallelism", y="runtime", color="runs", title=title, template="simple_white",
                     labels={"parallelism": "Parallelism", "runtime": "Runtime (seconds)", "runs": "Runs"})

    fig.add_scatter(x=adf["parallelism"], y=adf["runtime"], name="Average", mode="lines")

    fig.add_scatter(x=test_values, y=dictionary["values"], name="Prediction", mode="markers",
                    marker=dict(size=10, symbol="x", color="red"))

    fig.write_image(graphs_dir + "ml_" + operation + "_" + subfolder + ".png")


def load_dataframes(results_dir, subfolder):
    dataframe_dir = results_dir + "/Dataframes/"
    df = pd.read_pickle(dataframe_dir + subfolder + "_data.pickle")
    adf = pd.read_pickle(dataframe_dir + subfolder + "_average_data.pickle")
    return df, adf


def find_best_predictions(results_dir, subfolder):
    # dir definition
    models_dir = results_dir + "/Models/"
    summaries_dir = results_dir + "/Summaries/"
    auto_mkdir(summaries_dir)
    interpolation_dir = models_dir + "Interpolation/"
    extrapolation_dir = models_dir + "Extrapolation/"
    sfs_model = subfolder + "_model_SFS/"
    no_sfs_model = subfolder + "_model_noSFS/"

    # interpolation
    workdir = [interpolation_dir + sfs_model, interpolation_dir + no_sfs_model]
    interpolation = find_best_prediction(workdir, summaries_dir + subfolder + "_interpolation.txt")

    # extrapolation
    workdir = [extrapolation_dir + sfs_model, extrapolation_dir + no_sfs_model]
    extrapolation = find_best_prediction(workdir, summaries_dir + subfolder + "_extrapolation.txt")

    return interpolation, extrapolation


def find_best_prediction(workdir, summary_path):
    best_model_path, min_mape, summary = find_best_model(workdir)

    list_of_strings_to_file(summary, summary_path)

    prediction_csv = pd.read_csv(best_model_path + "/prediction.csv")
    prediction_values = []
    for p in prediction_csv["pred"]:
        prediction_values.append(round(p, 2))

    best_model = best_model_path.split("_")[-1]
    if "noSFS" in best_model_path:
        best_model += " (no SFS)"
    else:
        best_model += " (SFS)"

    dictionary = {
        "best_model": best_model,
        "mape": str(min_mape) + " %",
        "values": prediction_values,
    }

    return dictionary


def find_best_model(workdir):
    min_mape = 10
    best_model = ""

    summary = ["noSFS"]

    # the two workdir are of course the models obtained with SFS and the ones without
    for w in workdir:
        results = os.listdir(w)
        for r in results:
            if "output_predict" in r:
                with open(w + r + "/mape.txt", "r") as file:
                    mape = float(file.readline())
                    mape_h = round(mape * 100, 2)  # human readable
                    summary.append("\t" + r.split("_")[2] + " " + str(mape_h) + " %")
                    # if current model is better updates values
                    if mape < min_mape:
                        min_mape = mape
                        best_model = w + r
        summary.append("SFS")
    return best_model, round(min_mape * 100, 2), summary[:-1]


def make_runtime_core_csv_for_ml(results_dir, subfolder, data, averaged_data, operation):
    """
    generates the training and test set for the interpolation (or extrapolation) tests of a campaign
    :param results_dir: path to the "Results" folder of the considered campaign
    :param subfolder: specifies whether we're considering the full workflow ("full") or a service
    :param data: complete dataframe with all values obtained in the campaign, used for the training set
    :param averaged_data: reduced dataframe with averaged values, used for the test set
    :param operation: specifies whether we are performing Interpolation or Extrapolation
    """

    csvs_dir = results_dir + "/CSVs/"
    workdir = csvs_dir + operation + "/"

    auto_mkdir(workdir)

    filename_training = workdir + "training_set_" + subfolder + ".csv"
    filename_test = workdir + "test_set_" + subfolder + ".csv"
    header = "runtime,cores,log(cores)\n"

    if operation == "Interpolation":
        test_set_values = get_interpolation_values()
    elif operation == "Extrapolation":
        test_set_values = get_extrapolation_values()
    else:
        show_error("Invalid operation specified, exiting")
        quit()

    # makes training set file
    with open(filename_training, "w") as file:
        file.write(header)
        for i in range(len(data)):
            core = data["parallelism"][i]
            log_core = round(math.log10(int(core)), 5)
            runtime = data["runtime"][i]
            if core not in test_set_values:
                file.write(str(runtime) + "," + str(core) + "," + str(log_core) + "\n")

    # makes test set file
    with open(filename_test, "w") as file:
        file.write(header)
        for i in range(len(averaged_data)):
            core = averaged_data["parallelism"][i]
            log_core = round(math.log10(int(core)), 5)
            runtime = averaged_data["runtime"][i]
            if core in test_set_values:
                file.write(str(runtime) + "," + str(core) + "," + str(log_core) + "\n")


def save_dataframes(results_dir, subfolder, df, adf):
    """
    saves the two dataframes to file so that they can be reused (when making the prediction graphs for example)
    :param results_dir: path to the "Results" folder of the considered campaign
    :param subfolder: specifies whether we're considering the full workflow ("full") or a service
    :param df: dataframe containing the runtime/parallelism data for every run (including repetitions)
    :param adf: dataframe containing the runtime/parallelism data for every run (repeated runs are averaged)
    """

    dataframe_dir = results_dir + "/Dataframes/"

    auto_mkdir(dataframe_dir)

    df.to_pickle(dataframe_dir + subfolder + "_data.pickle")
    adf.to_pickle(dataframe_dir + subfolder + "_average_data.pickle")


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


# todo modify this method and use it
# will recieve directory, subfolder and list of services
def make_statistics(data):
	
	campaign_dir = "runs-results/10-seconds-vm"
	subfolder = "full"
	# service = "blur-faces"
	service = "mask-detector"
	
	
	# print(os.listdir(campaign_dir))
	
	data = {}
	
	for run in os.listdir(campaign_dir):
		if "Run" in run:
			filepath = os.path.join(campaign_dir, run, subfolder, service + "_jobs.csv")
			for row in csv_to_list_of_dict(filepath):
				data[row["job_name"]]= {
							"wait": float(row["wait"]),
							"pod_creation": float(row["pod_creation"]),
							"overhead": float(row["overhead"]),
							"compute_time": float(row["compute_time"]),
							"write_back": float(row["write_back"])
							}
	
	n = len(data)
	data_matrix = np.zeros((n, 5))
	
	keys = list(data.keys())
	
	
	for i in range(n):
		
		job = data[keys[i]]
		
		data_matrix[i, 0] += job["wait"]
		data_matrix[i, 1] += job["pod_creation"]
		data_matrix[i, 2] += job["overhead"]
		data_matrix[i, 3] += job["compute_time"]
		data_matrix[i, 4] += job["write_back"]
		
	print(data_matrix[0:10])
	total = np.sum(data_matrix, axis=0)
	print(total)
	mean = total / n
	mean += 0.0000000001
	print(mean)
	variance = np.square(data_matrix - mean)
	variance = np.sum(variance, axis=0) / n
	print(variance)
	sigma = np.sqrt(variance)  # population standard deviation
	coeff_of_variation = sigma / mean
	print(coeff_of_variation)
	
	return
