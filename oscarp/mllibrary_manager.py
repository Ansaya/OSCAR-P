import os
import configparser

import executables

from termcolor import colored

from utils import auto_mkdir, read_json, write_json

from aMLLibrary import sequence_data_processing
# from aMLLibrary.model_building.predictor import Predictor

import global_parameters as gp


def run_mllibrary():
    print(colored("\nGenerating models...", "blue"))

    # sets directories
    csvs_dir = gp.results_dir + "CSVs"
    # interpolation_csvs_dir = csvs_dir + "/Interpolation/"
    # extrapolation_csvs_dir = csvs_dir + "/Extrapolation/"
    models_dir = gp.results_dir + "Models/"
    auto_mkdir(models_dir)
    # interpolation_models_dir = models_dir + "Interpolation/"
    # extrapolation_models_dir = models_dir + "Extrapolation/"
    # auto_mkdir(interpolation_models_dir)
    # auto_mkdir(extrapolation_models_dir)

    # full training
    train_and_predict(csvs_dir, models_dir, gp.run_name)
    add_to_performance_models_json()

    # interpolation tests
    # train_and_predict(interpolation_csvs_dir, interpolation_models_dir)

    # extrapolation tests
    # train_and_predict(extrapolation_csvs_dir, extrapolation_models_dir)

    print(colored("Done!", "green"))


def train_and_predict(csvs_dir, workdir, run_name):
    """
    this function trains the regressors, both with and without SFS, and then makes the predictions
    :param csvs_dir: points to either CSVs/Interpolation or CSVs/Extrapolation and makes this function reusable
    :param workdir: points to either Models/Interpolation or Models/Extrapolation
    :param run_name:
    """

    # training_set = csvs_dir + "training_set.csv"
    # test_set = csvs_dir + "test_set.csv"
    training_set = csvs_dir + "/runtime_core_" + run_name + ".csv"
    current_model = run_name

    if not gp.has_active_lambdas:
        config_file = os.path.join(gp.application_dir, "oscarp", "aMLLibrary-config.ini")
        output_dir = workdir + current_model + "_model"
        train_models(config_file, training_set, output_dir)

    else:
        # dummy
        config_file = executables.amllibrary_dummy
        output_dir_no_sfs = workdir + current_model + "_model_noSFS"
        train_models(config_file, training_set, output_dir_no_sfs)

    """
    # prediction
    config_file = "aMLLibrary/aMLLibrary-predict.ini"
    set_mllibrary_predict_path(config_file, test_set)
    make_prediction(config_file, output_dir_sfs)
    make_prediction(config_file, output_dir_no_sfs)
    """


def train_models(config_file, filepath, output_dir):
    """
    this function train the four regressors, either with or without SFS depending on the parameters
    :param config_file: path to the configuration file
    :param filepath: path to the test set csv
    :param output_dir: output directory for the four regressors (i.e. "Models/Extrapolation/full_model_noSFS")
    :return:
    """
    set_mllibrary_config_path(config_file, filepath)
    sequence_data_processor = sequence_data_processing.SequenceDataProcessing(config_file, output=output_dir)
    sequence_data_processor.process()


def set_mllibrary_config_path(config_file, filepath):
    """
    this function sets the correct path to the train set in the SFS or noSFS configuration file
    :param config_file: path to the configuration file
    :param filepath: path to the test set csv
    """

    parser = configparser.ConfigParser()
    parser.read(config_file)
    parser.set("DataPreparation", "input_path", filepath)

    with open(config_file, "w") as file:
        parser.write(file)


def set_mllibrary_predict_path(config_file, filepath):
    """
    this function sets the correct path to the test set in the "predict" configuration file
    :param config_file: path to the configuration file
    :param filepath: path to the test set csv
    """

    parser = configparser.ConfigParser()
    parser.read(config_file)
    parser.set("DataPreparation", "input_path", filepath)

    with open(config_file, "w") as file:
        parser.write(file)


def make_prediction(config_file, workdir):
    """
    this functions makes predictions by using the trained models
    :param config_file: points to aMLLibrary-predict.ini
    :param workdir: points to the currently considered model (e.g. Models/Interpolation/full_model_noSFS)
    """

    regressors_list = ["DecisionTree", "RandomForest", "XGBoost"]
    for regressor_name in regressors_list:
        regressor_path = workdir + "/" + regressor_name + ".pickle"
        output_dir = workdir + "/output_predict_" + regressor_name
        predictor_obj = Predictor(regressor_file=regressor_path, output_folder=output_dir, debug=False)
        predictor_obj.predict(config_file=config_file, mape_to_file=True)


def add_to_performance_models_json():
    filepath = gp.application_dir + "oscarp/performance_models.json"
    performance_models = read_json(filepath)

    component_name, resource = gp.run_name.split('@')
    component_name = component_name.replace("C", "component")

    if "P" in component_name:
        component_name = component_name.replace("P", "_partition")
        component_name = component_name.replace(".", "_")
        partition_name = gp.components[component_name]["name"]
        component_name = partition_name.split("_partition")[0]
    else:
        component_name = gp.components[component_name]["name"]
        partition_name = component_name

    model_type = "CoreBasedPredictor"
    model_path = gp.results_dir + "Models/" + gp.run_name + "_model/best.pickle"

    if component_name not in performance_models.keys():
        performance_models[component_name] = {}

    if partition_name not in performance_models[component_name].keys():
        performance_models[component_name][partition_name] = {}

    performance_models[component_name][partition_name][resource] = {
        "model": model_type,
        "regressor_file": model_path
    }

    write_json(filepath, performance_models)


