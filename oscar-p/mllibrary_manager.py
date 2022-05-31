import os
import configparser

from termcolor import colored

from utils import auto_mkdir

from MLlibrary import sequence_data_processing
from MLlibrary.model_building.predictor import Predictor


def run_mllibrary(results_dir):
    """
    coordinator function for training and testing the models
    :param results_dir: path to the "Results" folder of the considered campaign
    """

    print(colored("\nGenerating models...", "blue"))

    # sets directories
    csvs_dir = results_dir + "/CSVs"
    interpolation_csvs_dir = csvs_dir + "/Interpolation/"
    extrapolation_csvs_dir = csvs_dir + "/Extrapolation/"
    models_dir = results_dir + "/Models/"
    auto_mkdir(models_dir)
    interpolation_models_dir = models_dir + "Interpolation/"
    extrapolation_models_dir = models_dir + "Extrapolation/"
    auto_mkdir(interpolation_models_dir)
    auto_mkdir(extrapolation_models_dir)

    # interpolation tests
    train_and_predict(interpolation_csvs_dir, interpolation_models_dir)

    # extrapolation tests
    train_and_predict(extrapolation_csvs_dir, extrapolation_models_dir)

    print(colored("Done!", "green"))


def train_and_predict(csvs_dir, workdir):
    """
    this function trains the regressors, both with and without SFS, and then makes the predictions
    :param csvs_dir: points to either CSVs/Interpolation or CSVs/Extrapolation and makes this function reusable
    :param workdir: points to either Models/Interpolation or Models/Extrapolation
    """

    results = os.listdir(csvs_dir)
    for r in results:
        if "training" in r:
            training_set = csvs_dir + r
            test_set = training_set.replace("training", "test")
            current_model = r.split('.')[0].split('_')[-1]

            # with SFS
            config_file = "MLlibrary/MLlibrary-config-SFS.ini"
            output_dir_sfs = workdir + current_model + "_model_SFS"
            train_models(config_file, training_set, output_dir_sfs)

            # without SFS
            config_file = "MLlibrary/MLlibrary-config-noSFS.ini"
            output_dir_no_sfs = workdir + current_model + "_model_noSFS"
            train_models(config_file, training_set, output_dir_no_sfs)

            # prediction
            config_file = "MLlibrary/MLlibrary-predict.ini"
            set_mllibrary_predict_path(config_file, test_set)
            make_prediction(config_file, output_dir_sfs)
            make_prediction(config_file, output_dir_no_sfs)


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
    :param config_file: points to MLlibrary-predict.ini
    :param workdir: points to the currently considered model (e.g. Models/Interpolation/full_model_noSFS)
    """

    regressors_list = ["DecisionTree", "LRRidge", "RandomForest", "XGBoost"]
    for regressor_name in regressors_list:
        regressor_path = workdir + "/" + regressor_name + ".pickle"
        output_dir = workdir + "/output_predict_" + regressor_name
        predictor_obj = Predictor(regressor_file=regressor_path, output_folder=output_dir, debug=False)
        predictor_obj.predict(config_file=config_file, mape_to_file=True)
