import os
import configparser

from termcolor import colored

from MLlibrary import sequence_data_processing
from MLlibrary.model_building.predictor import Predictor


def run_mllibrary(results_dir):
    print(colored("\nGenerating models...", "blue"))
    csvs_dir = results_dir + "/CSVs"
    interpolation_csvs_dir = csvs_dir + "/Interpolation/"
    extrapolation_csvs_dir = csvs_dir + "/Extrapolation/"
    models_dir = results_dir + "/Models/"
    os.mkdir(models_dir)
    interpolation_models_dir = models_dir + "Interpolation/"
    extrapolation_models_dir = models_dir + "Extrapolation/"
    os.mkdir(interpolation_models_dir)
    os.mkdir(extrapolation_models_dir)

    # interpolation tests
    train_and_predict(interpolation_csvs_dir, interpolation_models_dir)

    # extrapolation tests
    train_and_predict(extrapolation_csvs_dir, extrapolation_models_dir)
    print(colored("Done!", "green"))


# this function train the regressors, both with and without SFS, and then makes the predictions
# csvs_dir points to either CSVs/Interpolation or CSVs/Extrapolation and makes this function reusable
# workdir instead points to either Models/Interpolation or Models/Extrapolation
def train_and_predict(csvs_dir, workdir):
    results = os.listdir(csvs_dir)
    for r in results:
        if ".csv" in r and "training" in r and "full" in r:  # todo temporary, remove last condition
            training_set = csvs_dir + r
            test_set = training_set.replace("training", "test")
            current_model = r.strip(".csv").strip("training_set")

            # with SFS
            config_file = "MLlibrary/MLlibrary-config-SFS.ini"
            output_dir = workdir + current_model + "_model_SFS"
            # train_models(config_file, training_set, output_dir)

            # without SFS
            config_file = "MLlibrary/MLlibrary-config-noSFS.ini"
            output_dir = workdir + current_model + "_model_noSFS"
            train_models(config_file, training_set, output_dir)

            # prediction
            config_file = "MLlibrary/MLlibrary-predict.ini"
            set_mllibrary_predict_path(config_file, test_set)
            make_prediction(config_file, output_dir)


def train_models(config_file, filepath, output_dir):
    set_mllibrary_config_path(config_file, filepath)
    sequence_data_processor = sequence_data_processing.SequenceDataProcessing(config_file, output=output_dir)
    sequence_data_processor.process()


def set_mllibrary_config_path(config_file, filepath):
    parser = configparser.ConfigParser()
    parser.read(config_file)
    parser.set("DataPreparation", "input_path", filepath)

    with open(config_file, "w") as file:
        parser.write(file)


def set_mllibrary_predict_path(config_file, filepath):
    parser = configparser.ConfigParser()
    parser.read(config_file)
    parser.set("DataPreparation", "input_path", filepath)

    with open(config_file, "w") as file:
        parser.write(file)


# this functions makes predictions by used the trained models
# config_file points to MLlibrary-predict.ini
# workdir instead points to the currently considered model (eg. Models/Interpolation/full_model_noSFS)
def make_prediction(config_file, workdir):
    regressors_list = ["DecisionTree", "LRRidge", "RandomForest", "XGBoost"]
    for regressor_name in regressors_list:
        regressor_path = workdir + "/" + regressor_name + ".pickle"
        output_dir = workdir + "/output_predict_" + regressor_name
        predictor_obj = Predictor(regressor_file=regressor_path, output_folder=output_dir, debug=False)
        predictor_obj.predict(config_file=config_file, mape_to_file=True)
