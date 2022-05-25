import os
import configparser

from termcolor import colored

from MLlibrary import sequence_data_processing
from MLlibrary.model_building.predictor import Predictor


def run_mllibrary(campaign_name):
    print(colored("\nGenerating models...", "blue"))
    base_dir = campaign_name + "/CSVs/"
    campaign_name += "/Models/"
    os.mkdir(campaign_name)

    results = os.listdir(base_dir)
    for r in results:
        if ".csv" in r and "runtime" in r and "full" in r:  # todo temporary, remove last condition
            filepath = base_dir + r

            # with SFS
            config_file = "MLlibrary/MLlibrary-config-SFS.ini"
            output_dir = campaign_name + r.strip(".csv") + "_model_SFS"
            train_models(config_file, filepath, output_dir)

            # without SFS
            config_file = "MLlibrary/MLlibrary-config-noSFS.ini"
            output_dir = base_dir + r.strip(".csv") + "_model_noSFS"
            # train_models(config_file, filepath, output_dir)

            # prediction
            config_file = "MLlibrary/MLlibrary-predict.ini"
            # set_mllibrary_predict_path(config_file, filepath)
            # make_prediction(config_file, output_dir)
    print(colored("Done!", "green"))


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


def set_values_to_interpolate(config_file):
    parser = configparser.ConfigParser()
    parser.read(config_file)
    parser.set("General", "interpolation_columns", "{\"cores\": [8]}")

    with open(config_file, "w") as file:
        parser.write(file)


def set_values_to_extrapolate(config_file):
    parser = configparser.ConfigParser()
    parser.read(config_file)
    parser.set("General", "extrapolation_columns", "{\"cores\": 27.0}")

    with open(config_file, "w") as file:
        parser.write(file)


def set_mllibrary_predict_path(config_file, filepath):
    parser = configparser.ConfigParser()
    parser.read(config_file)
    parser.set("DataPreparation", "input_path", filepath)

    with open(config_file, "w") as file:
        parser.write(file)


def make_prediction(config_file, model_dir):
    regressor = "XGBoost"
    regressor = model_dir + "/" + regressor + "_training" + ".pickle"
    output_dir = "output_predict" + "_XGBoost"
    predictor_obj = Predictor(regressor_file=regressor, output_folder=output_dir, debug=False)
    predictor_obj.predict(config_file=config_file, mape_to_file=True)
    return



