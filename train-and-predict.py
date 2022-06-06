import os

from aMLLibrary import sequence_data_processing
from aMLLibrary.model_building.predictor import Predictor


def auto_mkdir(new_dir):
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)


def train_models(config_file, output_dir):
    sequence_data_processor = sequence_data_processing.SequenceDataProcessing(config_file, output=output_dir)
    sequence_data_processor.process()
    
    
def make_predictions(config_file, workdir):
    
    regressors_list = ["DecisionTree", "LRRidge", "RandomForest", "XGBoost"]
    
    for regressor_name in regressors_list:
        print(regressor_name)
        regressor_path = workdir + "/" + regressor_name + ".pickle"
    
        output_dir = workdir + "/output_predict_" + regressor_name
        
        predictor_obj = Predictor(regressor_file=regressor_path, output_folder=output_dir, debug=False)
        predictor_obj.predict(config_file=config_file, mape_to_file=True)
        

config_file_SFS = "new-results/Test-1/MLlibrary-config-SFS.ini"
config_file_noSFS = "new-results/Test-1/MLlibrary-config-noSFS.ini"
results_dir = "new-results/Test-1/Results/"
auto_mkdir(results_dir)
output_dir_SFS = "new-results/Test-1/Results/SFS"
auto_mkdir(output_dir_SFS)
output_dir_noSFS = "new-results/Test-1/Results/noSFS"
auto_mkdir(output_dir_noSFS)

train_models(config_file_SFS, output_dir_SFS)
train_models(config_file_noSFS, output_dir_noSFS)

make_predictions("new-results/Test-1/MLlibrary-predict.ini", output_dir_noSFS)
make_predictions("new-results/Test-1/MLlibrary-predict.ini", output_dir_SFS)
