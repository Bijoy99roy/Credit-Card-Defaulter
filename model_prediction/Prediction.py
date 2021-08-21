# performing important imports
import pandas as pd
import os
import numpy as np
from application_logging.logger import AppLogger
from prediction_data_validation.prediction_data_validation import PredictionDataValidation
from data_preprocessing.preprocessing import PreProcessing
from file_operation.file_handler import FileHandler


class Prediction:
    def __init__(self):
        self.logger = AppLogger()
        self.file_object = open("Prediction_Log/Prediction_Log.txt", 'a+')
        self.pred_data_val = PredictionDataValidation()

    def predict(self):
        """
        This function applies prediction on the provided data
        :return:
        """
        try:
            self.logger.log(self.file_object, 'Start of Prediction', 'Info')
            # initializing PreProcessor object
            preprocessor = PreProcessing(self.file_object, self.logger)
            # initializing FileHandler object
            model = FileHandler(self.file_object, self.logger)
            # getting the data file path
            file = os.listdir('Prediction_Files/')[0]
            # reading data file
            dataframe = pd.read_csv('Prediction_Files/'+file)
            data = dataframe.copy()
            # receiving values as tuple
            columninfo = self.pred_data_val.get_schema_values()
            numerical_columns = columninfo[3]
            # Scaling the data
            data = preprocessor.scale_data(data, numerical_columns)
            data = np.array(data)

            # loading Logistic Regression model
            support_vector_classifier = model.load_model('supportVectorClassifier')

            # predicting
            predicted = support_vector_classifier.predict(data)
            probablity = support_vector_classifier.predict_proba(data)[0]

            dataframe['predicted'] = ['Defaulter' if i == 0 else 'Not defaulter' for i in predicted]
            dataframe['probablity'] = [round(max(probablity) * 100, 2)]
            print(dataframe)
            dataframe.to_csv('Prediction_Files/Prediction.csv')
            self.logger.log(
                self.file_object,
                'Predction complete!!. Prediction.csv saved in Prediction_File as output. \
                Exiting Predict method of Prediction class ',
                'Info')
            # converting dict array to list
            columninfo = columninfo[4]
            columninfo.append('prediction')
            columninfo.append('probablity')
            return dataframe.to_numpy(), columninfo

        except Exception as e:
            self.logger.log(
                self.file_object,
                'Error occured while running the prediction!! Message: ' + str(e),
                'Error')
            raise e
