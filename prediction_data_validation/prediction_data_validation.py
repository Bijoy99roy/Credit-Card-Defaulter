# performing important imports
import json
import os
import shutil
import pandas as pd
import numpy as np
from application_logging.logger import AppLogger


class PredictionDataValidation:
    def __init__(self):
        self.logger = AppLogger()
        self.schema = 'Prediction_Schema.json'

    def delete_prediction_files(self):
        """
        Deletes the Prediction_Log directory and it's content
        :return:
        """
        file = open("Prediction_Log/folderHandling.txt", 'a+')
        try:
            self.logger.log(file, 'Entered deletePredictionFiles method of PredictionDataValidation class', 'Info')
            shutil.rmtree('Prediction_Files/')
            self.logger.log(file, 'Prediction_Files deleted.', 'Info')
            file.close()
        except Exception as e:
            self.logger.log(
                file,
                'Error occured in deleting folder in deletePredictionFiles method of \
                PredictionDataValidation class. Message: '+str(e),
                'Error')
            self.logger.log(file,
                            'Failed to delete folder.',
                            'Error')
            file.close()
            raise e

    def create_prediction_files(self, folder_name):
        """
        Creates new directory
        :param folder_name:
        :return:
        """
        file = open("Prediction_Log/folderHandling.txt", 'a+')
        try:
            self.logger.log(file, 'Entered createPredictionFiles method of PredictionDataValidation class', 'Info')

            os.mkdir(f'{folder_name}/')
            self.logger.log(file, 'Prediction_Files created.')
            file.close()
        except Exception as e:
            self.logger.log(file,
                            'Error occured in creating folder in createPredictionFiles method of\
                             PredictionDataValidation class. Message: ' + str(e),
                            'Error')
            self.logger.log(file,
                            'Failed to create folder.',
                            'Error')
            file.close()
            raise e

    def get_schema_values(self):
        """
        Retrives important data from Schema
        :return:
        """
        file = open("Prediction_Log/valuesFromSchemaLog.txt", 'a+')
        try:

            self.logger.log(file, 'Entered getSchemaValue method of PredictionDataValidation class', 'Info')
            with open(self.schema, 'r') as f:
                dic = json.load(f)
                f.close()
            column_names = dic["columnNames"]
            column_number = dic["columnNumber"]
            required_columns = dic["RequiredColumns"]
            numerical_columns = dic["Numerical"]
            output_columns = dic["Output"]

            message = "ColumnNumber: "+str(column_number)+"\t"+"RequiredColumns: "+str(required_columns)+"\n"
            self.logger.log(file, message, 'Info')
            file.close()

        except ValueError as v:
            message = "ValueError:Value not found inside Schema_prediction.json"
            self.logger.log(file, message, 'Error')
            file.close()
            raise v

        except KeyError as k:
            message = "KeyError:key value error incorrect key passed"
            self.logger.log(file, message, 'Error')
            file.close()
            raise k

        except Exception as e:
            self.logger.log(file, str(e), 'Error')
            file.close()
            raise e
        # returning tuple of these 4 values
        return column_number, column_names, required_columns, numerical_columns, output_columns

    def validate_data_type(self):

        file = open("Prediction_Log/ValidationLog.txt", 'a+')
        try:
            self.logger.log(file, 'Entered ValidateDataType method of PredictionDataValidation class', 'Info')
            data = pd.read_csv('Prediction_Files/input.csv')

            for i in data.dtypes:
                if i == np.int64 or i == np.float64:
                    pass
                else:
                    self.logger.log(file, 'Failed valiadtion. Exiting.....', 'Error')
                    raise Exception('Different Datatype found..')
            self.logger.log(
                file,
                'Datatype validation complete exiting ValidateDataType method of PredictionDataValidation class',
                'Info')
        except Exception as e:
            self.logger.log(file, 'Error occured in Validating datatypes. Message: '+str(e), 'Error')
            self.logger.log(file, 'Failed to validate datatype. Exiting.....', 'Error')
            raise e
