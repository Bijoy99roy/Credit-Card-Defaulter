#performing important imports
from application_logging.logger import App_Logger
import json
import os
import shutil
import pandas as pd

class PredictionDataValidation:
    def __init__(self):
        self.logger = App_Logger()
        self.schema = 'Prediction_Schema.json'

    def deletePredictionFiles(self):
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
            self.logger.log(file, 'Error occured in deleting folder in deletePredictionFiles method of PredictionDataValidation class. Message: '+str(e), 'Error')
            self.logger.log(file,
                            'Failed to delete folder.', 'Error')
            file.close()
            raise e

    def createPredictionFiles(self, folderName):
        """
        Creates new directory
        :param folderName:
        :return:
        """
        file = open("Prediction_Log/folderHandling.txt", 'a+')
        try:
            self.logger.log(file, 'Entered createPredictionFiles method of PredictionDataValidation class','Info')

            os.mkdir(f'{folderName}/')
            self.logger.log(file, 'Prediction_Files created.')
            file.close()
        except Exception as e:
            self.logger.log(file,
                            'Error occured in creating folder in createPredictionFiles method of PredictionDataValidation class. Message: ' + str(
                                e), 'Error')
            self.logger.log(file,
                            'Failed to create folder.', 'Error')
            file.close()
            raise e
    def getSchemaValues(self):
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
            columnNames = dic["columnNames"]
            columnNumber = dic["columnNumber"]
            requiredColumns = dic["RequiredColumns"]
            numericalColumns = dic["Numerical"]


            message = "ColumnNumber: "+str(columnNumber)+"\t"+"RequiredColumns: "+str(requiredColumns)+"\n"
            self.logger.log(file,message, 'Info')

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
            raise  e
        #returning tuple of these 3 values
        return columnNumber, columnNames, requiredColumns, numericalColumns

    '''
    def validateColumnLength(self, columnNumber):
        """
        This function validates the number of columns in the provided data
        :param columnNumber:
        :return:
        """
        try:
            f = open("Prediction_Log/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Column Length Validation Started!!", 'Info')
            for file in os.listdir('Prediction_Files/'):
                csv = pd.read_csv('Prediction_Files/'+file)
                if csv.shape[1] == columnNumber:
                    pass
                else:
                    self.logger.log(f, "Invalid column length for the file!! Exiting...", 'Error')
                    raise Exception(f"Invalid column length for the file!!. Column should be {columnNumber}, {csv.shape[1]} found")
            self.logger.log(f, "Columns length validation complete!!")
        except Exception as e:
            self.logger.log(f, str(e), 'Error')
            f.close()
            raise e
        f.close()
    '''
    '''
    def getRequiredColumns(self, requiredColumns):
        """
        This function extracts the columns that are required
        :param requiredColumns:
        :return: columns:List
        """
        try:
            columns = None
            f = open("Prediction_Log/RequiredColumnsLog.txt", 'a+')
            self.logger.log(f, "Getting required columns...", 'Info')
            for file in os.listdir('Prediction_Files/'):
                csv = pd.read_csv('Prediction_Files/'+file)
                columns = csv[requiredColumns].copy()
                self.logger.log(f,'Columns aquired from '+file, 'Info')
        except Exception as e:
            self.logger.log(f, 'Exception occured in getRequiredColumns method in PredictionDataValidation class. Message: '+str(e), 'Error')
            f.close()
            raise e
        f.close()
        return columns

    '''




