#performing important imports
import pandas as pd
from file_operation.file_handler import FileHandler
class PreProcessing:
    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger = logger_object
    '''
    def encodeData(self, predictorData):
        """
        This function encodes the data
        :param data:
        :return: data
        """
        self.logger.log(self.file_object, 'Entered the encodeData method of PreProcessor class', 'Info')
        try:
            self.one_hot_encoder = FileHandler(self.file_object, self.logger).loadModel('oneHotEncoder')
            self.predictorData = self.one_hot_encoder.transform(predictorData)
            #self.label_encoder = FileHandler(self.file_object, self.logger).loadModel('labelEncoder')
            #self.outputData = self.one_hot_encoder.transform(outputData)

            self.logger.log(self.file_object, 'Encoding complete!!. Exiting encoding method...', 'Info')
            return self.predictorData
        # except ValueError:
        #     self.logger.log(self.file_object, 'Value Error occured in encodeData method of PreProcessing class. Message: ' + str(ValueError))
        #     self.logger.log(self.file_object, 'encoding values failed. Exited the encodeData method of the Preprocessor class')
        #     raise ValueError
        except Exception as e:
            self.logger.log(self.file_object, 'Exception occured in encodeData method of PreProcessing class. Message: '+str(e), 'Error')
            self.logger.log(self.file_object, 'Failed to Encode data. Exiting....', 'Error')
            raise e
    '''
    '''
    def isNullPresent(self, data):
        """
        This function returns True if data has missing value
        :param data:
        :return: has_null
        """
        self.logger.log(self.file_object, 'Entered the isNullPresent method of PreProcessor class', 'Info')
        self.has_null = False
        self.cols_with_missing_values = []
        self.cols = data.columns
        self.missing_value_count = []

        try:
            self.cols_with_missing_values = [i for i in self.cols if data[i].isnull().sum()>=1]
            self.missing_value_count = [data[i].isnull().sum() for i in self.cols_with_missing_values]
            if len(self.cols_with_missing_values) > 0:
                self.has_null = True
                # self.dataframe_with_null = pd.DataFrame()
                # self.dataframe_with_null['Columns'] = self.cols_with_missing_values
                # self.dataframe_with_null['Missing value count'] = self.missing_value_count
                # self.dataframe_with_null.to_csv('preprocessing_data/null_values.csv')
                self.logger.log(self.file_object, 'Found missing values. Exiting isNummPresent method of PreProcessor class', 'Info')
            else:
                self.logger.log(self.file_object, 'No missing value found. Exiting isNummPresent method of PreProcessor class', 'Info')
            return self.has_null
        except Exception as e:
            self.logger.log(self.file_object,
                                   'Exception occured in isNullPresent method of the PreProcessor class. Exception message:  ' + str(e), 'Error')
            self.logger.log(self.file_object,
                                   'Finding missing values failed. Exited the isNullPresent method of the PreProcessor class', 'Error')
            raise e
    '''
    '''
    def imputeMissingValues(self, data):
        """
        This function helps to fill the missing value with Imputation techniques
        :param data:
        :return: data
        """
        self.logger.log(self.file_object, 'Entered imputeMissingValues method of PreProcessing class', 'Info')
        self.data = data

        try:

            self.imputer = FileHandler(self.file_object, self.logger).loadModel('simpleImputer')
            self.logger.log(self.file_object, 'Started imputing data.', 'Info')
            #for i in self.cols_with_missing_values:
            self.data = pd.DataFrame(self.imputer.transform(self.data), columns=self.data.columns)
            self.logger.log(self.file_object, 'Imputing data complete!!. Exiting imputeMissingValues method of PreProcessing class', 'Info')
            return self.data
        except ValueError as v:
            self.logger.log(self.file_object, 'Value Error occured in imputeMissingValues method of PreProcessing class. Message: '+ str(ValueError), 'Error')
            self.logger.log(self.file_object, 'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class', 'Error')
            raise v
        except Exception as e:
            self.logger.log(self.file_object, 'Exception occured in impute_missing_values method of the Preprocessor class. Exception message:  ' + str(e), 'Error')
            self.logger.log(self.file_object, 'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class', 'Error')
            raise e

    '''
    def ScaleData(self, data):
        '''

        :return:
        '''
        self.logger.log(self.file_object, 'Entered ScaleData method of PreProcessing class', 'Info')
        self.data = data

        try:
            self.scaler = FileHandler(self.file_object, self.logger).loadModel('StandardScaler')
            self.data = self.scaler.transform(self.data)
        except Exception as e:
            print(e)










