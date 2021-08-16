#performing important imports
import pandas as pd
from file_operation.file_handler import FileHandler
class PreProcessing:
    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger = logger_object

    def ScaleData(self, data,numericalColumns):
        '''

        :return:
        '''
        self.logger.log(self.file_object, 'Entered ScaleData method of PreProcessing class', 'Info')
        self.data = data

        try:
            self.logger.log(self.file_object, 'Initiating Scaling', 'Info')
            self.scaler = FileHandler(self.file_object, self.logger).loadModel('StandardScaler')
            self.data[numericalColumns] = self.scaler.transform(self.data[numericalColumns])
            print('s',self.data)
            self.logger.log(self.file_object, 'Scaling data complete', 'Info')
            return self.data
        except Exception as e:
            self.logger.log(self.file_object,
                            'Exception occured in ScaleData method of the Preprocessor class. Exception message:  ' + str(
                                e), 'Error')
            self.logger.log(self.file_object,
                            'Scaling data failed. Exited the ScaleData method of the Preprocessor class',
                            'Error')
            raise e










