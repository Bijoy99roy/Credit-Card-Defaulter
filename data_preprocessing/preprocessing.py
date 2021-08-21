# performing important imports
from file_operation.file_handler import FileHandler


class PreProcessing:
    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger = logger_object

    def scale_data(self, data, numerical_columns):
        """
        This function scales down the data
        :return: scaled data
        """
        self.logger.log(self.file_object, 'Entered ScaleData method of PreProcessing class', 'Info')
        try:
            self.logger.log(self.file_object, 'Initiating Scaling', 'Info')
            scaler = FileHandler(self.file_object, self.logger).load_model('StandardScaler')
            data[numerical_columns] = scaler.transform(data[numerical_columns])
            self.logger.log(self.file_object, 'Scaling data complete', 'Info')
            return data
        except Exception as e:
            self.logger.log(self.file_object,
                            'Exception occured in ScaleData method of the Preprocessor class. Exception message:  ' +
                            str(e), 'Error')
            self.logger.log(self.file_object,
                            'Scaling data failed. Exited the ScaleData method of the Preprocessor class',
                            'Error')
            raise e
