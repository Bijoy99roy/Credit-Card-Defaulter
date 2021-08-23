# performing important imports
from file_operation.file_handler import FileHandler


class PreProcessing:
    def __init__(self, table_name, logger_object):
        self.table_name = table_name
        self.logger = logger_object

    def scale_data(self, data, numerical_columns):
        """
        This function scales down the data
        :return: scaled data
        """

        self.logger.log(self.table_name, 'Entered ScaleData method of PreProcessing class', 'Info')
        try:
            self.logger.log(self.table_name, 'Initiating Scaling', 'Info')
            scaler = FileHandler(self.table_name, self.logger).load_model('StandardScaler')
            data[numerical_columns] = scaler.transform(data[numerical_columns])
            self.logger.log(self.table_name, 'Scaling data complete', 'Info')
            return data
        except Exception as e:
            self.logger.log(self.table_name,
                            'Exception occured in ScaleData method of the Preprocessor class. Exception message:  ' +
                            str(e), 'Error')
            self.logger.log(self.table_name,
                            'Scaling data failed. Exited the ScaleData method of the Preprocessor class',
                            'Error')
            self.logger.database.close_connection()
            raise e
