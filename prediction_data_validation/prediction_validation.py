# performing important imports
from application_logging.logger import AppLogger
from prediction_data_validation.prediction_data_validation import PredictionDataValidation


class PredictionValidation:
    def __init__(self):
        self.raw_data = PredictionDataValidation()
        self.logger = AppLogger()
        self.logger.database.connect_db()

    def validation(self):
        #file = open("prediction_logs/Prediction_Log.txt", "a+")
        file = 'prediction_log'
        try:
            self.logger.log(file, "Validation started for Prediction Data", "Info")
            # validating Datatype
            self.logger.log(file, "Starting datatype validation", "Info")
            self.raw_data.validate_data_type()
            self.logger.log(file, "Datatype validation complete!!", "Info")
            #file.close()

        except Exception as e:
            #file.close()
            self.logger.database.close_connection()
            raise e
