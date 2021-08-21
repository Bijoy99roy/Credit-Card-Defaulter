# performing important imports
from application_logging.logger import AppLogger
from prediction_data_validation.prediction_data_validation import PredictionDataValidation


class PredictionValidation:
    def __init__(self):
        self.raw_data = PredictionDataValidation()
        self.logger = AppLogger()

    def validation(self):
        file = open("Prediction_Log/Prediction_Log.txt", "a+")
        try:
            self.logger.log(file, "Validation started for Prediction Data")
            # validating Datatype
            self.logger.log(file, "Starting datatype validation")
            self.raw_data.validate_data_type()
            self.logger.log(file, "Datatype validation complete!!")
            file.close()

        except Exception as e:
            file.close()
            raise e
