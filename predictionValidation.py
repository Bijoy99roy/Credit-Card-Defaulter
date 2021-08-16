#performing important imports
from application_logging.logger import App_Logger
from prediction_data_validation.prediction_data_validation import PredictionDataValidation

class PredictionValidation:
    def __init__(self):
        self.raw_data = PredictionDataValidation()
        self.logger = App_Logger()

    def validation(self):
        try:
            f = open("Prediction_Log/Prediction_Log.txt", "a+")

            self.logger.log(f,"Validation started for Prediction Data")

            #validating Datatyoe
            self.logger.log(f, "Starting dataype validation")
            self.raw_data.ValidateDataType()
            self.logger.log(f, "Datatype validation complete!!")
            f.close()

        except Exception as e:
            f.close()
            raise e


