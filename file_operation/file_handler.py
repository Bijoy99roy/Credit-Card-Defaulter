# performing important imports
import pickle


class FileHandler:
    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger = logger_object
        self.model_path = 'models/'

    def load_model(self, filename):
        """
        This function helps to load different .sav files
        :param filename:
        :return: FileObject
        """
        print('fil')
        self.logger.log(self.file_object, 'Entered the loadModel method of FileHandler class', 'Info')
        print('fil2')
        try:
            with open(self.model_path+filename+'.sav', 'rb') as f:
                self.logger.log(
                    self.file_object,
                    filename+' loaded. Exiting loadModel method of FileHandler class',
                    'Info')
                return pickle.load(f)

        except Exception as e:
            self.logger.log(
                self.file_object,
                'Error occured in loadModel method of FileHandler class. Message: '+str(e),
                'Error')
            self.logger.database.close_connection()
            raise e
