# performing important imports
from datetime import datetime
from db_operations.database import DataBase


class AppLogger:
    def __init__(self):
        self.database = DataBase()

    def create_table(self):
        self.database.create_tables()

    def log(self, table_name, message, level=''):
        now = datetime.now()
        date = now.date()
        current_time = now.strftime('%H:%M:%S')
        self.database.insert_data(table_name, date, current_time, message, level)

    # def log(self, file_object, log_message, level=None):
    #     now = datetime.now()
    #     date = now.date()
    #     current_time = now.strftime('%H:%M:%S')
    #     file_object.write(
    #         str(date)+'/'+str(current_time)+': '+str(level)+' :'+'\t\t'+log_message+'\n'
    #     )
