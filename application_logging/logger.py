# performing important imports
from datetime import datetime


class AppLogger:
    def __init__(self):
        pass

    def log(self, file_object, log_message, level=None):
        now = datetime.now()
        date = now.date()
        current_time = now.strftime('%H:%M:%S')
        file_object.write(
            str(date)+'/'+str(current_time)+': '+str(level)+' :'+'\t\t'+log_message+'\n'
        )
