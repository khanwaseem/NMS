import sqlite3
from datetime import datetime
import logging

def convert(date_time): 
    format = '%d-%m-%Y %H:%M:%S'
    datetime_str = datetime.strptime(date_time, format) 
    return datetime_str 
   
def read_device_status_db():
    active_list = []
    try:
        conn = sqlite3.connect('STATUS_RECEIVE_API/STATUS.db')
        c = conn.cursor()
        c.execute("SELECT * FROM Device_status where DeviceStatus='1'")
        x = c.fetchall()
        conn.close()
        current_time = datetime.now()
        current_time = current_time.strftime('%d-%m-%Y %H:%M:%S')
        current_time = datetime.strptime(current_time, "%d-%m-%Y %H:%M:%S")
        #print(type(current_time))
        for item in x:
            status_tme = item[3]
            status_tme = convert(status_tme)
            #print(type(status_tme),status_tme)
            tm_diff = current_time-status_tme
            print(current_time,status_tme)
            tm_diff = round(tm_diff.total_seconds()/60.0)
            print(tm_diff)
            if tm_diff<10:
                active_list.append(item[0])
        return active_list
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        return active_list
