import sqlite3
from datetime import datetime

def convert(date_time): 
    format = '%d-%m-%y %H:%M:%S'
    datetime_str = datetime.strptime(date_time, format) 
    return datetime_str 
   
def read_vids_db():
    active_list = []
    try:
        
        conn = sqlite3.connect('VIDS_STATUS_API/VIDS.db')
        c = conn.cursor()
        c.execute('SELECT * FROM vids_status')
        x = c.fetchall()
        conn.close()
        current_time = datetime.now()
        current_time = current_time.strftime('%d-%m-%y %H:%M:%S')
        current_time = datetime.strptime(current_time, "%d-%m-%y %H:%M:%S")
        #print(type(current_time))
        for item in x:
            status_tme = item[2]
            status_tme = convert(status_tme)
            #print(type(status_tme),status_tme)
            tm_diff = current_time-status_tme
            tm_diff = round(tm_diff.total_seconds()/60.0)
            if tm_diff<10:
                active_list.append(int(item[0]))
        return active_list
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        return active_list