import sqlite3
from datetime import datetime
import logging
from datetime import datetime
logging.basicConfig(filename=datetime.now().strftime('%d_%m_%Y.log'),
                    filemode='a',
                    format='%(asctime)s:%(msecs)d-%(name)s-%(levelname)s-%(message)s',
                    datefmt='%H:%M:%S',level=logging.DEBUG)


def convert(date_time): 
    format = '%d-%b-%Y %H:%M:%S'
    datetime_str = datetime.strptime(date_time, format) 
    return datetime_str 
   
def read_ecb_db():
    active_list = []
    try:
        conn = sqlite3.connect('ECB_APIv1.0/ECB.db')
        c = conn.cursor()
        c.execute('SELECT * FROM ECB_status')
        fetch_list = c.fetchall()
        print(fetch_list)
        conn.close()
        current_time = datetime.now()
        current_time = current_time.strftime('%d-%m-%y %H:%M:%S')
        current_time = datetime.strptime(current_time, "%d-%m-%y %H:%M:%S")
        #print(type(current_time))
        for item in fetch_list:
            status_tme = item[2]
            status_tme = convert(status_tme)
            #print(type(status_tme),status_tme)
            tm_diff = current_time-status_tme
            tm_diff = round(tm_diff.total_seconds()/60.0)
            print(tm_diff)
            if tm_diff<10 and item[3]=="1":
                active_list.append(item[0])
        return active_list
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        return active_list
    
