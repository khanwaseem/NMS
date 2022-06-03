import logging
from datetime import datetime
logging.basicConfig(filename=datetime.now().strftime('%d_%m_%Y.log'),
                    filemode='a',
                    format='%(asctime)s:%(msecs)d-%(name)s-%(levelname)s-%(message)s',
                    datefmt='%H:%M:%S',level=logging.DEBUG)

try:
    from flask import Flask, request, jsonify
    app = Flask(__name__)
    import sqlite3
    import pkg_resources.py2_warn
    import xml.etree.ElementTree as ET
except Exception as e:
    logging.error(e, exc_info=True)

try:
    tree = ET.parse('ATMS_ECB_STATUS.xml')
    root = tree.getroot()
    config_data = []
    for elem in root:
        for subelem in elem:
            config_data.append(subelem.text)
            #print("Data file:",config_data[0])

    conf_ip = config_data[0]
    conf_port = int(config_data[1])
except Exception as e:
    logging.error(e, exc_info=True)

try:
    conn = sqlite3.connect('ECB.db')
    c = conn.cursor()
    # Create table
    c.execute("CREATE TABLE ECB_status(ip_add text,Status real, Time text, Message text)")
    conn.commit()
    conn.close()
except Exception as e:
    logging.error(e, exc_info=True)
    
try:
    conn = sqlite3.connect('ECB.db')
    c = conn.cursor()
    # Create table
    c.execute("CREATE TABLE ECB_Event(Event_ID real, ip_add text,Status real, Time text, Message text)")
    conn.commit()
    conn.close()
except Exception as e:
    logging.error(e, exc_info=True)

def insert_status(ip_add,status,event_date_str,message):
    try:
        conn = sqlite3.connect('ECB.db')
        c = conn.cursor()
        c.execute("UPDATE ECB_status SET Time='%s',Message='%s' WHERE ip_add='%s'" %(event_date_str,message,ip_add))
        conn.commit()
        c = conn.cursor()
        x = c.execute("SELECT changes();")
        y = x.fetchall()
        no_of_update = y[0][0]
        print(no_of_update)
        print(type(no_of_update))
        #print(time)
        if no_of_update==0:
            c = conn.cursor()
            c.execute("INSERT INTO ECB_status VALUES ('%s',%d,'%s','%s')" %(ip_add,status,event_date_str,message))
            conn.commit()
            conn.close()
        else:
            conn.close()
    except Exception as e:
        logging.error(e, exc_info=True)
    
    
    
def insert_event(event_id,ip_add,status,event_date_str,message):
    try:
        conn = sqlite3.connect('ECB.db')
        c = conn.cursor()
        c.execute("INSERT INTO ECB_Event VALUES (%d,'%s',%d,'%s','%s')" %(event_id,ip_add,status,event_date_str,message))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(e, exc_info=True)
    
@app.route('/ecb_status/', methods=['GET', 'POST'])
def add_message():
    try:
        content = request.json
        #print(content)
        tran_id = content['EventMessageId']
        event_id = content['EventType']
        user_type = content['UserType']
        voip_num = content['VoipNumber']
        message = content['Message']
        status = content['Status']
        ip_add = content['IpAddress']
        event_date_str = content['EventDateTimeString']
        #print(event_id,type(event_id))
        #print(tran_id,event_id,user_type,voip_num,message,status,ip_add,event_date_str)
        if event_id==0:
            insert_status(ip_add,status,event_date_str,message)
        if event_id==9 or event_id==10 or event_id==11:
            insert_event(event_id,ip_add,status,event_date_str,message)
        return jsonify({'Status':1})
    except Exception as e:
        logging.error(e, exc_info=True)
        return jsonify({'Status':0})

if __name__ == '__main__':
    app.run(host= '%s'%conf_ip,port = '%d'%conf_port,debug=False)