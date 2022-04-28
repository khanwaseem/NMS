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
    tree = ET.parse('STATUS_RECEIVE_API.xml')
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
    conn = sqlite3.connect('STATUS.db')
    c = conn.cursor()
    # Create table
    c.execute("CREATE TABLE Device_status(DeviceIp text,DevicePort real, DeviceStatus real,StatusTimeStamp text)")
    conn.commit()
    conn.close()
except Exception as e:
    logging.error(e, exc_info=True)
    
def insert_into_device(d_ip,d_port,d_status,time):
    try:
        conn = sqlite3.connect('STATUS.db')
        c = conn.cursor()
        c.execute("UPDATE Device_status SET StatusTimeStamp='%s',DeviceStatus=%d WHERE DeviceIp='%s' and DevicePort=%d" %(time,d_status,d_ip,d_port))
        conn.commit()
        c = conn.cursor()
        x = c.execute("SELECT changes();")
        y = x.fetchall()
        no_of_update = y[0][0]
        print(no_of_update)
        print(type(no_of_update))
        print(time)
        if no_of_update==0:
            c = conn.cursor()
            c.execute("INSERT INTO Device_status VALUES ('%s',%d,%d,'%s')" %(d_ip,d_port,d_status,time))
            conn.commit()
            conn.close()
        else:
            conn.close()
    except Exception as e:
        logging.error(e, exc_info=True)
    

@app.route('/status/', methods=['POST'])
def add_message():
    try:
        content = request.get_json(force=True)
        print(content)
        d_ip = content['DeviceIp']
        d_port = content['DevicePort']
        d_status = content['DeviceStatus']
        time = content['StatusTimeStamp']
        insert_into_device(d_ip,d_port,d_status,time)
        return jsonify({'Status':1})
    except Exception as e:
        logging.error(e, exc_info=True)
        return jsonify({'Status':0})

if __name__ == '__main__':
    app.run(host= '%s'%conf_ip,port = '%d'%conf_port,debug=False)