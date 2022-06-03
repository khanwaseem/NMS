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
    tree = ET.parse('ATMS_VIDS_STATUS.xml')
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
    conn = sqlite3.connect('VIDS.db')
    c = conn.cursor()
    # Create table
    c.execute("CREATE TABLE vids_status(VIDS_ID real,Status real, Time text)")
    conn.commit()
    conn.close()
except Exception as e:
    logging.error(e, exc_info=True)

def insert(v_id,time,status):
    try:
        conn = sqlite3.connect('VIDS.db')
        c = conn.cursor()
        c.execute("UPDATE vids_status SET Time='%s' WHERE VIDS_ID=%d" %(time,v_id))
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
            c.execute("INSERT INTO vids_status VALUES (%d,%d,'%s')" %(v_id,status,time))
            conn.commit()
            conn.close()
        else:
            conn.close()
    except Exception as e:
        logging.error(e, exc_info=True)
    
    
@app.route('/Vids_status/', methods=['GET', 'POST'])
def add_message():
    try:
        content = request.json
        v_id = content['VIDS_ID']
        time = content['StatusTimeStamp']
        status = content['DeviceStatus']
        insert(v_id,time,status)
        return jsonify({'Status':1})
    except Exception as e:
        logging.error(e, exc_info=True)
        return jsonify({'Status':0})

if __name__ == '__main__':
    app.run(host= '%s'%conf_ip,port = '%d'%conf_port,debug=False)