#python snmp trap receiver
import logging
from datetime import datetime,timedelta
logging.basicConfig(filename=datetime.now().strftime('%d_%m_%Y.log'),
                    filemode='a',
                    format='%(asctime)s:%(msecs)d-%(name)s-%(levelname)s-%(message)s',
                    datefmt='%d-%m-%Y %H:%M:%S',level=logging.DEBUG)
try:
    import pkg_resources.py2_warn
    from pysnmp.entity import engine, config
    from pysnmp.carrier.asyncore.dgram import udp
    from pysnmp.entity.rfc3413 import ntfrcv
    import os
    import json
    import requests
    import xml.etree.ElementTree as ET
    import time
    import sqlite3
    import pandas as pd
    import time
    tree = ET.parse('Trap.xml')
    root = tree.getroot()
    config_data = []
    for elem in root:
        for subelem in elem:
            config_data.append(subelem.text)
    ip = config_data[0]
    port = int(config_data[1])
    TrapUrl = config_data[2]
    receiveUrl = config_data[3]
except Exception as e:
    logging.error(e, exc_info=True)

    
try:
    try:
        r = requests.get(url = receiveUrl) 
        data = r.json()
        df = pd.DataFrame(data)
    except Exception as e:
        logging.error(e, exc_info=True)
    
except Exception as e:
    logging.error(e, exc_info=True)

try:
    df_swt = df.loc[df['HardwareTypeId'] ==11]
    df_swt = df_swt[df_swt['DeviceId'].notnull()]
    df_swt = df_swt[df_swt['LocalIpAddress'].notnull()]
    Device_Dict = dict(zip(df_swt.LocalIpAddress, df_swt.DeviceId))
except Exception as e:
    logging.error(e, exc_info=True)
        

#Device_Dict2={'192.168.184.177':1}
Device_Dict3 = {}    
for key, value in Device_Dict.items():
    key2 = ("'%s'"%key)
    Device_Dict3[key2]=value

def SendTrap(DeviceId,trap_id,current_time):
    try:
        DeviceId = DeviceId
        TrapedId = trap_id
        StatusTimeStamp = current_time
        print("send json",(DeviceId,TrapedId,StatusTimeStamp))
        my_json_string = json.dumps(dict({'DeviceId': DeviceId, 'TrapedId': TrapedId,'StatusTimeStamp':StatusTimeStamp}))
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        r = requests.post(TrapUrl, my_json_string,headers=headers)
        print(r.status_code, r.reason)
    except Exception as e:
        logging.error(e, exc_info=True)
    
try:
    snmpEngine = engine.SnmpEngine()

    TrapAgentAddress=ip; #Trap listerner address
    Port=port;  #trap listerner port

    print("Agent is listening SNMP Trap on "+TrapAgentAddress+" , Port : " +str(Port));
    print('--------------------------------------------------------------------------');
    config.addTransport(
        snmpEngine,
        udp.domainName + (1,),
        udp.UdpTransport().openServerMode((TrapAgentAddress, Port))
    )

    #Configure community here
    config.addV1System(snmpEngine, 'my-area', 'VaaaN')
except Exception as e:
    logging.error(e, exc_info=True)

# Callback function for receiving notifications
def cbFun(snmpEngine, stateReference, contextEngineId, contextName,
          varBinds, cbCtx):
    for name, val in varBinds:  
        detect = False
        x = val.prettyPrint()
        #with open("TrapOutput.txt", "a+") as text_file:
            #text_file.write(x+ '\n')
        if x=="1.3.6.1.6.3.1.1.5.5":
            trap_id = 1
            detect = True
        if x=="1.3.6.1.6.3.1.1.5.4":
            trap_id = 2
            detect = True
        if x=="1.3.6.1.6.3.1.1.5.3":
            trap_id = 3
            detect = True
        if x=="1.3.6.1.6.3.1.1.5.2":
            trap_id = 4
            detect = True
        if x=="1.3.6.1.6.3.1.1.5.1":
            trap_id = 5
            detect = True
        if x=="1.3.6.1.4.1.4526.11.1.0.13":
            trap_id = 1
            detect = True
        if x == "1.3.6.1.2.1.1":
            trap_id = 1
            detect = True
            #print('%s = %s' % (name.prettyPrint(), val.prettyPrint()))
        if detect == True:
            try:
                execContext = snmpEngine.observer.getExecutionContext('rfc3412.receiveMessage:request')
                with open("TrapOutput.txt", "a+") as text_file:
                    text_file.write(str(execContext)+ '\n')
                print('Notification from %s:%s' % execContext['transportAddress'])
                source_ip = str(execContext['transportAddress'])
                source_ip = source_ip.split(",")[0]
                source_ip = source_ip.split("(")[1]
                print(type(source_ip))
                current_time = datetime.now()
                current_time = current_time.strftime('%d-%m-%Y %H:%M:%S')
                print(source_ip,trap_id,current_time)
                source_ip=str(source_ip)
                text = source_ip+" %d"%trap_id+" "+str(current_time)
                with open("TrapOutput.txt", "a+") as text_file:
                    text_file.write(text+ '\n')
                #print(Device_Dict2)
                print(Device_Dict3)
                current_time = datetime.now()
                current_time = current_time.strftime('%d-%m-%Y %H:%M:%S')
                try:
                    DeviceId = Device_Dict3[source_ip]
                except:
                    DeviceId = Device_Dict[source_ip]
                SendTrap(DeviceId,trap_id,current_time)
                break
            except Exception as e:
                logging.error(e, exc_info=True)


try:
    ntfrcv.NotificationReceiver(snmpEngine, cbFun)

    snmpEngine.transportDispatcher.jobStarted(1)  

    try:
        snmpEngine.transportDispatcher.runDispatcher()
    except Exception as e:
        logging.error(e, exc_info=True)
        snmpEngine.transportDispatcher.closeDispatcher()
        raise
except Exception as e:
    logging.error(e, exc_info=True)
