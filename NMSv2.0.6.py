#Start of NMSv2 

#Function to insert in DeviceStatusHistory table      
def StatusSend(i,Status,network_type,latency,time_1):
    try:
        DeviceId = i
        NetworkStatus = Status
        NetworkType = network_type
        NetworkLatency = latency
        StatusTimeStamp = time_1
        print("send json",(DeviceId,NetworkStatus,NetworkType,NetworkLatency,StatusTimeStamp))
        my_json_string = json.dumps(dict({'DeviceId': DeviceId, 'NetworkStatus': NetworkStatus,'NetworkType':NetworkType,'NetworkLatency':NetworkLatency,'StatusTimeStamp':StatusTimeStamp}))
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        r = requests.post(Sendurl, my_json_string,headers=headers)
        print(r.status_code, r.reason)
    except Exception as e:
        logging.error(e, exc_info=True)
        

#Function to check ATMS/NMS/other web application in control room through telnet command 
def SOFTWRE_Test(ip,port):
    try:
        port = str(port)
        conn = telnetlib.Telnet(ip,port)
        response = 1
    except:
        response = 0
    finally:
        return response   
    
def TCP_Test(ip,port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip, int(port)))
        s.shutdown(2)
        return 1
    except:
        return 0
        
# Function to test the SNMP status of SNMP device 
def SNMP_Test(ip):
    try:
        #print("IP",ip)
        errorIndication, errorStatus, errorIndex, varBinds = next(
        getCmd(SnmpEngine(),
               CommunityData("VaaaN", mpModel=0),
               UdpTransportTarget((ip, 161)),
               ContextData(),
               ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0)))
        )

        if errorIndication:
            print(errorIndication)
            return 0
        elif errorStatus:
            print('%s at %s' % (errorStatus.prettyPrint(),
                            errorIndex and varBinds[int(errorIndex) - 1][0] or '?'))
            return 0
        else:
            for varBind in varBinds:
                x =' = '.join([x.prettyPrint() for x in varBind])
                print(x)
                x = x.split("=")[1]
            if len(x)>0:
                return 1
            else:
                return 0
    except Exception as e:
        logging.error(e, exc_info=True)
        return 0
    
#Function to check the cctv cameras 
def CAMERA():
    while True:
        try:
            global CCTV_List
            global CCTV_DICT
            global CCTV_USER_DICT
            global CCTV_PASSWPRD_DICT
            global ServiceType_Dict
            global DeviceNameMapping
            
            Equip_ID = 1
            for i in CCTV_List:
                #print("cycle")
                current_time = datetime.now()
                current_time = current_time.strftime('%d-%b-%Y %H:%M:%S')
                ip = CCTV_DICT[i]
                network_type = 1
                if ServiceType_Dict[i]==0:
                    Status,latency=pl.pings(ip)     
                if ServiceType_Dict[i]==1:
                    Status,latency=pl.pings(ip)
                    CCTV_USER_DICT,CCTV_PASSWPRD_DICT
                    cam_user = CCTV_USER_DICT[i]
                    cam_password = CCTV_PASSWPRD_DICT[i]
                    cap = cv2.VideoCapture("rtsp://{0}:{1}@{2}".format(cam_user,cam_password,ip))
                    ret,frame = cap.read()
                    if ret:
                        Status=1
                    else:
                        cap = cv2.VideoCapture("rtsp://{0}:{1}@{2}:554/cam/realmonitor?channel=1&subtype=0".format(cam_user,cam_password,ip))
                        ret,frame = cap.read()
                        if ret:
                            Status=1 
                        else:
                            Status=0
                print(i,Status,network_type,latency,current_time)
                StatusSend(i,Status,network_type,latency,current_time)

        except Exception as e:
            logging.error(e, exc_info=True)
        time.sleep(sleep_time)


def OTHERS():
    try:
        global Device_List
        global Device_DICT_MAP
        global Device_DICT
        global Device_PORT_MAP_DICT
        global ServiceType_Dict
        global DeviceNameMapping
        
        for i in Device_List:
            HardwareTypeId = Device_DICT_MAP[i]
            current_time = datetime.now()
            current_time = current_time.strftime('%d-%b-%Y %H:%M:%S')
            ip = Device_DICT[i] 
            network_type = 1
            if ServiceType_Dict[i]==0:
                Status,latency=pl.pings(ip)
            if ServiceType_Dict[i]==2:
                Status,latency=pl.pings(ip)
                Status = SNMP_Test(ip)
            if ServiceType_Dict[i]==3:
                Status,latency=pl.pings(ip)
                port = Device_PORT_MAP_DICT[i]
                Status = SOFTWRE_Test(ip,port)
            if ServiceType_Dict[i]==5:
                Status,latency=pl.pings(ip)
                port = Device_PORT_MAP_DICT[i]
                Status = TCP_Test(ip,port)
            if ServiceType_Dict[i]==4:
                Status,latency=pl.pings(ip)
                try:
                    ACTIVE_LIST = rs.read_device_status_db() 
                except Exception as e:
                    logging.error(e, exc_info=True)
                if HardwareTypeId==18:
                    ip = DeviceNameMapping[i]
                if ip in ACTIVE_LIST:
                    Status = 1
                else:
                    Status= 0
            StatusSend(i,Status,network_type,latency,current_time) 
    except Exception as e:
        logging.error(e, exc_info=True)
        
import logging
from datetime import datetime,timedelta
logging.basicConfig(filename=datetime.now().strftime('%d_%m_%Y.log'),
                    filemode='a',
                    format='%(asctime)s:%(msecs)d-%(name)s-%(levelname)s-%(message)s',
                    datefmt='%d-%m-%Y %H:%M:%S',level=logging.DEBUG)
try:
    import pkg_resources.py2_warn
    import sys
    import os
    try:
        print (sys._MEIPASS)
        print (os.listdir(sys._MEIPASS + '/pysnmp_mibs' ))
    except:
        pass
    from pysnmp.hlapi import *
    import ping_latency_new as pl
    import sqlite3
    import pandas as pd
    import time
    time.sleep(10)
    from datetimerange import DateTimeRange
    import DeviceStatusRead as rs
    import cv2
    import sys
    from threading import Thread
    import telnetlib
    import json
    import subprocess
    import socket
    import requests
    import xml.etree.ElementTree as ET
except Exception as e:
    logging.error(e, exc_info=True)
    
try:
    tree = ET.parse('NMSSQL.xml')
    root = tree.getroot()
    config_data = []
    for elem in root:
        for subelem in elem:
            config_data.append(subelem.text)
    Server = config_data[0]
    user_id = config_data[1]
    password = config_data[2]
    sleep_time = int(config_data[3])
    Sendurl = config_data[4]
    receiveUrl = config_data[5]
    print(Sendurl)
    try:
        r = requests.get(url = receiveUrl) 
        data = r.json()
        df = pd.DataFrame(data)
        print(df.head(10))
    except Exception as e:
        logging.error(e, exc_info=True)

    ACTIVE_EQUIPMENT_LIST = df["HardwareTypeId"].unique().tolist()
    ServiceType_Dict = dict(zip(df.DeviceId, df.IntegrationMethodologyId))
    DeviceNameMapping = dict(zip(df.DeviceId, df.DeviceName))
    
except Exception as e:
    logging.error(e, exc_info=True)

try:
    df_cctv = df.loc[df['IntegrationMethodologyId'] ==1]
    df_cctv = df_cctv[df_cctv['DeviceId'].notnull()]
    df_cctv = df_cctv[df_cctv['LocalIpAddress'].notnull()]
    CCTV_DICT = dict(zip(df_cctv.DeviceId, df_cctv.LocalIpAddress))
    CCTV_List = df_cctv["DeviceId"].unique().tolist()
    CCTV_USER_DICT = dict(zip(df_cctv.DeviceId, df_cctv.DeviceLoginId))
    CCTV_PASSWPRD_DICT = dict(zip(df_cctv.DeviceId, df_cctv.DevicePassword))
    print("Camera",CCTV_List)
except Exception as e:
    logging.error(e, exc_info=True)
        
try:
    df_met = df.loc[df['IntegrationMethodologyId'] !=1]
    df_met = df_met[df_met['DeviceId'].notnull()]
    df_met = df_met[df_met['LocalIpAddress'].notnull()]
    Device_DICT = dict(zip(df_met.DeviceId, df_met.LocalIpAddress))
    Device_List = df_met["DeviceId"].unique().tolist()
    Device_DICT_MAP = dict(zip(df_met.DeviceId, df_met.HardwareTypeId))
    Device_PORT_MAP_DICT = dict(zip(df_met.DeviceId, df_met.LocalPort))
    print("OTHER",Device_List)
    print("OTHER",Device_DICT_MAP)
except Exception as e:
    logging.error(e, exc_info=True)
    
    
try:
    thread1 = Thread(target = CAMERA,daemon=True)
    thread1.start()
    while True:
        time.sleep(sleep_time)
        try:
            OTHERS()
            r = requests.get(url = receiveUrl) 
            data = r.json()
            df = pd.DataFrame(data)
            ServiceType_Dict = dict(zip(df.DeviceId, df.IntegrationMethodologyId))
            try:
                DeviceNameMapping = dict(zip(df.DeviceId, df.DeviceName))
                df_cctv = df.loc[df['IntegrationMethodologyId'] ==1]
                df_cctv = df_cctv[df_cctv['DeviceId'].notnull()]
                df_cctv = df_cctv[df_cctv['LocalIpAddress'].notnull()]
                CCTV_DICT = dict(zip(df_cctv.DeviceId, df_cctv.LocalIpAddress))
                CCTV_List = df_cctv["DeviceId"].unique().tolist()
                CCTV_USER_DICT = dict(zip(df_cctv.DeviceId, df_cctv.DeviceLoginId))
                CCTV_PASSWPRD_DICT = dict(zip(df_cctv.DeviceId, df_cctv.DevicePassword))
                print("Camera",CCTV_List)
            except Exception as e:
                logging.error(e, exc_info=True)
                
            try:
                df_met = df.loc[df['IntegrationMethodologyId'] !=1]
                df_met = df_met[df_met['DeviceId'].notnull()]
                df_met = df_met[df_met['LocalIpAddress'].notnull()]
                Device_DICT = dict(zip(df_met.DeviceId, df_met.LocalIpAddress))
                Device_List = df_met["DeviceId"].unique().tolist()
                Device_DICT_MAP = dict(zip(df_met.DeviceId, df_met.HardwareTypeId))
                Device_PORT_MAP_DICT = dict(zip(df_met.DeviceId, df_met.LocalPort))
                print("OTHER",Device_List)
                print("OTHER",Device_DICT_MAP)
            except Exception as e:
                logging.error(e, exc_info=True)
            
        except Exception as e:
            logging.error(e, exc_info=True)    
        
except Exception as e:
    logging.error(e, exc_info=True)
 

