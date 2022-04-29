import logging
from datetime import datetime
logging.basicConfig(filename=datetime.now().strftime('%d_%m_%Y.log'),
                    filemode='a',
                    format='%(asctime)s:%(msecs)d-%(name)s-%(levelname)s-%(message)s',
                    datefmt='%H:%M:%S',level=logging.DEBUG)

from pysnmp.hlapi import *
import SwitchUtilization_08_02_2021 as util
from datetime import datetime,timedelta


from pysnmp.hlapi import *
import time
import json


#Function to insert in DeviceStatusHistory table      
def SwitchUtilizationSend(jsn):
    try:
        print("send json",jsn)
        my_json_string = json.dumps(dict(jsn))
       
        #with open('SwitchUtilization.json', 'w') as f:
        #    json.dump(dict(jsn), f)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        r = requests.post(switchUtilurl, my_json_string,headers=headers)
        print(r.status_code, r.reason)
    except Exception as e:
        logging.error(e, exc_info=True)

def Average(lst): 
    return sum(lst) / len(lst)
        
        
def switch_interfaces(ip,community_string):
    try:
        count = 0
        count_up=0
        count_down=0
        test_list = ["Po","po","Gi"]
        for (errorIndication,
             errorStatus,
             errorIndex,
             varBinds) in nextCmd(SnmpEngine(),
                                  CommunityData(community_string, mpModel=0),
                                  UdpTransportTarget((ip, 161)),
                                  ContextData(),
                                  ObjectType(ObjectIdentity('IF-MIB', 'ifDescr')),
                                  ObjectType(ObjectIdentity('IF-MIB', 'ifOperStatus')),
                                  lexicographicMode=False):

            if errorIndication:
                print(errorIndication)
                break
            elif errorStatus:
                print('%s at %s' % (errorStatus.prettyPrint(),
                                    errorIndex and varBinds[int(errorIndex)-1][0] or '?'))
                break

            else:
                for varBind in varBinds:
                    x= '='.join([x.prettyPrint() for x in varBind])
                    x = x.split("=")[1]
                    #print(x)
                    res = list(filter(lambda i:  i in x, test_list)) 
                    if len(res)>0:
                        count+=1
                        for varBind in varBinds:
                            x= '='.join([x.prettyPrint() for x in varBind])
                            x = x.split("=")[1]
                            if x=="up":
                                count_up+=1
                            if x=="down":
                                count_down+=1

        return count,count_up,count_down
    except Exception as e:
        logging.error(e, exc_info=True)
        return None

def switch_sysUpTime(ip,community_string):
    try:
        errorIndication, errorStatus, errorIndex, varBinds = next(
        getCmd(SnmpEngine(),
               CommunityData(community_string, mpModel=0),
               UdpTransportTarget((ip, 161)),
               ContextData(),
               ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysUpTime', 0)))
        )

        if errorIndication:
            print(errorIndication)
            return None
        elif errorStatus:
            print('%s at %s' % (errorStatus.prettyPrint(),
                            errorIndex and varBinds[int(errorIndex) - 1][0] or '?'))
            return None
        else:
            for varBind in varBinds:
                #print(varBind)
                x =' = '.join([x.prettyPrint() for x in varBind])
                x = x.split(",")[0]
                x = x.split("=")[1]
                x = int(x)//100
                x= x//60
            return int(x)
    except Exception as e:
        logging.error(e, exc_info=True)
        return None

    
def switch_name(ip,community_string):
    try:
        errorIndication, errorStatus, errorIndex, varBinds = next(
        getCmd(SnmpEngine(),
               CommunityData(community_string, mpModel=0),
               UdpTransportTarget((ip, 161)),
               ContextData(),
               ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0)))
        )

        if errorIndication:
            print(errorIndication)
            return None
        elif errorStatus:
            print('%s at %s' % (errorStatus.prettyPrint(),
                            errorIndex and varBinds[int(errorIndex) - 1][0] or '?'))
            return None
        else:
            for varBind in varBinds:
                x =' ='.join([x.prettyPrint() for x in varBind])
                #print(x)
                #x = x.split(",")[0]
                x = x.split("=")[1]
                x = x.split(",")[0]
            return x
    except:
        return None
    
def last_change(ip,community_string): 
    try:
        change=[]
        for (errorIndication,
             errorStatus,
             errorIndex,
             varBinds) in nextCmd(SnmpEngine(),
                                  CommunityData(community_string, mpModel=0),
                                  UdpTransportTarget((ip, 161)),
                                  ContextData(),
                                  ObjectType(ObjectIdentity('IF-MIB', 'ifLastChange')),
                                  lexicographicMode=False):

            if errorIndication:
                print(errorIndication)
                break
            elif errorStatus:
                print('%s at %s' % (errorStatus.prettyPrint(),
                                    errorIndex and varBinds[int(errorIndex)-1][0] or '?'))
                break
            else:
                for varBind in varBinds:
                    x='='.join([x.prettyPrint() for x in varBind])
                    x = int(x.split("=")[1])
                    #print(x)
                    change.append(x)
        change = [i for i in change if i!= 0]
        try:
            min_change = min(change)
            return int(min_change)
        except:
            return 0
    
    except Exception as e:
        logging.error(e, exc_info=True)
        return None

def maximum_speed(ip,community_string): 
    try:
        speed=[]
        for (errorIndication,
             errorStatus,
             errorIndex,
             varBinds) in nextCmd(SnmpEngine(),
                                  CommunityData(community_string, mpModel=0),
                                  UdpTransportTarget((ip, 161)),
                                  ContextData(),
                                  ObjectType(ObjectIdentity('IF-MIB', 'ifSpeed')),
                                  lexicographicMode=False):

            if errorIndication:
                print(errorIndication)
                break
            elif errorStatus:
                print('%s at %s' % (errorStatus.prettyPrint(),
                                    errorIndex and varBinds[int(errorIndex)-1][0] or '?'))
                break
            else:
                for varBind in varBinds:
                    x='='.join([x.prettyPrint() for x in varBind])
                    x = int(x.split("=")[1])
                    speed.append(x)
        try:
            max_speed = max(speed)
        except:
            max_speed = 10000
        return int(max_speed)

    except Exception as e:
        logging.error(e, exc_info=True)
        return None

def Dropped_packedt(ip,community_string): 
    try:
        dis_packet=[]
        for (errorIndication,
             errorStatus,
             errorIndex,
             varBinds) in nextCmd(SnmpEngine(),
                                  CommunityData(community_string, mpModel=0),
                                  UdpTransportTarget((ip, 161)),
                                  ContextData(),
                                  ObjectType(ObjectIdentity('IF-MIB', 'ifInDiscards')),
                                  ObjectType(ObjectIdentity('IF-MIB', 'ifOutDiscards')),
                                  lexicographicMode=False):

            if errorIndication:
                print(errorIndication)
                break
            elif errorStatus:
                print('%s at %s' % (errorStatus.prettyPrint(),
                                    errorIndex and varBinds[int(errorIndex)-1][0] or '?'))
                break
            else:
                for varBind in varBinds:
                    x='='.join([x.prettyPrint() for x in varBind])
                    x = int(x.split("=")[1])
                    #print(x)
                    dis_packet.append(x)
        discard = sum(dis_packet)
        return int(discard)

    except Exception as e:
        logging.error(e, exc_info=True)
        return None
    
def error_packedt(ip,community_string): 
    try:
        err_packet=[]
        for (errorIndication,
             errorStatus,
             errorIndex,
             varBinds) in nextCmd(SnmpEngine(),
                                  CommunityData(community_string, mpModel=0),
                                  UdpTransportTarget((ip, 161)),
                                  ContextData(),
                                  ObjectType(ObjectIdentity('IF-MIB', 'ifOutErrors')),
                                  ObjectType(ObjectIdentity('IF-MIB', 'ifInErrors')),
                                  lexicographicMode=False):

            if errorIndication:
                print(errorIndication)
                break
            elif errorStatus:
                print('%s at %s' % (errorStatus.prettyPrint(),
                                    errorIndex and varBinds[int(errorIndex)-1][0] or '?'))
                break
            else:
                for varBind in varBinds:
                    x='='.join([x.prettyPrint() for x in varBind])
                    x = int(x.split("=")[1])
                    #print(x)
                    err_packet.append(x)
        error = sum(err_packet)
        return int(error)
    
    except Exception as e:
        logging.error(e, exc_info=True)
        return None
    

def switch_detail(ip):
    community_string = "VaaaN"
    num_of_interfaces,num_of_uplink,num_of_downlink = switch_interfaces(ip,community_string)
    s_name = switch_name(ip,community_string)
    s_uptime = switch_sysUpTime(ip,community_string)
    num_of_packet_dropped = Dropped_packedt(ip,community_string)
    num_of_packet_error = error_packedt(ip,community_string)
    max_speed = maximum_speed(ip,community_string)
    l_change = last_change(ip,community_string)
    return s_name,s_uptime,num_of_interfaces,num_of_uplink,num_of_downlink,l_change,num_of_packet_dropped,num_of_packet_error

def Switch_Info(DeviceId_list,Device_Ip_Map):
    #DeviceId_list=[1]
    # Device_Ip_Map = {1:"192.168.2.36"}
    mapped={}
    while True:
        try:
            for i in DeviceId_list:
                ip = Device_Ip_Map[i]
                print(i,ip)
                Model,UpTime,TotalInterface,UpInterface,DownInterface,LastChanged,DropedPacket,ErrorPacket=switch_detail(ip)
                try:
                    walk1 = mapped[i]
                except:
                    walk1 = []
                AverageUtilization,PortUtilzation,walk1 = util.linkspeed(ip,walk1)
                AverageUtilization = round(AverageUtilization,4)
                mapped[i]=walk1
                current_time = datetime.now()
                current_time = current_time.strftime('%d-%m-%Y %H:%M:%S')
                if len(PortUtilzation)>0:
                    jsn = {"DeviceId":i,"StatusTimeStamp":current_time,"AverageUtilization":AverageUtilization,"PortUtilization":PortUtilzation,"Model":Model,"UpTime":UpTime,"TotalInterface":TotalInterface,"UpInterface":UpInterface,"DownInterface":DownInterface,"LastChanged":LastChanged,"DropedPacket":DropedPacket,"ErrorPacket":ErrorPacket}
                    SwitchUtilizationSend(jsn)
                    #time.sleep(1)
        except Exception as e:
            logging.error(e, exc_info=True)
            


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
    import sqlite3
    import pandas as pd
    import time
    #time.sleep(10)
    from datetimerange import DateTimeRange
    import status_read as rs
    import cv2
    import sys
    from threading import Thread
    import telnetlib
    import json
    import subprocess
    import socket
    import requests
    import json
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
    switchUtilurl = config_data[6]
    print(switchUtilurl)
    try:
        r = requests.get(url = receiveUrl) 
        data = r.json()
        df = pd.DataFrame(data)
    except Exception as e:
        logging.error(e, exc_info=True)

    ACTIVE_EQUIPMENT_LIST = df["HardwareTypeId"].unique().tolist()
    ServiceType_Dict = dict(zip(df.DeviceId, df.IntegrationMethodologyId))
    
except Exception as e:
    logging.error(e, exc_info=True)
    
try:
    df_met = df.loc[df['HardwareTypeId'] ==11]
    df_met = df_met[df_met['DeviceId'].notnull()]
    df_met = df_met[df_met['LocalIpAddress'].notnull()]
    Device_DICT = dict(zip(df_met.DeviceId, df_met.LocalIpAddress))
    Device_List = df_met["DeviceId"].unique().tolist()
    Device_DICT_MAP = dict(zip(df_met.DeviceId, df_met.HardwareTypeId))
    Device_PORT_MAP_DICT = dict(zip(df_met.DeviceId, df_met.LocalPort))
    print("Switch",Device_List)
    print("Switch",Device_DICT_MAP)
except Exception as e:
    logging.error(e, exc_info=True)

try:
    #Device_List=[50]
    #Device_DICT = {50:"192.168.1.24"}
    thread1 = Thread(target = Switch_Info, args = (Device_List,Device_DICT),daemon=True)
    thread1.start()
    while True:
        time.sleep(1000)
except Exception as e:
    logging.error(e, exc_info=True)