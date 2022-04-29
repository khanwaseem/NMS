import logging
from datetime import datetime
logging.basicConfig(filename=datetime.now().strftime('%d_%m_%Y.log'),
                    filemode='a',
                    format='%(asctime)s:%(msecs)d-%(name)s-%(levelname)s-%(message)s',
                    datefmt='%H:%M:%S',level=logging.DEBUG)

from pysnmp.hlapi import *
import time

def Average(lst): 
    return sum(lst) / len(lst)

def linkspeed(ip,walk1):
    community_string = "VaaaN"
    try:
        l = []
        l1 = []
        for (errorIndication,
             errorStatus,
             errorIndex,
             varBinds) in nextCmd(SnmpEngine(),
                                    CommunityData(community_string, mpModel=0),
                                    UdpTransportTarget((ip, 161)),
                                    ContextData(),
                                    ObjectType(ObjectIdentity('IF-MIB', 'ifName')),
                                    ObjectType(ObjectIdentity('IF-MIB', 'ifIndex')),
                                    ObjectType(ObjectIdentity('IF-MIB', 'ifSpeed')),
                                    lexicographicMode=False):
            l2 = []
            if errorIndication:
                print(errorIndication)
                break
            elif errorStatus:
                print('%s at %s' % (errorStatus.prettyPrint(),
                                    errorIndex and varBinds[int(errorIndex)-1][0] or '?'))
                break
            else:
                for varBind in varBinds:
                    #print(varBind)
                    x = '='.join([x.prettyPrint() for x in varBind])
                    x = x.split('=')[1]
                    l2.append(x)
            l.append(l2)
        #print(l)
        #print(len(l))
        for (errorIndication,
             errorStatus,
             errorIndex,
             varBinds) in nextCmd(SnmpEngine(),
                                  CommunityData(community_string, mpModel=0),
                                  UdpTransportTarget((ip, 161)),
                                  ContextData(),
                                  ObjectType(ObjectIdentity('IF-MIB', 'ifInOctets')),
                                  lexicographicMode=False):
            l22 = []
            if errorIndication:
                print(errorIndication)
                break
            elif errorStatus:
                print('%s at %s' % (errorStatus.prettyPrint(),
                                    errorIndex and varBinds[int(errorIndex)-1][0] or '?'))
                break
            else:
                for varBind in varBinds:
                    #print(varBind)
                    x = '='.join([x.prettyPrint() for x in varBind])
                    y= x.split('=')[1]
                    z = x.split('=')[0]
                    t = time.time()
                    l22.append(y)
                    l22.append(z)
                    l22.append(t)
            l1.append(l22)
            
        #print(l1)
        #print(len(l1))
        def mergelst(l,l1):
            f_lst = []
            for i in l:
                port_number_l = i[1]
                for j in l1:
                    port_number_l1 = j[1].split('.')[1]
                    if port_number_l1==port_number_l:
                        i.append(j[0])
                        i.append(j[2])
                        f_lst.append(i)
            return f_lst
            
        walk2 = mergelst(l,l1)
        if len(walk1)==0:
            avg=0
            dct=[]
            return avg,dct,walk2
        else:
            t_diff = list(map(lambda x, y: y[4]-x[4], walk1, walk2))
            #print(t_diff)
            #print(len(t_diff))
            def process(i,j,k):
                secoct = j[3]
                i.append(secoct)
                i.append(k)
                return i
            oct_merge = list(map(lambda i,j,k: process(i,j,k),walk1,walk2,t_diff))
            #print(oct_merge)
            #print(len(oct_merge))
            def util(lst):
                try:
                    name = lst[0]
                    octold = int(lst[3])
                    octnew= int(lst[5])
                    speed = int(lst[2])
                    dt = float(lst[6])
                    doct = octnew-octold
                    if doct<0:
                        octnew = octnew+4294967296
                        doct = octnew-octold
                    print(doct)
                    n = doct*8*100
                    d = dt*speed
                    ut = (n/d)
                    ut = round(ut,4)
                    return ut
                except Exception as e:
                    print(e)
                    return 0
            result = list(map(lambda i: util(i),oct_merge))
            #print(x)
            print('------------------------------------------------------------')
            #print(len(result))
            port_name = [xs[0] for xs in walk1]
            port_name_result = all(element == port_name[0] for element in port_name)
            if port_name_result:
                port_number = [xs[1] for xs in walk1]
                x = []
                for k in range(len(port_number)):
                    tem_dict = {}
                    tem_dict["PortName"]=port_number[k]
                    tem_dict["Utilization"] = result[k] 
                    #kk = dict(port_number[k],result[k])
                    x.append(tem_dict)
                #x = dict(zip(port_number,result))
                #print(len(port_name))
            else:
                x=[]
                for k in range(len(port_name)):
                    tem_dict = {}
                    tem_dict["PortName"]=port_name[k]
                    tem_dict["Utilization"] = result[k] 
                    #kk = dict(port_name[k],result[k])
                    x.append(tem_dict)
                #x = dict(zip(port_name,result))
            #print(x)
            average = Average(result)
            walk1 = walk2
            return average,x,walk1
    except Exception as e:
        logging.error(e, exc_info=True)
        average = 0
        walk1=[]
        x=[]
        return average,x,walk1