from pysnmp.hlapi import *

def switch_interfaces(ip):
    try:
        count = 0
        count_up=0
        count_down=0
        test_list = ["Gi","gi","Po"]
        for (errorIndication,
             errorStatus,
             errorIndex,
             varBinds) in nextCmd(SnmpEngine(),
                                  CommunityData('public', mpModel=0),
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

def switch_sysUpTime(ip):
    try:
        errorIndication, errorStatus, errorIndex, varBinds = next(
        getCmd(SnmpEngine(),
               CommunityData('public', mpModel=0),
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

    
def switch_name(ip):
    try:
        errorIndication, errorStatus, errorIndex, varBinds = next(
        getCmd(SnmpEngine(),
               CommunityData('public', mpModel=0),
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
                x = x.split(",")[0]
                x = x.split("=")[1]
            return x
    except:
        return None
    
def last_change(ip): 
    try:
        change=[]
        for (errorIndication,
             errorStatus,
             errorIndex,
             varBinds) in nextCmd(SnmpEngine(),
                                  CommunityData('public', mpModel=0),
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
        min_change = min(change)
        return int(min_change)
    
    except Exception as e:
        logging.error(e, exc_info=True)
        return None

def maximum_speed(ip): 
    try:
        speed=[]
        for (errorIndication,
             errorStatus,
             errorIndex,
             varBinds) in nextCmd(SnmpEngine(),
                                  CommunityData('public', mpModel=0),
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
        max_speed = max(speed)
        return int(max_speed)

    except Exception as e:
        logging.error(e, exc_info=True)
        return None

def Dropped_packedt(ip): 
    try:
        dis_packet=[]
        for (errorIndication,
             errorStatus,
             errorIndex,
             varBinds) in nextCmd(SnmpEngine(),
                                  CommunityData('public', mpModel=0),
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
    
def error_packedt(ip): 
    try:
        err_packet=[]
        for (errorIndication,
             errorStatus,
             errorIndex,
             varBinds) in nextCmd(SnmpEngine(),
                                  CommunityData('public', mpModel=0),
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
    num_of_interfaces,num_of_uplink,num_of_downlink = switch_interfaces(ip)
    s_name = switch_name(ip)
    s_uptime = switch_sysUpTime(ip)
    num_of_packet_dropped = Dropped_packedt(ip)
    num_of_packet_error = error_packedt(ip)
    max_speed = maximum_speed(ip)
    l_change = last_change(ip)
    return s_name,s_uptime,num_of_interfaces,num_of_uplink,num_of_downlink,l_change,max_speed,num_of_packet_dropped,num_of_packet_error