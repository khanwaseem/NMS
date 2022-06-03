def insert_into_ecb_dowtime(down_time,i,j,last_status,START_TIME,END_TIME,ip_add,conn,Tran_id):
    try:
        cur = conn.cursor()
        cur.execute("insert into ATMS_ECB_DOWN_RECORD (TRAN_ID,DEV_ID,IP,END_TIME,STATUS,EQUIP_ID,DOWN,START_TIME) values (:1, :2, :3, :4, :5, :6, :7, :8)", (Tran_id,j,ip_add,END_TIME,last_status,i,down_time,START_TIME))
        conn.commit()
        #conn.close()
        logging.info('Data Inserted %s'%ip_add)
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
     
        
        
def update_ECB_Downtime(Tran_id,START_TIME,END_TIME,down,conn):
    try:
        START_TIME = START_TIME.strftime('%d/%m/%Y %H:%M:%S')
        END_TIME = END_TIME.strftime('%d/%m/%Y %H:%M:%S')
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        query = ("update DOWN_TIME_RECORD Set START_TIME=TO_DATE ('{0}','DD/MM/YYYY HH24:MI:SS'), END_TIME=TO_DATE ('{1}','DD/MM/YYYY HH24:MI:SS'),DOWN={2} where TRAN_ID={3}".format(START_TIME,END_TIME,down,Tran_id))
        cur.execute(query)
        conn.commit()
        #conn.close()
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        
        
def test_fun(df):
    try:
        print(df.dtypes)
        df['DEV_ID'] = df['DEV_ID'].astype(str).astype(int)
        df1 = df.loc[df['DEV_ID'] %2==0]
        #print(df1.head(10))
        count_row = df1.shape[0]
        if count_row==3:
            actual_down = df1['DOWN'].min()
            print(actual_down)
            min_row = df1[df1['DOWN']==actual_down]
            for index1, row1 in min_row.iterrows():
                start_tm = row1[7]
                end_tm = row1[3]
                print(start_tm,end_tm,actual_down)
            df1['DOWN'] = actual_down
            df1['END_TIME'] =end_tm
            df1['START_TIME'] = start_tm
        
        print("---------------------------")
        df2 = df.loc[df['DEV_ID'] %2==1]
        #print(df2.head(10))
        count_row = df2.shape[0]
        if count_row==3:
            count_row = df2.shape[0]
            actual_down = df2['DOWN'].min()
            print(actual_down)
            min_row2 = df2[df2['DOWN']==actual_down]
            for index2, row2 in min_row2.iterrows():
                start_tm = row2[7]
                end_tm = row2[3]
                print(start_tm,end_tm,actual_down)
            df2['DOWN'] = actual_down
            df2['END_TIME'] =end_tm
            df2['START_TIME'] = start_tm
        df_combine = pd.concat([df1, df2], ignore_index=True)
        df_combine['DEV_ID'] = df_combine['DEV_ID'].apply(str)
        df['DEV_ID'] = df['DEV_ID'].apply(str)
        #print(df_combine.dtypes)
        return df_combine
    except Exception as e:
        df['DEV_ID'] = df['DEV_ID'].apply(str)
        logging.error("Exception occurred", exc_info=True)
        return df
    
def NMS_Process(df,device_list,equip_id):
    col = df.columns.to_list()
    col.append("Taken")
    final_df = pd.DataFrame(columns=col)
    list_of_df = [g for _, g in df.groupby([df['END_TIME'].dt.date])]       
    df = None
    #print(final_df)
    if equip_id==2:
        for df in list_of_df:
            if len(device_list)==0:
                device_list = df['DEV_ID'].tolist()
                device_list = list(set(device_list))
                for m in range(0, len(device_list)): 
                    device_list[m] = int(device_list[m])
            for i in device_list:
                df3 = df[df['DEV_ID']=="%s"%i]
                df2 = df[df['DEV_ID']!="%s"%i]
                #k=i+1
                #df2 = df[df['DEV_ID']=="%s"%k]
                #k=i+2
                #df_temp = df[df['DEV_ID']=="%s"%k]
                #df2 = pd.concat([df2, df_temp], ignore_index=True)
                #k=i-1
                #df_temp = df[df['DEV_ID']=="%s"%k]
                #df2 = pd.concat([df2, df_temp], ignore_index=True)
                #k = i-2
                #df_temp = df[df['DEV_ID']=="%s"%k]
                #df2 = pd.concat([df2, df_temp], ignore_index=True)

                lst2 = []
                for index, row in df3.iterrows():
                    lst= []
                    tt = DateTimeRange(row['START_TIME'], row['END_TIME'])
                    for index1, row1 in df2.iterrows():
                        time_range = DateTimeRange(row1['START_TIME'], row1['END_TIME'])
                        t_diff = time_range.intersection(tt)
                        t_diff = str(t_diff)
                        t2 = t_diff.split(" ")[2]
                        t1 = t_diff.split(" ")[0]
                        t1 = t1.replace("T", " ", 1)
                        t2 = t2.replace("T", " ", 1)
                        if t1 != "Na ":
                            t1 = datetime.strptime(t1, '%Y-%m-%d %H:%M:%S')
                            t2 = datetime.strptime(t2, '%Y-%m-%d %H:%M:%S')
                            t_difference = t2-t1
                            #print(t_difference)
                            tdiff = t_difference.total_seconds() / 60
                            if tdiff>480:
                                lst.append(1)
                            else:
                                lst.append(0)
                        else:
                            lst.append(0)
                            
                            
                    df_2_copy = None
                    
                    df_2_copy = df2.copy()
                    df_2_copy["Three_Con"] = lst
                    #print(df2,df_2_copy)
                    df_con = df_2_copy[df_2_copy['Three_Con']==1]
                    df_conv = df_con.sort_values('DEV_ID')
                    #print(df_conv)
                    conv_list = df_conv['DEV_ID'].to_list()
                    for n in range(0, len(conv_list)): 
                        conv_list[n] = int(conv_list[n])
                    if test(conv_list,i)==True:
                        lst2.append(1)
                    else:
                        lst2.append(0)

                if len(lst2)!=0:
                    df3["Taken"] = lst2
                    final_df = pd.concat([final_df, df3], ignore_index=True)
        final_df = final_df[final_df['Taken']==1]
        del final_df['Taken']
        send_frame = test_fun(final_df)
        return send_frame
    
    
def read_data(equip_id,device_id,start_date,to_date):
    try:
        conn = cx_Oracle.connect('%s'%login_parameter)
        #print(type(equip_id),type(device_id))
        device_list=[]
        if device_id!=0:
            device_list.append(device_id)
        if device_id==0:
            if equip_id==2:
                device_list=[]
        #print(start_date,to_date)
        query = ("select * from DOWN_TIME_RECORD where END_TIME between to_date('%s') and to_date('%s') and DOWN>=480 and EQUIP_ID='%s'" %(start_date,to_date,equip_id))
        df = pd.read_sql(query, con=conn)
        conn.close()
        return df,device_list
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        
        
def test(c_list,value):
    if (value-2) in c_list and (value+2) in c_list:
        return True
    if (value+2) in c_list and (value+4) in c_list:
        return True
    if (value-2) in c_list and (value-4) in c_list:
        return True
    return False
    

    
def ECB_Report():
    try:
        #days_process = flask_port
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.strftime('%d/%b/%y')
        to_date = datetime.now()
        to_date = to_date.strftime('%d/%b/%y')
        #print(start_date,type(start_date))
        equip_id=2
        device_id=0 
        df,device_list = read_data(equip_id,device_id,start_date,to_date)
        df['DEV_ID_2'] = df.DEV_ID.astype(int)
        df_odd = df[df['DEV_ID_2']%2==1]
        del df_odd['DEV_ID_2']
        df_even = df[df['DEV_ID_2']%2==0]
        del df_even['DEV_ID_2']
        #print(df.head(100))
        #print(df.shape)
        #print(df_odd.shape,df_even.shape)
        #print(df.dtypes)
        received_df_even = NMS_Process(df_even,device_list,equip_id)
        conn = cx_Oracle.connect('%s'%login_parameter)
        for ind, row in received_df_even.iterrows():
            i = row[5]
            j = row[1]
            down_time=row[6]
            last_status = row[4]
            ip_add= row[2]
            START_TIME = row[7]
            END_TIME  =row[3]
            Tran_id = row[0]
            insert_into_ecb_dowtime(down_time,i,j,last_status,START_TIME,END_TIME,ip_add,conn,Tran_id)
            update_ECB_Downtime(Tran_id,START_TIME,END_TIME,down_time,conn)
            #Tran_id+=1
        #time.sleep(200)
        received_df_odd = NMS_Process(df_odd,device_list,equip_id)
        #conn = cx_Oracle.connect('%s'%login_parameter)
        for ind, row in received_df_odd.iterrows():
            i = row[5]
            j = row[1]
            down_time=row[6]
            last_status = row[4]
            ip_add= row[2]
            START_TIME = row[7]
            END_TIME  =row[3]
            Tran_id = row[0]
            insert_into_ecb_dowtime(down_time,i,j,last_status,START_TIME,END_TIME,ip_add,conn,Tran_id)
            update_ECB_Downtime(Tran_id,START_TIME,END_TIME,down_time,conn)
            #Tran_id+=1
        conn.close()
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
    
# def ECB_Report():
#     try:
#         yesterday = datetime.now() - timedelta(days=1)
#         start_date = yesterday.strftime('%d/%b/%y')
#         to_date = datetime.now()
#         to_date = to_date.strftime('%d/%b/%y')
#         print(start_date,type(start_date))
#         equip_id=2
#         device_id=0 
#         df,device_list = read_data(equip_id,device_id,start_date,to_date)
#         received_df = NMS_Process(df,device_list,equip_id)
#         conn = cx_Oracle.connect('%s'%login_parameter)
#         for ind, row in received_df.iterrows():
#             i = row[5]
#             j = row[1]
#             down_time=row[6]
#             last_status = row[4]
#             ip_add= row[2]
#             START_TIME = row[7]
#             END_TIME  =row[3]
#             Tran_id = row[0]
#             insert_into_ecb_dowtime(down_time,i,j,last_status,START_TIME,END_TIME,ip_add,conn,Tran_id)
#             Tran_id+=1
#         conn.close()
        
#     except Exception as e:
#         logging.error("Exception occurred", exc_info=True)

def pings(hostname):
    batcmd="ping -n 1 " + hostname
    try:
        result = subprocess.check_output(batcmd, shell=True)
        if b'unreachable' in result or b'timed out' in result:
            return False
        else:
            return True
    except:
        return False
    
def ATMS_Test(ip):
    try:
        port = "5510"
        conn = telnetlib.Telnet(ip,port)
        response = True
    except:
        response = False
    finally:
        return response
    
def insert_into_database(ID,DEV_id,ip_address,time,status,EQ_ID,conn):
    try:
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("insert into ATMS.TBL_UP_DOWN (TRAN_ID,DEV_ID,IP,STATUS_TIME,STATUS,EQUIP_ID) values (:1, :2, :3, :4, :5, :6)", (ID,DEV_id,ip_address,time,status,EQ_ID))
        conn.commit()
        #conn.close()
    except Exception as e:
        logging.error(e, exc_info=True)

        
def insert_into_database_2(ID,dev_id,ip_address,end_time,status,EQ_ID,down,start_time,conn):
    try:
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("insert into ATMS.DOWN_TIME_RECORD (TRAN_ID,DEV_ID,IP,END_TIME,STATUS,EQUIP_ID,DOWN,START_TIME) values (:1, :2, :3, :4, :5, :6, :7, :8)", (ID,dev_id,ip_address,end_time,status,EQ_ID,down,start_time))
        conn.commit()
        logging.info('Data Inserted %s'%ip_address)
        #conn.close()
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

        
def insert_into_database_CR(ID,DEV_id,ip_address,time,status,EQ_ID,conn):
    try:
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("insert into ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS (TRAN_ID,DEV_ID,IP,STATUS_TIME,STATUS,EQUIP_ID) values (:1, :2, :3, :4, :5, :6)", (ID,DEV_id,ip_address,time,status,EQ_ID))
        conn.commit()
        #conn.close()
    except Exception as e:
        logging.error(e, exc_info=True)

        
def insert_into_database_2_CR(ID,dev_id,ip_address,end_time,status,EQ_ID,down,start_time,conn):
    try:
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("insert into ATMS.ATMS_CR_ASSET_DOWN_TIME (TRAN_ID,DEV_ID,IP,END_TIME,STATUS,EQUIP_ID,DOWN,START_TIME) values (:1, :2, :3, :4, :5, :6, :7, :8)", (ID,dev_id,ip_address,end_time,status,EQ_ID,down,start_time))
        conn.commit()
        logging.info('Data Inserted %s'%ip_address)
        #conn.close()
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        
def update_into_database_CR(dev_id,update_time,EQ_ID,conn):
    try:
        update_time_insert = update_time.strftime('%d/%m/%Y %H:%M:%S')
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        query = ("update ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS Set STATUS_TIME=TO_DATE ('{0}','DD/MM/YYYY HH24:MI:SS') where DEV_ID={1} AND EQUIP_ID={2}".format(update_time_insert,dev_id,EQ_ID))
        cur.execute(query)
        conn.commit()
        #conn.close()
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
    
    
def insert_switch_info(sr_no,switch_id,switch_status,model,uptime,no_interface,up_L,down_L,last_mod,max_speed,drop_p,error_p,s_time,conn):
    try:
        
        cur = conn.cursor()
        cur.execute("insert into ATMS.ATMS_SWITCH_STATUS (SR_NO,SWITCH_ID,SWITCH_STATUS,SWITCH_MODEL,UPTIME_MINUTES,NO_OF_INTERFACE,NO_OF_UPLINK,NO_OF_DOWNLINK,LAST_MODIFY,MAX_SPEED,NO_OF_DROPED_PACKET,NO_OF_ERROR_PACKET,STATUS_TIME) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12,: 13)", (sr_no,switch_id,switch_status,model,uptime,no_interface,up_L,down_L,last_mod,max_speed,drop_p,error_p,s_time))
        conn.commit()
        
    except Exception as e:
        logging.error(e, exc_info=True)
        
        
def insert_into_database_switch(ID,DEV_id,ip_address,time,status,EQ_ID,conn):
    try:
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("insert into ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS (TRAN_ID,DEV_ID,IP,STATUS_TIME,STATUS,EQUIP_ID) values (:1, :2, :3, :4, :5, :6)", (ID,DEV_id,ip_address,time,status,EQ_ID))
        conn.commit()
        #conn.close()
    except Exception as e:
        logging.error(e, exc_info=True)

        
def insert_into_database_2_switch(ID,dev_id,ip_address,end_time,status,EQ_ID,down,start_time,conn):
    try:
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("insert into ATMS.ATMS_CR_ASSET_DOWN_TIME (TRAN_ID,DEV_ID,IP,END_TIME,STATUS,EQUIP_ID,DOWN,START_TIME) values (:1, :2, :3, :4, :5, :6, :7, :8)", (ID,dev_id,ip_address,end_time,status,EQ_ID,down,start_time))
        conn.commit()
        logging.info('Data Inserted %s'%ip_address)
        #conn.close()
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

        
def SNMP_Test(ip):
    try:
        errorIndication, errorStatus, errorIndex, varBinds = next(
        getCmd(SnmpEngine(),
               CommunityData('VaaaN', mpModel=0),
               UdpTransportTarget((ip, 161)),
               ContextData(),
               ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0)))
        )

        if errorIndication:
            print(errorIndication)
            return False
        elif errorStatus:
            print('%s at %s' % (errorStatus.prettyPrint(),
                            errorIndex and varBinds[int(errorIndex) - 1][0] or '?'))
            return False
        else:
            for varBind in varBinds:
                x =' = '.join([x.prettyPrint() for x in varBind])
                x = x.split("=")[1]
            if len(x)>0:
                return True
            else:
                return False
    except:
        return False
    

def SWITCH_ACTION(SWITCH_List):
    global Tran_id_switch
    global Tran_id_CR
    global Tran_id_cr_up_down
    try:
        Equip_ID = 14
        factors = (60, 1, 1/60)
        conn = cx_Oracle.connect('%s'%login_parameter)
        for i in SWITCH_List:
            last_status = None
            try:
                cur = conn.cursor()
                query = ("select * from (select STATUS,STATUS_TIME from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID='%d' AND EQUIP_ID=14 order by TRAN_ID desc) where rownum =1"%i)
                cur.execute(query)
                for row in cur:
                    last_status = row[0]
                    last_status_time = row[1]
                    #print(last_status)
                    #print(last_status)
                #conn.close()
                if last_status is None:
                    last_status = 0
                    last_status_time = 0

            except Exception as e:
                logging.error(e, exc_info=True)

            try:
                ip = SWITCH_DICT[i]
                SNMP_status = SNMP_Test(ip)
            except Exception as e:
                logging.error(e, exc_info=True)
            print(SNMP_status)
            
            if SNMP_status==True:
                model,uptime,no_interface,up_L,down_L,last_mod,max_speed,drop_p,error_p  = sf.switch_detail(ip)
                s_time = datetime.now()
                sr_no =Tran_id_switch
                switch_id=i
                switch_status =1
                insert_switch_info(sr_no,switch_id,switch_status,model,uptime,no_interface,up_L,down_L,last_mod,max_speed,drop_p,error_p,s_time,conn)
                Tran_id_switch+=1
            if SNMP_status==True and last_status==0:
                print("11111")
                current_status =1
                try:
                    time = datetime.now()
                    if last_status_time!=0:
                        today_time = datetime.now()
                        status_Date = today_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = last_status_time.strftime('%Y-%m-%d')
                        if last_status_Date==status_Date_check:
                            #last_status_time_cal =  last_status_time.strftime('%H:%M:%S')
                            try:
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = int(total_datwise_s)
                                #t1 = sum(p*q for p, q in zip(map(int, last_status_time_cal.split(':')), factors))
                                #print("t1",t1)
                                #last_status_time_t1 = t1%1440
                                #total_datwise_sum = int(last_status_time_t1)
                                #print(i,j,"down",down_time)
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        if last_status_Date!=status_Date_check:
                            try:
                                today_p_start = datetime.now()
                                status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = total_datwise_s%1440
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                    if last_status_time==0:
                        total_datwise_sum = 0
                        last_status_time = datetime.now()
                    insert_into_database_switch(Tran_id_CR,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id_CR+=1
                    if total_datwise_sum>5:
                        insert_into_database_2_switch(Tran_id_cr_up_down,i,ip,time,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                        Tran_id_cr_up_down+=1
                except Exception as e:
                    logging.error(e, exc_info=True)

            if SNMP_status==True and last_status==1:
                print("22222")
                continue

            if SNMP_status==False and last_status==0:
                print("33333")
                exist = False
                current_status = 0
                time = datetime.now()
                try:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID='%d' AND EQUIP_ID=14 order by TRAN_ID desc) where rownum =1"%i)
                    cur.execute(query)
                    for row in cur:
                        exist = True
                    if exist==False:
                        insert_into_database_switch(Tran_id_CR,i,ip,time,current_status,Equip_ID,conn)
                        Tran_id_CR+=1
                        #last_status = 0
                        #last_status_time = 0
                    #conn.close()
                except Exception as e:
                    logging.error(e, exc_info=True)
                continue

            if SNMP_status==False and last_status==1:
                print("44444")
                current_status=0
                time = datetime.now()
                try:
                    time = datetime.now()
                    insert_into_database_switch(Tran_id_CR,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id_CR+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
        conn.close()
        
    except Exception as e:
        logging.error(e, exc_info=True)


def ECB_ACTION(ECB_list):
    global Tran_id
    global Tran_id_up_down
    ACTIVE_LIST=[]
    try:
        Equip_ID = 2
        try:
            ACTIVE_LIST = es.read_ecb_db() 
        except Exception as e:
            logging.error(e, exc_info=True)
        #print(ACTIVE_LIST)
        try:
            conn = cx_Oracle.connect('%s'%login_parameter)
        except Exception as e:
            logging.error(e, exc_info=True)
        for i in ECB_list:
            last_status = None
            try:
                #conn = cx_Oracle.connect('%s'%login_parameter)
                cur = conn.cursor()
                query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=2 order by TRAN_ID desc) where rownum =1"%i)
                cur.execute(query)
                for row in cur:
                    last_status = row[0]
                    last_status_time = row[1]
                if last_status is None:
                    last_status = 0
                    last_status_time = 0

            except Exception as e:
                logging.error(e, exc_info=True)

            try:
                ip = ECB_DICT_MAP[i]
                #ping_status = pings(ip)
                #ip2 = ECB_DICT_MAP[i]
            except Exception as e:
                logging.error(e, exc_info=True)

            if ip in ACTIVE_LIST and last_status==1:
                continue

            if ip in ACTIVE_LIST and last_status==0:
                #print("hello")
                current_status =1
                try:
                    time = datetime.now()
                    if last_status_time!=0:
                        today_time = datetime.now()
                        status_Date = today_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = last_status_time.strftime('%Y-%m-%d')
                        if last_status_Date==status_Date_check:
                            #last_status_time_cal =  last_status_time.strftime('%H:%M:%S')
                            try:
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = int(total_datwise_s)
                                #t1 = sum(p*q for p, q in zip(map(int, last_status_time_cal.split(':')), factors))
                                #print("t1",t1)
                                #last_status_time_t1 = t1%1440
                                #total_datwise_sum = int(last_status_time_t1)
                                #print(i,j,"down",down_time)
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        if last_status_Date!=status_Date_check:
                            try:
                                today_p_start = datetime.now()
                                status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = total_datwise_s%1440
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                    if last_status_time==0:
                        total_datwise_sum = 0
                        last_status_time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                    if total_datwise_sum>5:
                        insert_into_database_2(Tran_id_up_down,i,ip,time,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                        Tran_id_up_down+=1
                except Exception as e:
                    logging.error(e, exc_info=True)


            if ip not in ACTIVE_LIST and last_status==0:
                exist = False
                current_status = 0
                time_1 = datetime.now()
                try:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor() 
                    query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=2 order by TRAN_ID desc) where rownum =1"%i)
                    cur.execute(query)
                    for row in cur:
                        exist = True
                    #conn.close()
                    if exist==False:
                        insert_into_database(Tran_id,i,ip,time_1,current_status,Equip_ID,conn)
                        Tran_id+=1
                        #last_status = 0
                        #last_status_time = 0
                except Exception as e:
                    logging.error(e, exc_info=True)
                #continue

            if ip not in ACTIVE_LIST and last_status==1:
                current_status = 0
                try:
                    time_1 = datetime.now()
                    insert_into_database(Tran_id,i,ip,time_1,current_status,Equip_ID,conn)
                    Tran_id+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
        conn.close()
        
    except Exception as e:
        logging.error(e, exc_info=True)


def CONTROL_ROOM_ASSET_ACTION(CR_ASSET_list):
    global Tran_id_CR
    global Tran_id_cr_up_down
    try:
        conn = cx_Oracle.connect('%s'%login_parameter)
        factors = (60, 1, 1/60)
        for i in CR_ASSET_list:
            Equip_ID = CR_ASSET_DICT_MAP[i]
            #time.sleep(1)
            last_status = None
            #print("-------------------------------------------------------")
            try:
                cur = conn.cursor()
                query = ("select * from (select STATUS,STATUS_TIME from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID={0} AND EQUIP_ID={1} order by TRAN_ID desc) where rownum =1".format(i,Equip_ID))
                cur.execute(query)
                for row in cur:
                    last_status = row[0]
                    last_status_time = row[1]

                #conn.close()
                if last_status is None:
                    last_status = 0
                    last_status_time = 0

            except Exception as e:
                logging.error("Exception occurred", exc_info=True)
                #last_status = 0
                #last_status_time = 0

            ip = CR_ASSET_DICT[i] 
            #print(ip)
            #print(last_status,last_status_time,ip)
            ping_status = pings(ip)
            #print(ip,ping_status)
            if ping_status==True and last_status==0:
                #print("1",ip,last_status)
                current_status = 1
                try:
                    time_1 = datetime.now()
                    if last_status_time!=0:
                        today_time = datetime.now()
                        status_Date = today_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = last_status_time.strftime('%Y-%m-%d')

                        if last_status_Date==status_Date_check:
                            #last_status_time_cal =  last_status_time.strftime('%H:%M:%S')
                            try:
                                down_time_cal = time_1-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = int(total_datwise_s)
                                #t1 = sum(p*q for p, q in zip(map(int, last_status_time_cal.split(':')), factors))
                                #last_status_time_t1 = t1%1440
                                #total_datwise_sum = int(last_status_time_t1)
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        if last_status_Date!=status_Date_check:
                            try:
                                today_p_start = datetime.now()
                                status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                                down_time_cal = time_1-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = total_datwise_s%1440
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)
                        #down_time_cal = time_1-last_status_time
                        #total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                        #total_datwise_sum = total_datwise_s%1440
                    if last_status_time==0:
                        total_datwise_sum = 0
                        last_status_time = datetime.now()

                    #print("1",ip,last_status,last_status_time,total_datwise_sum)
                    insert_into_database_CR(Tran_id_CR,i,ip,time_1,current_status,Equip_ID,conn)
                    Tran_id_CR+=1
                    if total_datwise_sum>5:
                        insert_into_database_2_CR(Tran_id_cr_up_down,i,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                        Tran_id_cr_up_down+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
                continue

            if ping_status==True and last_status==1:
                #print("2",ip,last_status)
                continue

            if ping_status==False and last_status==0:
                exist = False
                current_status = 0
                time_1 = datetime.now()
                try:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID={0} AND EQUIP_ID={1} order by TRAN_ID desc) where rownum =1".format(i,Equip_ID))
                    cur.execute(query)
                    for row in cur:
                        exist = True
                    #conn.close()
                    if exist==False:
                        insert_into_database_CR(Tran_id_CR,i,ip,time_1,current_status,Equip_ID,conn)
                        Tran_id_CR+=1
                        #last_status = 0
                        #last_status_time = 0
                except Exception as e:
                    logging.error(e, exc_info=True)
                #print("3",ip,last_status)
                continue

            if ping_status==False and last_status==1:
                current_status = 0
                time_1 = datetime.now()
                try:
                    #print("4",ip,last_status)
                    insert_into_database_CR(Tran_id_CR,i,ip,time_1,current_status,Equip_ID,conn)
                    Tran_id_CR+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
        conn.close()
    except Exception as e:
        logging.error(e, exc_info=True)

    
def CCTV_ACTION(CCTV_list):
    global Tran_id
    global Tran_id_up_down
    try:
        Equip_ID = 1
        factors = (60, 1, 1/60)
        conn = cx_Oracle.connect('%s'%login_parameter)
        for i in CCTV_list:
            last_status = None
            try:
                cur = conn.cursor()
                query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=1 order by TRAN_ID desc) where rownum =1"%i)
                cur.execute(query)
                for row in cur:
                    last_status = row[0]
                    last_status_time = row[1]
                    #print(last_status)
                if last_status is None:
                    last_status = 0
                    last_status_time = 0
                #conn.close()
            except Exception as e:
                logging.error("Exception occurred", exc_info=True)
                #last_status = 0
                #last_status_time = datetime.now()

            ip = CCTV_DICT[i]
            #print(last_status,ip) 
            #print(i,ip)
            try:
                cap = cv2.VideoCapture("rtsp://admin:admin@123@%s"%ip)
                ret,frame = cap.read()
            except Exception as e:
                logging.error(e, exc_info=True)


            if frame is not None and last_status==0:
                #print("hello")
                current_status =1
                try:
                    time = datetime.now()
                    if last_status_time!=0:
                        today_time = datetime.now()
                        status_Date = today_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = last_status_time.strftime('%Y-%m-%d')

                        if last_status_Date==status_Date_check:
                            #last_status_time_cal =  last_status_time.strftime('%H:%M:%S')
                            try:
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = int(total_datwise_s)
                                #t1 = sum(p*q for p, q in zip(map(int, last_status_time_cal.split(':')), factors))
                                #print("t1",t1)
                                #last_status_time_t1 = t1%1440
                                #total_datwise_sum = int(last_status_time_t1)
                                #print(i,j,"down",down_time)
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        if last_status_Date!=status_Date_check:
                            try:
                                today_p_start = datetime.now()
                                status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = total_datwise_s%1440
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        #down_time_cal = time-last_status_time
                        #print(down_time_cal,time,last_status_time)
                        #total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                        #print(type(total_datwise_sum))
                        #total_datwise_sum = total_datwise_s%1440
                    if last_status_time==0:
                        total_datwise_sum = 0
                        last_status_time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                    if total_datwise_sum>5:
                        insert_into_database_2(Tran_id_up_down,i,ip,time,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                        Tran_id_up_down+=1
                except Exception as e:
                    logging.error(e, exc_info=True)

                #insert_into_database(i,ip,current_time,status)

            if frame is not None and last_status==1:
                continue

            if frame is None and last_status==0:
                exist = False
                current_status = 0
                time = datetime.now()
                try:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=1 order by TRAN_ID desc) where rownum =1"%i)
                    cur.execute(query)
                    for row in cur:
                        exist = True
                    if exist==False:
                        insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                        Tran_id+=1
                        #last_status = 0
                        #last_status_time = 0
                    #conn.close()
                except Exception as e:
                    logging.error(e, exc_info=True)
                continue

            if frame is None and last_status==1:
                current_status=0
                time = datetime.now()
                try:
                    time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
        conn.close()
        
    except Exception as e:
        logging.error(e, exc_info=True)
    
            

def ATMS_ACTION(ATMS_list):
    global Tran_id_CR
    global Tran_id_cr_up_down
    try:
        conn = cx_Oracle.connect('%s'%login_parameter)
        factors = (60, 1, 1/60)
        for i in ATMS_list:
            Equip_ID = ATMS_DICT_MAP[i]
            #time.sleep(1)
            last_status = None
            #print("----------------------------------------------------------------")
            try:
                cur = conn.cursor()
                query = ("select * from (select STATUS,STATUS_TIME from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID={0} AND EQUIP_ID={1} order by TRAN_ID desc) where rownum =1".format(i,Equip_ID))
                cur.execute(query)
                for row in cur:
                    last_status = row[0]
                    last_status_time = row[1]

                #conn.close()
                if last_status is None:
                    last_status = 0
                    last_status_time = 0

            except Exception as e:
                logging.error("Exception occurred", exc_info=True)
                #last_status = 0
                #last_status_time = 0

            ip = ATMS_DICT[i] 
            #print(ip)
            #print(last_status,last_status_time,ip)
            atms_status = ATMS_Test(ip)
            #print(ip,ping_status)
            if atms_status==True and last_status==0:
                #print("1",ip,last_status)
                current_status = 1
                try:
                    time_1 = datetime.now()
                    if last_status_time!=0:
                        today_time = datetime.now()
                        status_Date = today_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = last_status_time.strftime('%Y-%m-%d')

                        if last_status_Date==status_Date_check:
                            #last_status_time_cal =  last_status_time.strftime('%H:%M:%S')
                            try:
                                down_time_cal = time_1-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = int(total_datwise_s)
                                #t1 = sum(p*q for p, q in zip(map(int, last_status_time_cal.split(':')), factors))
                                #last_status_time_t1 = t1%1440
                                #total_datwise_sum = int(last_status_time_t1)
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        if last_status_Date!=status_Date_check:
                            try:
                                today_p_start = datetime.now()
                                status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                                down_time_cal = time_1-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = total_datwise_s%1440
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)
                        #down_time_cal = time_1-last_status_time
                        #total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                        #total_datwise_sum = total_datwise_s%1440
                    if last_status_time==0:
                        total_datwise_sum = 0
                        last_status_time = datetime.now()

                    #print("1",ip,last_status,last_status_time,total_datwise_sum)
                    insert_into_database_CR(Tran_id_CR,i,ip,time_1,current_status,Equip_ID,conn)
                    Tran_id_CR+=1
                    if total_datwise_sum>5:
                        insert_into_database_2_CR(Tran_id_cr_up_down,i,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                        Tran_id_cr_up_down+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
                continue

            if atms_status==True and last_status==1:
                #print("2",ip,last_status)
                continue

            if atms_status==False and last_status==0:
                exist = False
                current_status = 0
                time_1 = datetime.now()
                try:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID={0} AND EQUIP_ID={1} order by TRAN_ID desc) where rownum =1".format(i,Equip_ID))
                    cur.execute(query)
                    for row in cur:
                        exist = True
                    #conn.close()
                    if exist==False:
                        insert_into_database_CR(Tran_id_CR,i,ip,time_1,current_status,Equip_ID,conn)
                        Tran_id_CR+=1
                        #last_status = 0
                        #last_status_time = 0
                except Exception as e:
                    logging.error(e, exc_info=True)
                #print("3",ip,last_status)
                continue

            if atms_status==False and last_status==1:
                current_status = 0
                time_1 = datetime.now()
                try:
                    #print("4",ip,last_status)
                    insert_into_database_CR(Tran_id_CR,i,ip,time_1,current_status,Equip_ID,conn)
                    Tran_id_CR+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
        conn.close()
    except Exception as e:
        logging.error(e, exc_info=True)


def ATCC_ACTION(ATCC_list):
    global Tran_id
    global Tran_id_up_down
    try:
        Equip_ID = 3
        factors = (60, 1, 1/60)
        try:
            conn = cx_Oracle.connect('%s'%login_parameter)
            cur = conn.cursor()
            query = "select RECEIVE_TIME,ATCC_ID from ATMS.ATMS_ATCC_CURRENT_TRANSACTION where RECEIVE_TIME > sysdate - (15/1440)"
            cur.execute(query)
            ACTIVE_LIST = []
            for row in cur:
                ACTIVE_LIST.append(row[1])
                #print(row)
            #conn.close()
            #print(ACTIVE_LIST)
            #print("ACTIVE_LIST_LENGTH",len(ACTIVE_LIST))
        except Exception as e:
            logging.error(e, exc_info=True)

        for i in ATCC_list:
            last_status = None
            try:
                #conn = cx_Oracle.connect('%s'%login_parameter)
                cur = conn.cursor()
                query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=3 order by TRAN_ID desc) where rownum =1"%i)
                cur.execute(query)
                #print("hello")
                #print(cur)
                for row in cur:
                    last_status = row[0]
                    last_status_time = row[1]
                    #print(last_status)
                    #print(last_status)
                #conn.close()
                if last_status is None:
                    last_status = 0
                    last_status_time = 0

            except Exception as e:
                logging.error(e, exc_info=True)

            try:
                ip = ATCC_DICT[i]
                ping_status = pings(ip)
                #ip2 = ECB_DICT_MAP[i]
            except Exception as e:
                logging.error(e, exc_info=True)
                
            ##if ping_status==True and i in ACTIVE_LIST:
             #   ping_status= True
            #else:
             #   ping_status=False

            if ping_status==True and last_status==0:
                #print("hello")
                current_status =1
                try:
                    time = datetime.now()
                    if last_status_time!=0:
                        today_time = datetime.now()
                        status_Date = today_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = last_status_time.strftime('%Y-%m-%d')
                        if last_status_Date==status_Date_check:
                            #last_status_time_cal =  last_status_time.strftime('%H:%M:%S')
                            try:
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = int(total_datwise_s)
                                #t1 = sum(p*q for p, q in zip(map(int, last_status_time_cal.split(':')), factors))
                                #print("t1",t1)
                                #last_status_time_t1 = t1%1440
                                #total_datwise_sum = int(last_status_time_t1)
                                #print(i,j,"down",down_time)
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        if last_status_Date!=status_Date_check:
                            try:
                                today_p_start = datetime.now()
                                status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = total_datwise_s%1440
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                    if last_status_time==0:
                        total_datwise_sum = 0
                        last_status_time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                    if total_datwise_sum>5:
                        insert_into_database_2(Tran_id_up_down,i,ip,time,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                        Tran_id_up_down+=1
                except Exception as e:
                    logging.error(e, exc_info=True)

            if ping_status==True and last_status==1:
                continue

            if ping_status==False and last_status==0:
                exist = False
                current_status = 0
                time = datetime.now()
                try:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=3 order by TRAN_ID desc) where rownum =1"%i)
                    cur.execute(query)
                    for row in cur:
                        exist = True
                    if exist==False:
                        insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                        Tran_id+=1
                        #last_status = 0
                        #last_status_time = 0
                    #conn.close()
                except Exception as e:
                    logging.error(e, exc_info=True)
                continue

            if ping_status==False and last_status==1:
                current_status=0
                time = datetime.now()
                try:
                    time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
        conn.close()
        
    except Exception as e:
        logging.error(e, exc_info=True)


def ANPR_ACTION(ANPR_list):
    global Tran_id
    global Tran_id_up_down
    try:
        Equip_ID = 4
        previous_ip = None
        factors = (60, 1, 1/60)
        try:
            conn = cx_Oracle.connect('%s'%login_parameter)
            cur = conn.cursor()
            query = "SELECT * FROM (SELECT TO_CHAR (EVENT_TIME, 'MI') MINUTE,TO_CHAR (SYSDATE, 'MI') TODAY_MIN,EVENT_TIME,GANTRY_ID,LANE_ID FROM ATMS_LATEST_ANPR_RAWDATA WHERE     TO_CHAR (EVENT_TIME, 'HH24') = TO_CHAR (SYSDATE, 'HH24'))WHERE TODAY_MIN-MINUTE < 59"
            cur.execute(query)
            ACTIVE_LIST = []
            for row in cur:
                ACTIVE_LIST.append((row[3],row[4]))
                #print(row)
            #conn.close()
            #print(ACTIVE_LIST)
            #print("ACTIVE_LIST_LENGTH",len(ACTIVE_LIST))
        except Exception as e:
            logging.error(e, exc_info=True)

        for i in ANPR_list:
            last_status = None
            #time.sleep(1)
            try:
                #conn = cx_Oracle.connect('%s'%login_parameter)
                cur = conn.cursor()
                query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=4 order by TRAN_ID desc) where rownum =1"%i)
                cur.execute(query)
                #print("hello")
                #print(cur)
                for row in cur:
                    last_status = row[0]
                    last_status_time = row[1]
                    #print(last_status)
                    #print(last_status)
                #conn.close()
                if last_status is None:
                    last_status = 0
                    last_status_time = 0

            except Exception as e:
                logging.error(e, exc_info=True)

            try:
                ip = ANPR_DICT[i]
                gantry_lane_id = ANPR_DICT_MAP[i]
            except Exception as e:
                logging.error(e, exc_info=True)

            if previous_ip is None:
                ping_status = pings(ip)  

            if previous_ip is not None:
                pre_ip,pre_status = previous_ip[0],previous_ip[1]
                if pre_ip==ip:
                    ping_status= pre_status
                if pre_ip!=ip:
                    ping_status = pings(ip) 

            previous_ip = (ip,ping_status)

            if ping_status==True and last_status==0:
                #print("hello")
                current_status =1
                try:
                    time = datetime.now()
                    if last_status_time!=0:
                        today_time = datetime.now()
                        status_Date = today_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = last_status_time.strftime('%Y-%m-%d')
                        if last_status_Date==status_Date_check:
                            #last_status_time_cal =  last_status_time.strftime('%H:%M:%S')
                            try:
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = int(total_datwise_s)
                                #t1 = sum(p*q for p, q in zip(map(int, last_status_time_cal.split(':')), factors))
                                #print("t1",t1)
                                #last_status_time_t1 = t1%1440
                                #total_datwise_sum = int(last_status_time_t1)
                                #print(i,j,"down",down_time)
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        if last_status_Date!=status_Date_check:
                            try:
                                today_p_start = datetime.now()
                                status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = total_datwise_s%1440
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        #down_time_cal = time-last_status_time
                        #print(down_time_cal,time,last_status_time)
                        #total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                        #print(type(total_datwise_sum))
                        #total_datwise_sum = total_datwise_s%1440
                    if last_status_time==0:
                        total_datwise_sum = 0
                        last_status_time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                    if total_datwise_sum>5:
                        insert_into_database_2(Tran_id_up_down,i,ip,time,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                        Tran_id_up_down+=1
                except Exception as e:
                    logging.error(e, exc_info=True)

            if ping_status==True and last_status==1:      
                continue

            if ping_status==False and last_status==0:
                exist = False
                current_status = 0
                time = datetime.now()
                try:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=4 order by TRAN_ID desc) where rownum =1"%i)
                    cur.execute(query)
                    for row in cur:
                        exist = True
                    if exist==False:
                        insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                        Tran_id+=1
                        #last_status = 0
                        #last_status_time = 0
                    #conn.close()
                except Exception as e:
                    logging.error(e, exc_info=True)
                continue

            if ping_status==False and last_status==1:
                current_status=0
                time = datetime.now()
                try:
                    time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
        conn.close()
        
    except Exception as e:
        logging.error(e, exc_info=True)

def RADAR_ACTION(RADAR_list):
    global Tran_id
    global Tran_id_up_down
    try:
        factors = (60, 1, 1/60)
        Equip_ID = 5
        conn = cx_Oracle.connect('%s'%login_parameter)
        try:
            cur = conn.cursor()
            query = "select INCIDENT_TIME,RADAR_ID from ATMS.ATMS_LATEST_RADAR_INCIDENT where INCIDENT_TIME > sysdate - (15/1440)"
            cur.execute(query)
            #print("done")
            ACTIVE_LIST = []
            for row in cur:
                ACTIVE_LIST.append(row[1])
                #print(row)
            #conn.close()
            #print(ACTIVE_LIST)
            #print("ACTIVE_LIST_LENGTH",len(ACTIVE_LIST))
        except Exception as e:
            logging.error(e, exc_info=True)

        for i in RADAR_list:
            last_status = None
            try:
                #conn = cx_Oracle.connect('%s'%login_parameter)
                cur = conn.cursor()
                query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=5 order by TRAN_ID desc) where rownum =1"%i)
                cur.execute(query)
                #print("hello")
                #print(cur)
                for row in cur:
                    print(row)
                    last_status = row[0]
                    last_status_time = row[1]
                    #print(last_status)
                    #print(last_status)
                #conn.close()
                if last_status is None:
                    last_status = 0
                    last_status_time = 0

            except Exception as e:
                logging.error(e, exc_info=True)
            #print(last_status)  
            try:
                ip = RADAR_DICT[i]
                ping_status = pings(ip)
                #ip2 = ECB_DICT_MAP[i]
            except Exception as e:
                logging.error(e, exc_info=True)

            if ping_status==True and last_status==0:
                current_status =1
                try:
                    time = datetime.now()
                    if last_status_time!=0:
                        today_time = datetime.now()
                        status_Date = today_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = last_status_time.strftime('%Y-%m-%d')
                        if last_status_Date==status_Date_check:
                            #last_status_time_cal =  last_status_time.strftime('%H:%M:%S')
                            try:
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = int(total_datwise_s)
                                #t1 = sum(p*q for p, q in zip(map(int, last_status_time_cal.split(':')), factors))
                                #print("t1",t1)
                                #last_status_time_t1 = t1%1440
                                #total_datwise_sum = int(last_status_time_t1)
                                #print(i,j,"down",down_time)
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        if last_status_Date!=status_Date_check:
                            try:
                                today_p_start = datetime.now()
                                status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = total_datwise_s%1440
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)


                    if last_status_time==0:
                        total_datwise_sum = 0
                        last_status_time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                    if total_datwise_sum>5:
                        insert_into_database_2(Tran_id_up_down,i,ip,time,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                        Tran_id_up_down+=1
                except Exception as e:
                    logging.error(e, exc_info=True)

            if ping_status==True and last_status==1:
                continue

            if ping_status==False and last_status==0:
                exist = False
                current_status = 0
                time = datetime.now()
                try:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=5 order by TRAN_ID desc) where rownum =1"%i)
                    cur.execute(query)
                    for row in cur:
                        exist = True
                    if exist==False:
                        insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                        Tran_id+=1
                        #last_status = 0
                        #last_status_time = 0
                    #conn.close()
                except Exception as e:
                    logging.error(e, exc_info=True)
                continue


            if ping_status==False and last_status==1:
                current_status=0
                time = datetime.now()
                try:
                    time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
        conn.close()
        
    except Exception as e:
        logging.error(e, exc_info=True)
    
                    
def VIDS_ACTION(VIDS_list):
    global Tran_id
    global Tran_id_up_down
    try:
        factors = (60, 1, 1/60)
        Equip_ID = 11
        try:
            ACTIVE_LIST = vs.read_vids_db() 
        except Exception as e:
            logging.error(e, exc_info=True)
        conn = cx_Oracle.connect('%s'%login_parameter)
        
        for i in VIDS_list:
            last_status = None
            try:
                cur = conn.cursor()
                query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=11 order by TRAN_ID desc) where rownum =1"%i)
                cur.execute(query)
                #print("hello")
                #print(cur)
                for row in cur:
                    print(row)
                    last_status = row[0]
                    last_status_time = row[1]
                    #print(last_status)
                    #print(last_status)
                #conn.close()
                if last_status is None:
                    last_status = 0
                    last_status_time = 0

            except Exception as e:
                logging.error(e, exc_info=True)
            #print(last_status)  
            try:
                ip = VIDS_DICT[i]
                #ip2 = ECB_DICT_MAP[i]
            except Exception as e:
                logging.error(e, exc_info=True)

            if i in ACTIVE_LIST and last_status==0:
                current_status =1
                try:
                    time = datetime.now()
                    if last_status_time!=0:
                        today_time = datetime.now()
                        status_Date = today_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = last_status_time.strftime('%Y-%m-%d')
                        if last_status_Date==status_Date_check:
                            #last_status_time_cal =  last_status_time.strftime('%H:%M:%S')
                            try:
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = int(total_datwise_s)
                                #t1 = sum(p*q for p, q in zip(map(int, last_status_time_cal.split(':')), factors))
                                #print("t1",t1)
                                #last_status_time_t1 = t1%1440
                                #total_datwise_sum = int(last_status_time_t1)
                                #print(i,j,"down",down_time)
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        if last_status_Date!=status_Date_check:
                            try:
                                today_p_start = datetime.now()
                                status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = total_datwise_s%1440
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                    if last_status_time==0:
                        total_datwise_sum = 0
                        last_status_time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                    if total_datwise_sum>5:
                        insert_into_database_2(Tran_id_up_down,i,ip,time,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                        Tran_id_up_down+=1
                except Exception as e:
                    logging.error(e, exc_info=True)

            if i in ACTIVE_LIST and last_status==1:
                continue

            if i not in ACTIVE_LIST and last_status==0:
                exist = False
                current_status = 0
                time = datetime.now()
                try:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=11 order by TRAN_ID desc) where rownum =1"%i)
                    cur.execute(query)
                    for row in cur:
                        exist = True
                    if exist==False:
                        insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                        Tran_id+=1
                        #last_status = 0
                        #last_status_time = 0
                    #conn.close()
                except Exception as e:
                    logging.error(e, exc_info=True)
                continue


            if i not in ACTIVE_LIST and last_status==1:
                current_status=0
                time = datetime.now()
                try:
                    time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
        conn.close()
        
    except Exception as e:
        logging.error(e, exc_info=True)
        
        
    
def VMS_ACTION(VMS_list):
    global Tran_id
    global Tran_id_up_down
    try:
        factors = (60, 1, 1/60)
        Equip_ID = 12
        try:
            conn = cx_Oracle.connect('%s'%login_parameter)
            #cur = conn.cursor()
            #query = "select INCIDENT_TIME,RADAR_ID from ATMS.ATMS_LATEST_RADAR_INCIDENT where INCIDENT_TIME > sysdate - (15/1440)"
            #cur.execute(query)
            #print("done")
            #ACTIVE_LIST = []
            #for row in cur:
                #ACTIVE_LIST.append(row[1])
                #print(row)
            #conn.close()
            #print(ACTIVE_LIST)
            #print("ACTIVE_LIST_LENGTH",len(ACTIVE_LIST))
        except Exception as e:
            logging.error(e, exc_info=True)

        for i in VMS_list:
            last_status = None
            try:
                #conn = cx_Oracle.connect('%s'%login_parameter)
                cur = conn.cursor()
                query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=12 order by TRAN_ID desc) where rownum =1"%i)
                cur.execute(query)
                #print("hello")
                #print(cur)
                for row in cur:
                    print(row)
                    last_status = row[0]
                    last_status_time = row[1]
                    #print(last_status)
                    #print(last_status)
                #conn.close()
                if last_status is None:
                    last_status = 0
                    last_status_time = 0

            except Exception as e:
                logging.error(e, exc_info=True)
            #print(last_status)  
            try:
                ip = VMS_DICT[i]
                ping_status = pings(ip)
                #ip2 = ECB_DICT_MAP[i]
            except Exception as e:
                logging.error(e, exc_info=True)

            if ping_status==True and last_status==0:
                current_status =1
                try:
                    time = datetime.now()
                    if last_status_time!=0:
                        today_time = datetime.now()
                        status_Date = today_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = last_status_time.strftime('%Y-%m-%d')
                        if last_status_Date==status_Date_check:
                            #last_status_time_cal =  last_status_time.strftime('%H:%M:%S')
                            try:
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = int(total_datwise_s)
                                #t1 = sum(p*q for p, q in zip(map(int, last_status_time_cal.split(':')), factors))
                                #print("t1",t1)
                                #last_status_time_t1 = t1%1440
                                #total_datwise_sum = int(last_status_time_t1)
                                #print(i,j,"down",down_time)
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        if last_status_Date!=status_Date_check:
                            try:
                                today_p_start = datetime.now()
                                status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = total_datwise_s%1440
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)


                    if last_status_time==0:
                        total_datwise_sum = 0
                        last_status_time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                    if total_datwise_sum>5:
                        insert_into_database_2(Tran_id_up_down,i,ip,time,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                        Tran_id_up_down+=1
                except Exception as e:
                    logging.error(e, exc_info=True)

            if ping_status==True and last_status==1:
                continue

            if ping_status==False and last_status==0:
                exist = False
                current_status = 0
                time = datetime.now()
                try:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=12 order by TRAN_ID desc) where rownum =1"%i)
                    cur.execute(query)
                    for row in cur:
                        exist = True
                    if exist==False:
                        insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                        Tran_id+=1
                        #last_status = 0
                        #last_status_time = 0
                    #conn.close()
                except Exception as e:
                    logging.error(e, exc_info=True)
                continue


            if ping_status==False and last_status==1:
                current_status=0
                time = datetime.now()
                try:
                    time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
        conn.close()
        
    except Exception as e:
        logging.error(e, exc_info=True)
           
        
def MET_ACTION(MET_list):
    global Tran_id
    global Tran_id_up_down
    try:
        factors = (60, 1, 1/60)
        Equip_ID = 13
        try:
            conn = cx_Oracle.connect('%s'%login_parameter)
            cur = conn.cursor()
            query = "select MET_ID from ATMS.ATMS_MET_CURRENT_DATA where CREATION_DATE > sysdate - (33/1440)"
            cur.execute(query)
            #print("done")
            ACTIVE_LIST = []
            for row in cur:
                ACTIVE_LIST.append(row[0])
                print(row)
            #conn.close()
            #print(ACTIVE_LIST)
            #print("ACTIVE_LIST_LENGTH",len(ACTIVE_LIST))
        except Exception as e:
            logging.error(e, exc_info=True)

        for i in MET_list:
            last_status = None
            try:
                #conn = cx_Oracle.connect('%s'%login_parameter)
                cur = conn.cursor()
                query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=13 order by TRAN_ID desc) where rownum =1"%i)
                cur.execute(query)
                #print("hello")
                #print(cur)
                for row in cur:
                    print(row)
                    last_status = row[0]
                    last_status_time = row[1]
                    #print(last_status)
                    #print(last_status)
                #conn.close()
                if last_status is None:
                    last_status = 0
                    last_status_time = 0

            except Exception as e:
                logging.error(e, exc_info=True)
            #print(last_status)  
            try:
                ip = MET_DICT[i]
                ping_status = pings(ip)
                #ip2 = ECB_DICT_MAP[i]
            except Exception as e:
                logging.error(e, exc_info=True)
                
            if ping_status==True and i in ACTIVE_LIST:
                ping_status= True
            else:
                ping_status=False
                
            if ping_status==True and last_status==0:
                current_status =1
                try:
                    time = datetime.now()
                    if last_status_time!=0:
                        today_time = datetime.now()
                        status_Date = today_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = last_status_time.strftime('%Y-%m-%d')
                        if last_status_Date==status_Date_check:
                            #last_status_time_cal =  last_status_time.strftime('%H:%M:%S')
                            try:
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = int(total_datwise_s)
                                #t1 = sum(p*q for p, q in zip(map(int, last_status_time_cal.split(':')), factors))
                                #print("t1",t1)
                                #last_status_time_t1 = t1%1440
                                #total_datwise_sum = int(last_status_time_t1)
                                #print(i,j,"down",down_time)
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        if last_status_Date!=status_Date_check:
                            try:
                                today_p_start = datetime.now()
                                status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                                down_time_cal = time-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = total_datwise_s%1440
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)


                    if last_status_time==0:
                        total_datwise_sum = 0
                        last_status_time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                    if total_datwise_sum>5:
                        insert_into_database_2(Tran_id_up_down,i,ip,time,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                        Tran_id_up_down+=1
                except Exception as e:
                    logging.error(e, exc_info=True)

            if ping_status==True and last_status==1:
                continue

            if ping_status==False and last_status==0:
                exist = False
                current_status = 0
                time = datetime.now()
                try:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=13 order by TRAN_ID desc) where rownum =1"%i)
                    cur.execute(query)
                    for row in cur:
                        exist = True
                    if exist==False:
                        insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                        Tran_id+=1
                        #last_status = 0
                        #last_status_time = 0
                    #conn.close()
                except Exception as e:
                    logging.error(e, exc_info=True)
                continue


            if ping_status==False and last_status==1:
                current_status=0
                time = datetime.now()
                try:
                    time = datetime.now()
                    insert_into_database(Tran_id,i,ip,time,current_status,Equip_ID,conn)
                    Tran_id+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
        conn.close()
        
    except Exception as e:
        logging.error(e, exc_info=True)
    
    
    
def NMS_STATUS_CHECK(NMS_List,CCTV_List,ECB_List,VMS_List,VIDS_List,ATCC_List,CR_ASSET_List,MET_List,ANPR_List,RADAR_List,ATMS_List):
    global Tran_id_up_down
    global Tran_id
    global Tran_id_CR
    global Tran_id_cr_up_down
    factors = (60, 1, 1/60)
    
    try:
        for i in NMS_List:
            Equip_ID = NMS_DICT_MAP[i]
            #print(type(Equip_ID))
            time.sleep(1)
            last_status = None
            #print("----------------------------------------------------------------")
            try:
                conn = cx_Oracle.connect('%s'%login_parameter)
                cur = conn.cursor()
                query = ("select * from (select STATUS,STATUS_TIME from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID={0} AND EQUIP_ID={1} order by TRAN_ID desc) where rownum =1".format(i,Equip_ID))
                cur.execute(query)
                for row in cur:
                    last_status = row[0]
                    last_status_time = row[1]
                    #print(last_status)
                #conn.close()
                if last_status is None:
                    last_status = 0
                    last_status_time = 0

            except Exception as e:
                logging.error("Exception occurred", exc_info=True)
                #last_status = 0
                #last_status_time = 0

            ip = NMS_DICT[i]
            #print(ip)
            #print(last_status,last_status_time,ip)
            ping_status = True
            #print(ip,ping_status)

            if ping_status==True and last_status==0:
                #print("1",ip,last_status)
                current_status = 1
                try:
                    time_1 = datetime.now()
                    if last_status_time!=0:
                        today_time = datetime.now()
                        status_Date = today_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = last_status_time.strftime('%Y-%m-%d')
                        if last_status_Date==status_Date_check:
                            #last_status_time_cal =  last_status_time.strftime('%H:%M:%S')
                            try:
                                down_time_cal = time_1-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = int(total_datwise_s)
                                #t1 = sum(p*q for p, q in zip(map(int, last_status_time_cal.split(':')), factors))
                                #print("t1",t1)
                                #last_status_time_t1 = t1%1440
                                #total_datwise_sum = int(last_status_time_t1)
                                #print(i,j,"down",down_time)
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                        if last_status_Date!=status_Date_check:
                            try:
                                today_p_start = datetime.now()
                                status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                                down_time_cal = time_1-last_status_time
                                total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                                total_datwise_sum = total_datwise_s%1440
                            except Exception as e:
                                logging.error("Exception occurred", exc_info=True)

                    if last_status_time==0:
                        total_datwise_sum = 0
                        last_status_time = datetime.now()

                    #print("1",ip,last_status,last_status_time,total_datwise_sum)
                    insert_into_database_CR(Tran_id_CR,i,ip,time_1,current_status,Equip_ID,conn)
                    insert_into_database_2_CR(Tran_id_cr_up_down,i,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                    Tran_id_CR+=1
                    Tran_id_cr_up_down+=1
                except Exception as e:
                    logging.error(e, exc_info=True)
                continue

            if ping_status==True and last_status==1:
                time_1 = datetime.now()
                current_status = 1
                try:
                    today_time = datetime.now()
                    status_Date = today_time.strftime('%Y%m%d')
                    status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                    status_Date_check = status_Date.strftime('%Y-%m-%d')
                    last_status_Date = last_status_time.strftime('%Y-%m-%d')

                    if last_status_Date==status_Date_check:
                        down_time_cal = time_1-last_status_time
                        total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                        total_datwise_sum = int(total_datwise_s)
                        if total_datwise_sum<=30:
                            update_into_database_CR(i,time_1,Equip_ID,conn)
                            #Tran_id_CR+=1
                        if total_datwise_sum>30:
                            update_into_database_CR(i,time_1,Equip_ID,conn)
                            insert_into_database_2_CR(Tran_id_cr_up_down,i,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                            Tran_id_cr_up_down+=1

                            for cr_item in CR_ASSET_List:
                                Equip_ID = CR_ASSET_DICT_MAP[cr_item]
                                query = ("select * from (select STATUS,IP from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID={0} AND EQUIP_ID={1} order by TRAN_ID desc) where rownum =1".format(cr_item,Equip_ID))
                                cur.execute(query)
                                for row in cur:
                                    last_cr_asset_status = row[0]
                                    ip = row[1]
                                if last_cr_asset_status==1:
                                    insert_into_database_2_CR(Tran_id_cr_up_down,cr_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_cr_up_down+=1
                                    #if cr_item!=7:
                                    insert_into_database_CR(Tran_id_CR,cr_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id_CR+=1 
                                    
                            for atms_item in ATMS_List:
                                Equip_ID = ATMS_DICT_MAP[atms_item]
                                query = ("select * from (select STATUS,IP from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID={0} AND EQUIP_ID={1} order by TRAN_ID desc) where rownum =1".format(atms_item,Equip_ID))
                                cur.execute(query)
                                for row in cur:
                                    last_atms_status = row[0]
                                    ip = row[1]
                                if last_atms_status==1:
                                    insert_into_database_2_CR(Tran_id_cr_up_down,atms_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_cr_up_down+=1
                                    #if cr_item!=7:
                                    insert_into_database_CR(Tran_id_CR,atms_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id_CR+=1 


                            for cctv_item in CCTV_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=1 order by TRAN_ID desc) where rownum =1"%cctv_item)
                                cur.execute(query)
                                for row in cur:
                                    last_cctv_status = row[0]
                                    ip = row[1]
                                if last_cctv_status==1:
                                    Equip_ID = 1
                                    insert_into_database_2(Tran_id_up_down,cctv_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,cctv_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1

                            for atcc_item in ATCC_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=3 order by TRAN_ID desc) where rownum =1"%atcc_item)
                                cur.execute(query)
                                for row in cur:
                                    last_atcc_status = row[0]
                                    ip = row[1]
                                if last_atcc_status==1:
                                    Equip_ID = 3
                                    insert_into_database_2(Tran_id_up_down,atcc_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,atcc_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1

                            for ecb_item in ECB_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=2 order by TRAN_ID desc) where rownum =1"%ecb_item)
                                cur.execute(query)
                                for row in cur:
                                    last_ecb_status = row[0]
                                    ip = row[1]
                                if last_ecb_status==1:
                                    Equip_ID = 2
                                    insert_into_database_2(Tran_id_up_down,ecb_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,ecb_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1

                            for anpr_item in ANPR_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=4 order by TRAN_ID desc) where rownum =1"%anpr_item)
                                cur.execute(query)
                                for row in cur:
                                    last_anpr_status = row[0]
                                    ip = row[1]
                                if last_anpr_status==1:
                                    Equip_ID = 4
                                    insert_into_database_2(Tran_id_up_down,anpr_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,anpr_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1

                            for radar_item in RADAR_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=5 order by TRAN_ID desc) where rownum =1"%radar_item)
                                cur.execute(query)
                                for row in cur:
                                    last_radar_status = row[0]
                                    ip = row[1]
                                if last_radar_status==1:
                                    Equip_ID = 5
                                    insert_into_database_2(Tran_id_up_down,radar_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,radar_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1
                                    
                            for vids_item in VIDS_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=11 order by TRAN_ID desc) where rownum =1"%vids_item)
                                cur.execute(query)
                                for row in cur:
                                    last_vids_status = row[0]
                                    ip = row[1]
                                if last_vids_status==1:
                                    Equip_ID = 11
                                    insert_into_database_2(Tran_id_up_down,vids_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,vids_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1
                                    
                            for vms_item in VMS_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=12 order by TRAN_ID desc) where rownum =1"%vms_item)
                                cur.execute(query)
                                for row in cur:
                                    last_vms_status = row[0]
                                    ip = row[1]
                                if last_vms_status==1:
                                    Equip_ID = 12
                                    insert_into_database_2(Tran_id_up_down,vms_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,vms_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1
                            
                            for met_item in MET_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=13 order by TRAN_ID desc) where rownum =1"%met_item)
                                cur.execute(query)
                                for row in cur:
                                    last_met_status = row[0]
                                    ip = row[1]
                                if last_met_status==1:
                                    Equip_ID = 13
                                    insert_into_database_2(Tran_id_up_down,met_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,met_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1



                    if last_status_Date!=status_Date_check:
                        today_p_start = datetime.now()
                        status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                        status_Date_p_start = status_Date_p_start+" 00:00:01"
                        last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                        down_time_cal = time_1-last_status_time
                        total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                        total_datwise_sum = total_datwise_s%1440
                        if total_datwise_sum<=30:
                            update_into_database_CR(i,time_1,Equip_ID,conn)

                        if total_datwise_sum>30:
                            update_into_database_CR(i,time_1,Equip_ID,conn)
                            for cr_item in CR_ASSET_List:
                                Equip_ID = CR_ASSET_DICT_MAP[cr_item]
                                query = ("select * from (select STATUS,IP from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID={0} AND EQUIP_ID={1} order by TRAN_ID desc) where rownum =1".format(cr_item,Equip_ID))
                                cur.execute(query)
                                for row in cur:
                                    last_cr_asset_status = row[0]
                                    ip = row[1]
                                if last_cr_asset_status==1:
                                    insert_into_database_2_CR(Tran_id_cr_up_down,cr_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_cr_up_down+=1
                                    insert_into_database_CR(Tran_id_CR,cr_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id_CR+=1
                                 
                            for atms_item in ATMS_List:
                                Equip_ID = ATMS_DICT_MAP[atms_item]
                                query = ("select * from (select STATUS,IP from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID={0} AND EQUIP_ID={1} order by TRAN_ID desc) where rownum =1".format(atms_item,Equip_ID))
                                cur.execute(query)
                                for row in cur:
                                    last_atms_status = row[0]
                                    ip = row[1]
                                if last_atms_status==1:
                                    insert_into_database_2_CR(Tran_id_cr_up_down,atms_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_cr_up_down+=1
                                    insert_into_database_CR(Tran_id_CR,atms_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id_CR+=1


                            for cctv_item in CCTV_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=1 order by TRAN_ID desc) where rownum =1"%cctv_item)
                                cur.execute(query)
                                for row in cur:
                                    last_cctv_status = row[0]
                                    ip = row[1]
                                if last_cctv_status==1:
                                    Equip_ID = 1
                                    insert_into_database_2(Tran_id_up_down,cctv_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,cctv_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1

                            for atcc_item in ATCC_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=3 order by TRAN_ID desc) where rownum =1"%atcc_item)
                                cur.execute(query)
                                for row in cur:
                                    last_atcc_status = row[0]
                                    ip = row[1]
                                if last_atcc_status==1:
                                    Equip_ID = 3
                                    insert_into_database_2(Tran_id_up_down,atcc_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,atcc_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1

                            for ecb_item in ECB_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=2 order by TRAN_ID desc) where rownum =1"%ecb_item)
                                cur.execute(query)
                                for row in cur:
                                    last_ecb_status = row[0]
                                    ip = row[1]
                                if last_ecb_status==1:
                                    Equip_ID = 2
                                    insert_into_database_2(Tran_id_up_down,ecb_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,ecb_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1

                            for vids_item in VIDS_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=11 order by TRAN_ID desc) where rownum =1"%vids_item)
                                cur.execute(query)
                                for row in cur:
                                    last_vids_status = row[0]
                                    ip = row[1]
                                if last_vids_status==1:
                                    Equip_ID = 11
                                    insert_into_database_2(Tran_id_up_down,vids_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,vids_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1

                            for radar_item in RADAR_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=5 order by TRAN_ID desc) where rownum =1"%radar_item)
                                cur.execute(query)
                                for row in cur:
                                    last_radar_status = row[0]
                                    ip = row[1]
                                if last_radar_status==1:
                                    Equip_ID = 5
                                    insert_into_database_2(Tran_id_up_down,radar_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,radar_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1
                                    
                            for anpr_item in ANPR_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=4 order by TRAN_ID desc) where rownum =1"%anpr_item)
                                cur.execute(query)
                                for row in cur:
                                    last_anpr_status = row[0]
                                    ip = row[1]
                                if last_anpr_status==1:
                                    Equip_ID = 4
                                    insert_into_database_2(Tran_id_up_down,anpr_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,anpr_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1
                                    
                            for vms_item in VMS_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=12 order by TRAN_ID desc) where rownum =1"%vms_item)
                                cur.execute(query)
                                for row in cur:
                                    last_vms_status = row[0]
                                    ip = row[1]
                                if last_vms_status==1:
                                    Equip_ID = 12
                                    insert_into_database_2(Tran_id_up_down,vms_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,vms_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1  
                                    
                            for met_item in MET_List:
                                query = ("select * from (select STATUS,IP from ATMS.TBL_UP_DOWN where DEV_ID='%d' AND EQUIP_ID=13 order by TRAN_ID desc) where rownum =1"%met_item)
                                cur.execute(query)
                                for row in cur:
                                    last_met_status = row[0]
                                    ip = row[1]
                                if last_met_status==1:
                                    Equip_ID = 13
                                    insert_into_database_2(Tran_id_up_down,met_item,ip,time_1,current_status,Equip_ID,total_datwise_sum,last_status_time,conn)
                                    Tran_id_up_down+=1
                                    insert_into_database(Tran_id,met_item,ip,time_1,current_status,Equip_ID,conn)
                                    Tran_id+=1  
                            

                except Exception as e:
                    logging.error("Exception occurred", exc_info=True)

        conn.close()
        
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

import logging
from datetime import datetime,timedelta
logging.basicConfig(filename=datetime.now().strftime('%d_%m_%Y.log'),
                    filemode='a',
                    format='%(asctime)s:%(msecs)d-%(name)s-%(levelname)s-%(message)s',
                    datefmt='%d-%m-%Y %H:%M:%S',level=logging.DEBUG)
try:
    import pkg_resources.py2_warn
    import cx_Oracle
    from pysnmp.hlapi import *
    import Switch_info_2 as sf
    import sqlite3
    import pandas as pd
    import time
    time.sleep(100)
    from datetimerange import DateTimeRange
    import logging
    import vids_status as vs
    import ECB_status as es
    import cv2
    import sys
    import telnetlib
    import subprocess
    import socket
        #from datetime import datetime
    import xml.etree.ElementTree as ET
    
except Exception as e:
    logging.error(e, exc_info=True)
    #import pandas as pd
tree = ET.parse('ATMS_DOWN_STATUS.xml')
root = tree.getroot()
config_data = []
for elem in root:
    for subelem in elem:
        config_data.append(subelem.text)
        #print("Data file:",config_data[0])
conf_user_name = config_data[0]
conf_password = config_data[1]
conf_ip = config_data[2]
conf_service = config_data[3]


login_parameter = "{0}/{1}@{2}:1521/{3}".format(conf_user_name,conf_password,conf_ip,conf_service)

#print(login_parameter)


Tran_ID = None
Tran_id_up_down=None
Tran_id_CR = None
Tran_id_cr_up_down = None
Tran_id_switch = None
run_time_check = False


try:
    conn = cx_Oracle.connect('%s'%login_parameter)
    cur = conn.cursor()
    cur.execute("SELECT MAX(TRAN_ID) FROM ATMS.TBL_UP_DOWN")
    for row in cur:
        Tran_id = row[0]
    if Tran_id is None:
        Tran_id = 0
    Tran_id+=1
    conn.close()
    #print(Tran_id)
except Exception as e:
    logging.error(e, exc_info=True)
    
try:
    conn = cx_Oracle.connect('%s'%login_parameter)
    cur = conn.cursor()
    cur.execute("SELECT MAX(TRAN_ID) FROM ATMS.DOWN_TIME_RECORD")
    for row in cur:
        Tran_id_up_down = row[0]
    if Tran_id_up_down is None:
        Tran_id_up_down = 0
    Tran_id_up_down+=1
    conn.close()

except Exception as e:
    logging.error(e, exc_info=True)

    
try:
    conn = cx_Oracle.connect('%s'%login_parameter)
    cur = conn.cursor()
    cur.execute("SELECT MAX(TRAN_ID) FROM ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS")
    for row in cur:
        Tran_id_CR = row[0]
        
    if Tran_id_CR is None:
        Tran_id_CR = 0
    Tran_id_CR+=1
    conn.close()
    #print(Tran_id_CR)

except Exception as e:
    logging.error(e, exc_info=True)

try:
    conn = cx_Oracle.connect('%s'%login_parameter)
    cur = conn.cursor()
    cur.execute("SELECT MAX(TRAN_ID) FROM ATMS.ATMS_CR_ASSET_DOWN_TIME")
    for row in cur:
        Tran_id_cr_up_down = row[0]
        
    if Tran_id_cr_up_down is None:
        Tran_id_cr_up_down = 1
    Tran_id_cr_up_down+=1
    #conn.close()

except Exception as e:
    logging.error(e, exc_info=True)
    #print(Tran_id_CR,Tran_id_cr_up_down)
    
    

    
    
try:
    cur = conn.cursor()
    cur.execute("SELECT MAX(SR_NO) FROM ATMS.ATMS_SWITCH_STATUS")
    for row in cur:
        Tran_id_switch = row[0]
            
    if Tran_id_switch is None:
        Tran_id_switch = 1
    Tran_id_switch+=1
    #print(Tran_id)
except Exception as e:
    logging.error(e, exc_info=True)
    


    
try:
    query3 = "select * from ATMS.ATMS_OSD_SWITCH"
    df3 = pd.read_sql(query3, con=conn)
    df31 = df3.loc[df3['SUB_SYSTEM_ID'] ==14]
    ##df32 = df3.loc[df3['SUB_SYSTEM_ID'] ==10]
    #df42 = df4.loc[df4['SUB_SYSTEM_ID'] !=10]
    #df311 = df31.loc[df31['ASSET_ID'] ==7]
    #df312 = df31.loc[df31['ASSET_ID'] !=7]
    df32 = df31[["ASSET_ID","IP_ADDRESS"]]
    df32 = df32[df32['ASSET_ID'].notnull()]
    df32 = df32[df32['IP_ADDRESS'].notnull()]
    SWITCH_DICT = dict(zip(df32.ASSET_ID, df32.IP_ADDRESS))
    SWITCH_List = df32["ASSET_ID"].unique().tolist()
except Exception as e:
    logging.error(e, exc_info=True)
    
    
try:
    query1 = "select * from ATMS.ATMS_ECB_CHARGE_CONTROLLER"
    df1 = pd.read_sql(query1, con=conn)
    df1 = df1[["ECB_ID","ECB_NAME","IP_ADDRESS"]]
    df1 = df1[df1['ECB_ID'].notnull()]
    df1 = df1[df1['IP_ADDRESS'].notnull()]

    query2 = "select * from ATMS.ATMS_ECB"
    df2 = pd.read_sql(query2, con=conn)

    df2 = df2[["ECB_ID","IP_ADDRESS"]]
    df2 = df2[df2['IP_ADDRESS'].notnull()]
    df2 = df2[df2['ECB_ID'].notnull()]
    #time.sleep(10)
    ECB_DICT = dict(zip(df1.ECB_ID, df1.IP_ADDRESS))
    ECB_DICT_MAP = dict(zip(df2.ECB_ID, df2.IP_ADDRESS))
    ECB_List = df2["ECB_ID"].unique().tolist()
    #print(len())
    #print(len(ECB_DICT),len(ECB_DICT_MAP))

    query3 = "select * from ATMS.ATMS_CCTV"
    df3 = pd.read_sql(query3, con=conn)
    df3 = df3[["CAMERA_ID","IP_ADDRESS"]]
    df3 = df3[df3['CAMERA_ID'].notnull()]
    df3 = df3[df3['IP_ADDRESS'].notnull()]
    CCTV_DICT = dict(zip(df3.CAMERA_ID, df3.IP_ADDRESS))
    CCTV_List = df3["CAMERA_ID"].unique().tolist()

except Exception as e:
    logging.error(e, exc_info=True)

try:
    query4 = "select * from ATMS.ATMS_OSD_SWITCH"
    df4 = pd.read_sql(query4, con=conn)
    df4 = df4[["ASSET_ID","IP_ADDRESS","SUB_SYSTEM_ID"]]
    df4 = df4[df4['ASSET_ID'].notnull()]
    df4 = df4[df4['IP_ADDRESS'].notnull()]
    df4 = df4[df4['SUB_SYSTEM_ID'].notnull()]
    df41 = df4.loc[df4['SUB_SYSTEM_ID'] ==10]
    df42 = df4.loc[df4['SUB_SYSTEM_ID'] !=10]
    df43 = df42.loc[df42['SUB_SYSTEM_ID'] !=14]
    df411 = df41.loc[df41['ASSET_ID'] ==7]
    df412 = df41.loc[df41['ASSET_ID'] !=7]
    CR_ASSET_DICT = dict(zip(df43.ASSET_ID, df43.IP_ADDRESS))
    CR_ASSET_DICT_MAP = dict(zip(df43.ASSET_ID, df43.SUB_SYSTEM_ID))
    CR_ASSET_List = df43["ASSET_ID"].unique().tolist()
    NMS_DICT = dict(zip(df411.ASSET_ID, df411.IP_ADDRESS))
    NMS_DICT_MAP = dict(zip(df411.ASSET_ID, df411.SUB_SYSTEM_ID))
    NMS_List = df411["ASSET_ID"].unique().tolist()
    ATMS_DICT = dict(zip(df412.ASSET_ID, df412.IP_ADDRESS))
    ATMS_DICT_MAP = dict(zip(df412.ASSET_ID, df412.SUB_SYSTEM_ID))
    ATMS_List = df412["ASSET_ID"].unique().tolist()

except Exception as e:
    logging.error(e, exc_info=True)

try:
    query5 = "select * from ATMS.ATMS_ATCC"
    df5 = pd.read_sql(query5, con=conn)
    df5 = df5[["ATCC_ID","IP_ADDRESS"]]
    df5 = df5[df5['ATCC_ID'].notnull()]
    df5 = df5[df5['IP_ADDRESS'].notnull()]
    ATCC_DICT = dict(zip(df5.ATCC_ID, df5.IP_ADDRESS))
    ATCC_List = df5["ATCC_ID"].unique().tolist()
    #print(len(ATCC_DICT),ATCC_DICT)
    #print(len(ATCC_List))
except Exception as e:
    logging.error(e, exc_info=True)

    
try:
    query6 = "select * from ATMS.ATMS_RADAR"
    df6 = pd.read_sql(query6, con=conn)
    df6 = df6[["RADAR_ID","IP_ADDRESS"]]
    df6 = df6[df6['RADAR_ID'].notnull()]
    df6 = df6[df6['IP_ADDRESS'].notnull()]
    RADAR_DICT = dict(zip(df6.RADAR_ID, df6.IP_ADDRESS))
    RADAR_List = df6["RADAR_ID"].unique().tolist()
    #conn.close()
    #print(len(RADAR_DICT),RADAR_DICT)
    #print(len(ATCC_List))
except Exception as e:
    logging.error(e, exc_info=True)
    
try:
    query6 = "select * from ATMS.ATMS_VMS"
    df6 = pd.read_sql(query6, con=conn)
    df6 = df6[["VMS_ID","IP_ADDRESS"]]
    df6 = df6[df6['VMS_ID'].notnull()]
    df6 = df6[df6['IP_ADDRESS'].notnull()]
    VMS_DICT = dict(zip(df6.VMS_ID, df6.IP_ADDRESS))
    VMS_List = df6["VMS_ID"].unique().tolist()
    #conn.close()
    #print(len(RADAR_DICT),RADAR_DICT)
    #print(len(ATCC_List))
except Exception as e:
    logging.error(e, exc_info=True)
    

try:
    query6 = "select * from ATMS.ATMS_VIDS"
    df6 = pd.read_sql(query6, con=conn)
    df6 = df6[["VIDS_ID","IP_ADDRESS"]]
    df6 = df6[df6['VIDS_ID'].notnull()]
    df6 = df6[df6['IP_ADDRESS'].notnull()]
    VIDS_DICT = dict(zip(df6.VIDS_ID, df6.IP_ADDRESS))
    VIDS_List = df6["VIDS_ID"].unique().tolist()
    #conn.close()
    #print(len(RADAR_DICT),RADAR_DICT)
    #print(len(ATCC_List))
except Exception as e:
    logging.error(e, exc_info=True)
    
    
    
try:
    query6 = "select * from ATMS.ATMS_MET"
    df6 = pd.read_sql(query6, con=conn)
    df6 = df6[["MET_ID","IP_ADDRESS"]]
    df6 = df6[df6['MET_ID'].notnull()]
    df6 = df6[df6['IP_ADDRESS'].notnull()]
    MET_DICT = dict(zip(df6.MET_ID, df6.IP_ADDRESS))
    MET_List = df6["MET_ID"].unique().tolist()
    #conn.close()
    #print(len(RADAR_DICT),RADAR_DICT)
    #print(len(ATCC_List))
except Exception as e:
    logging.error(e, exc_info=True)
    
ANPR_List=[]
RADAR_List=[]
  

while True:
    run_time = datetime.now().strftime('%H')
    run_time = int(run_time)
    run_time_min = datetime.now().strftime('%M')
    run_time_min = int(run_time_min)
    #print(run_time,run_time_min)
    time.sleep(20)
    if run_time==0 and run_time_min<15:
        run_time_check = True
        continue
    elif run_time==0 and run_time_min>=15 and run_time_check==True:
        run_time_check = False
        try:
            conn = cx_Oracle.connect('%s'%login_parameter)
            cur = conn.cursor()
            cur.execute("SELECT MAX(TRAN_ID) FROM ATMS.DOWN_TIME_RECORD")
            for row in cur:
                Tran_id_up_down = row[0]
                Tran_id_up_down+=1
            conn.close()
        except Exception as e:
            logging.error(e, exc_info=True)
        try:
            ECB_Report()
        except Exception as e:
            logging.error(e, exc_info=True)
        time.sleep(2)
        try:
            conn = cx_Oracle.connect('%s'%login_parameter)
            cur = conn.cursor()
            cur.execute("SELECT MAX(TRAN_ID) FROM ATMS.ATMS_CR_ASSET_DOWN_TIME")
            for row in cur:
                Tran_id_cr_up_down = row[0]
                Tran_id_cr_up_down+= 1
            

        except Exception as e:
            logging.error(e, exc_info=True)
            


    else:
        NMS_STATUS_CHECK(NMS_List,CCTV_List,ECB_List,VMS_List,VIDS_List,ATCC_List,CR_ASSET_List,MET_List,ANPR_List,RADAR_List,ATMS_List)
        CCTV_ACTION(CCTV_List)
        ATMS_ACTION(ATMS_List)
        CONTROL_ROOM_ASSET_ACTION(CR_ASSET_List)
        ECB_ACTION(ECB_List)
        VMS_ACTION(VMS_List)
        VIDS_ACTION(VIDS_List)
        ATCC_ACTION(ATCC_List)
        SWITCH_ACTION(SWITCH_List)
        MET_ACTION(MET_List)
        time.sleep(30)
       

