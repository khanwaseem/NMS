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
    
    
def ECB_ACTION(ECB_list):
    import time as  tt
    global Tran_id
    global Tran_id_up_down
    try:
        Equip_ID = 2
        try:
            conn = cx_Oracle.connect('%s'%login_parameter)
            cur = conn.cursor()
            query = "select DEVICE_ID,STATUS from ATMS.ATMS_ECB_STATUS where STATUS_TIME > sysdate - (15/1440) and STATUS=1"
            cur.execute(query)
            ACTIVE_LIST = []
            for row in cur:
                ACTIVE_LIST.append(row[0])
            #conn.close()
        except Exception as e:
            logging.error(e, exc_info=True)
        #print(ACTIVE_LIST)
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
                ping_status = pings(ip)
                if ping_status==False:
                    tt.sleep(0.8)
                    ping_status = pings(ip)
                if ping_status == False:
                    tt.sleep(3)
                    ping_status = pings(ip)
            except Exception as e:
                logging.error(e, exc_info=True)

            if ping_status==True and last_status==1:
                continue

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


            if ping_status==False and last_status==0:
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

            if ping_status==False and last_status==1:
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
    import time as tt
    global Tran_id_CR
    global Tran_id_cr_up_down
    try:
        factors = (60, 1, 1/60)
        for i in CR_ASSET_list:
            Equip_ID = CR_ASSET_DICT_MAP[i]
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
            if ping_status==False:
                tt.sleep(0.5)
                ping_status = pings(ip)
            if ping_status == False:
                tt.sleep(5)
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
    import time as tt
    global Tran_id
    global Tran_id_up_down
    try:
        Equip_ID = 1
        factors = (60, 1, 1/60)

        for i in CCTV_list:
            last_status = None

            try:
                conn = cx_Oracle.connect('%s'%login_parameter)
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
                cap = cv2.VideoCapture("rtsp://admin:admin123@%s"%ip)
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
    
            

def ANPR_ACTION(ANPR_list):
    import time as tt
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
                if ping_status==False:
                    tt.sleep(0.5)
                    ping_status = pings(ip)
                if ping_status == False:
                    tt.sleep(5)
                    ping_status = pings(ip)

            if previous_ip is not None:
                pre_ip,pre_status = previous_ip[0],previous_ip[1]
                if pre_ip==ip:
                    ping_status= pre_status
                if pre_ip!=ip:
                    ping_status = pings(ip)
                    if ping_status==False:
                        tt.sleep(0.5)
                        ping_status = pings(ip)
                    if ping_status == False:
                        tt.sleep(5)
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


def ATCC_ACTION(ATCC_list):
    import time as tt
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
            print(ACTIVE_LIST)
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
                if ping_status==False:
                    tt.sleep(0.5)
                    ping_status = pings(ip)
                if ping_status == False:
                    tt.sleep(5)
                    ping_status = pings(ip)
                #ip2 = ECB_DICT_MAP[i]
            except Exception as e:
                logging.error(e, exc_info=True)

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

                    
def RADAR_ACTION(RADAR_list):
    import time as tt
    global Tran_id
    global Tran_id_up_down
    try:
        factors = (60, 1, 1/60)
        Equip_ID = 5
        try:
            conn = cx_Oracle.connect('%s'%login_parameter)
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
                if ping_status==False:
                    tt.sleep(0.5)
                    ping_status = pings(ip)
                if ping_status == False:
                    tt.sleep(5)
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
    
    
def NMS_STATUS_CHECK(NMS_List,CCTV_List,ECB_List,RADAR_List,ANPR_List,ATCC_List,CR_ASSET_List):
    global Tran_id_up_down
    global Tran_id
    global Tran_id_CR
    global Tran_id_cr_up_down
    import time as tt
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
                        if total_datwise_sum<=20:
                            update_into_database_CR(i,time_1,Equip_ID,conn)
                            #Tran_id_CR+=1
                        if total_datwise_sum>20:
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
                                    if cr_item!=7:
                                        insert_into_database_CR(Tran_id_CR,cr_item,ip,time_1,current_status,Equip_ID,conn)
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



                    if last_status_Date!=status_Date_check:
                        today_p_start = datetime.now()
                        status_Date_p_start = today_p_start.strftime('%Y/%m/%d')
                        status_Date_p_start = status_Date_p_start+" 00:00:01"
                        last_status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                        down_time_cal = time_1-last_status_time
                        total_datwise_s = round(down_time_cal.total_seconds()/60.0)
                        total_datwise_sum = total_datwise_s%1440
                        if total_datwise_sum<=20:
                            update_into_database_CR(i,time_1,Equip_ID,conn)

                        if total_datwise_sum>20:
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
                                    if cr_item!=7:
                                        insert_into_database_CR(Tran_id_CR,cr_item,ip,time_1,current_status,Equip_ID,conn)
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

                except Exception as e:
                    logging.error("Exception occurred", exc_info=True)

        conn.close()
        
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

import logging
from datetime import datetime
import cx_Oracle
import pandas as pd
import time
time.sleep(150)
import logging
import cv2
import sys
import subprocess
import socket
    #from datetime import datetime
import xml.etree.ElementTree as ET
    
#time.sleep(150)

logging.basicConfig(filename=datetime.now().strftime('%d_%m_%Y.log'),
                    filemode='a',
                    format='%(asctime)s:%(msecs)d-%(name)s-%(levelname)s-%(message)s',
                    datefmt='%H:%M:%S',level=logging.DEBUG)
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
run_time_check = False


try:
    conn = cx_Oracle.connect('%s'%login_parameter)
    cur = conn.cursor()
    cur.execute("SELECT MAX(TRAN_ID) FROM ATMS.TBL_UP_DOWN")
    for row in cur:
        Tran_id = row[0]
        Tran_id+=1
    if Tran_id is None:
        Tran_id = 1
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
        Tran_id_up_down+=1
    if Tran_id_up_down is None:
        Tran_id_up_down = 1
    conn.close()

except Exception as e:
    logging.error(e, exc_info=True)

    
try:
    conn = cx_Oracle.connect('%s'%login_parameter)
    cur = conn.cursor()
    cur.execute("SELECT MAX(TRAN_ID) FROM ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS")
    for row in cur:
        Tran_id_CR = row[0]
        Tran_id_CR+=1
    if Tran_id_CR is None:
        Tran_id_CR = 1
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
        Tran_id_cr_up_down+=1
    if Tran_id_cr_up_down is None:
        Tran_id_cr_up_down = 1
    #conn.close()

except Exception as e:
    logging.error(e, exc_info=True)
    #print(Tran_id_CR,Tran_id_cr_up_down)
    
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
    CR_ASSET_DICT = dict(zip(df42.ASSET_ID, df42.IP_ADDRESS))
    CR_ASSET_DICT_MAP = dict(zip(df42.ASSET_ID, df42.SUB_SYSTEM_ID))
    CR_ASSET_List = df42["ASSET_ID"].unique().tolist()
    NMS_DICT = dict(zip(df41.ASSET_ID, df41.IP_ADDRESS))
    NMS_DICT_MAP = dict(zip(df41.ASSET_ID, df41.SUB_SYSTEM_ID))
    NMS_List = df41["ASSET_ID"].unique().tolist()

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
    query7 = "select * from ATMS.ATMS_ANPR_NMS"
    df7 = pd.read_sql(query7, con=conn)
    df7 = df7[["ANPR_ID","IP_ADDRESS","GANTRY_ID","LANE_ID"]]
    df7 = df7[df7['ANPR_ID'].notnull()]
    df7 = df7[df7['IP_ADDRESS'].notnull()]
    df7 = df7[df7['LANE_ID'].notnull()]
    df7 = df7[df7['GANTRY_ID'].notnull()]
    ANPR_DICT = dict(zip(df7.ANPR_ID, df7.IP_ADDRESS))
    GANTRY_LANE = list(zip(df7.GANTRY_ID, df7.LANE_ID))
    #print(GANTRY_LANE)
    ANPR_DICT_MAP = dict(zip(df7.ANPR_ID,GANTRY_LANE))
    #print(ANPR_DICT_MAP)
    #ANPR_DICT_MAP_2 = dict(zip(df7.GANTRY_ID, df7.LANE_ID))
    ANPR_List = df7["ANPR_ID"].unique().tolist()
    conn.close()
    #print(len(ANPR_DICT),ANPR_List)
    #print(len(ATCC_List))
except Exception as e:
    logging.error(e, exc_info=True)

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
        time.sleep(2)
        try:
            conn = cx_Oracle.connect('%s'%login_parameter)
            cur = conn.cursor()
            cur.execute("SELECT MAX(TRAN_ID) FROM ATMS.ATMS_CR_ASSET_DOWN_TIME")
            for row in cur:
                Tran_id_cr_up_down = row[0]
                Tran_id_cr_up_down+= 1
            conn.close()

        except Exception as e:
            logging.error(e, exc_info=True)
    else:
        NMS_STATUS_CHECK(NMS_List,CCTV_List,ECB_List,RADAR_List,ANPR_List,ATCC_List,CR_ASSET_List)
        CCTV_ACTION(CCTV_List)
        CONTROL_ROOM_ASSET_ACTION(CR_ASSET_List)
        ECB_ACTION(ECB_List)
        RADAR_ACTION(RADAR_List)
        ANPR_ACTION(ANPR_List)
        ATCC_ACTION(ATCC_List)
        time.sleep(90)
       

