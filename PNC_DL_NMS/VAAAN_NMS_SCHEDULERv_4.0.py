#final at 3:55 on 6 dec 2019
import logging
from datetime import datetime, timedelta
logging.basicConfig(filename=datetime.now().strftime('%d_%m_%Y.log'),
                            filemode='a',
                            format='%(asctime)s:%(msecs)d-%(name)s-%(levelname)s-%(message)s',
                            datefmt='%H:%M:%S',level=logging.DEBUG)
try:
    import pkg_resources.py2_warn
    import time
    import pandas as pd
    import cx_Oracle
    from operator import mul
    import xml.etree.ElementTree as ET
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
    
except Exception as e:
    logging.error(e, exc_info=True)






def insert_into_database_3(ID_i,EQ_ID_i,dev_id_i,Date_i,Down_Time_i,conn):
    try:
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("insert into ATMS.ATMS_DOWNTIME_DAY (TRANSACTION_ID,EQUPMENT_ID,DEVICE_ID,DOWNTIME_DATE,DOWNTIME) values (:1, :2, :3, :4, :5)", (ID_i,EQ_ID_i,dev_id_i,Date_i,Down_Time_i))
        conn.commit()
        #logging.info('Data Inserted %s'%ip_address)
        #conn.close()
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

    
def insert_into_cr_downtime(ID_i,EQ_ID_i,dev_id_i,Date_i,Down_Time_i,conn):
    try:
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("insert into ATMS.ATMS_CR_DOWNTIME_DAY (TRANSACTION_ID,EQUPMENT_ID,DEVICE_ID,DOWNTIME_DATE,DOWNTIME) values (:1, :2, :3, :4, :5)", (ID_i,EQ_ID_i,dev_id_i,Date_i,Down_Time_i))
        conn.commit()
        #logging.info('Data Inserted %s'%ip_address)
        #conn.close()
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

       

def insert_into_database_switch_day(ID_i,EQ_ID_i,dev_id_i,Date_i,Down_Time_i,conn):
    try:
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("insert into ATMS.ATMS_CR_DOWNTIME_DAY (TRANSACTION_ID,EQUPMENT_ID,DEVICE_ID,DOWNTIME_DATE,DOWNTIME) values (:1, :2, :3, :4, :5)", (ID_i,EQ_ID_i,dev_id_i,Date_i,Down_Time_i))
        conn.commit()
        #logging.info('Data Inserted %s'%ip_address)
        #conn.close()
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

        
def insert_into_down_time_cr(down_time,i,j,last_status,status_time,ip_add,conn):
    try:
        Tran_id_cr_up_down=None
        yesterday_p = datetime.now() - timedelta(days=1)
        status_Date_p = yesterday_p.strftime('%Y/%m/%d')
        status_Date_p = status_Date_p+" 23:59:59"
        status_date_p = datetime.strptime(status_Date_p, "%Y/%m/%d %H:%M:%S")
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("SELECT MAX(TRAN_ID) FROM ATMS.ATMS_CR_ASSET_DOWN_TIME")
        for row in cur:
            Tran_id_cr_up_down = row[0]

        if Tran_id_cr_up_down is None:
            Tran_id_cr_up_down=1
        else:
            Tran_id_cr_up_down+=1
            
        cur.execute("insert into ATMS.ATMS_CR_ASSET_DOWN_TIME (TRAN_ID,DEV_ID,IP,END_TIME,STATUS,EQUIP_ID,DOWN,START_TIME) values (:1, :2, :3, :4, :5, :6, :7, :8)", (Tran_id_cr_up_down,j,ip_add,status_date_p,last_status,i,down_time,status_time))
        conn.commit()
        #conn.close()
        logging.info('Data Inserted %s'%ip_add)
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)


def insert_into_down_time__equipment(down_time,i,j,last_status,status_time,ip_add,conn):
    try:
        Tran_id_cr_up_down=None
        yesterday_p = datetime.now() - timedelta(days=1)
        status_Date_p = yesterday_p.strftime('%Y/%m/%d')
        status_Date_p = status_Date_p+" 23:59:59"
        status_date_p = datetime.strptime(status_Date_p, "%Y/%m/%d %H:%M:%S")
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("SELECT MAX(TRAN_ID) FROM ATMS.DOWN_TIME_RECORD")
        for row in cur:
            Tran_id_cr_up_down = row[0]
        if Tran_id_cr_up_down is None:
            Tran_id_cr_up_down=1
        else:
            Tran_id_cr_up_down+=1
        cur.execute("insert into ATMS.DOWN_TIME_RECORD (TRAN_ID,DEV_ID,IP,END_TIME,STATUS,EQUIP_ID,DOWN,START_TIME) values (:1, :2, :3, :4, :5, :6, :7, :8)", (Tran_id_cr_up_down,j,ip_add,status_date_p,last_status,i,down_time,status_time))
        conn.commit()
        #conn.close()
        logging.info('Data Inserted %s'%ip_add)
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)


def insert_into_down_time_switch(down_time,i,j,last_status,status_time,ip_add,conn):
    try:
        Tran_id_cr_up_down=None
        yesterday_p = datetime.now() - timedelta(days=1)
        status_Date_p = yesterday_p.strftime('%Y/%m/%d')
        status_Date_p = status_Date_p+" 23:59:59"
        status_date_p = datetime.strptime(status_Date_p, "%Y/%m/%d %H:%M:%S")
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("SELECT MAX(TRAN_ID) FROM ATMS.ATMS_CR_ASSET_DOWN_TIME")
        for row in cur:
            Tran_id_cr_up_down = row[0]
        if Tran_id_cr_up_down is None:
            Tran_id_cr_up_down=1
        else:
            Tran_id_cr_up_down+=1
        cur.execute("insert into ATMS.ATMS_CR_ASSET_DOWN_TIME (TRAN_ID,DEV_ID,IP,END_TIME,STATUS,EQUIP_ID,DOWN,START_TIME) values (:1, :2, :3, :4, :5, :6, :7, :8)", (Tran_id_cr_up_down,j,ip_add,status_date_p,last_status,i,down_time,status_time))
        conn.commit()
        #conn.close()
        logging.info('Data Inserted %s'%ip_add)
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

        
def control_room_daywise():
    factors = (60, 1, 1/60)
    yesterday = datetime.now() - timedelta(days=1)
    dt = yesterday.strftime('%d-%b-%y').upper()
    Blank = False
    #df2 = 0
    #dtt = str(dtt)
    #print(dt)
    try:
        conn = cx_Oracle.connect('%s'%login_parameter)
        query = ("select * from ATMS.ATMS_CR_ASSET_DOWN_TIME  where to_date(END_TIME,'DD-MON-YY')='{}'".format(dt))
        #query = ("SELECT * From ATMS.DOWN_TIME_RECORD WHERE to_char(ATMS.DOWN_TIME_RECORD.END_TIME,'MM/dd/yyyy') =%s"%dtt)
        df = pd.read_sql(query, con=conn)
        #conn.close()
        #print(df.shape)
    except Exception as e:
        logging.error(e, exc_info=True)

    #print("run")
    try:
        Tran_id2 = None
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("SELECT MAX(TRANSACTION_ID) FROM ATMS.ATMS_CR_DOWNTIME_DAY")
        for row in cur:
            Tran_id2 = row[0]
        if Tran_id2 is None:
            Tran_id2 = 0
        #conn.close()
    except Exception as e:
        logging.error(e, exc_info=True)
    
    Tran_id2+=1
    try:
        #print(len(df),Tran_id)
        if len(df)!=0:
            df['Date'] = df['END_TIME'].dt.date
            df2 = df.groupby(['EQUIP_ID',"DEV_ID","Date"], as_index=False)['DOWN'].sum()
            #print("df2",df2.shape)
        if len(df)==0:
            Blank = True 
    except Exception as e:
        logging.error(e, exc_info=True)
                
    #print(Blank)
    Equipment_list = [6,7,8,9,10,14]
    device_list = []
    for i in range(0,16):
        i_str = str(i)
        device_list.append(i_str)

    for i in Equipment_list:
        for j in device_list:
            try:
                if Blank==True:
                    #print("hello")
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME,IP from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID={} AND EQUIP_ID={} order by TRAN_ID desc) where rownum =1".format(j,i))
                    cur.execute(query)
                    for row in cur:
                        #print(row)
                        #down_time = 0
                        last_status = row[0]
                        status_time = row[1]
                        ip_add = row[2]
                        yesterday_3 = datetime.now() - timedelta(days=1)
                        status_Date = yesterday_3.strftime('%Y%m%d')
                        #status_Date = status_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        #status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = status_time.strftime('%Y-%m-%d')
                        #print(type(status_Date),type(status_Date_check))
                        #print(type(last_status_Date),type(status_Date_check))
                        #print(last_status_Date,status_Date_check)
                        if last_status_Date==status_Date_check:
                            #print("hello")
                            #print(last_status,last_status_Date)
                            if last_status==0:
                                #print("done")
                                last_status_time =  status_time.strftime('%H:%M:%S')
                                #print("last_status_time",last_status_time)
                                try:
                                    t1 = sum(p*q for p, q in zip(map(int, last_status_time.split(':')), factors))
                                    #print("t1",t1)
                                    last_status_time_t1 = t1%1440
                                    down_time = 1440-last_status_time_t1
                                    #print(i,j,"down",down_time)
                                except Exception as e:
                                    logging.error("Exception occurred", exc_info=True)
                            if last_status ==1:
                                down_time  = 0
                        if last_status_Date!=status_Date_check:
                            if last_status==0:
                                down_time=1440
                            if last_status==1:
                                down_time=0
                        down_time = int(down_time)
                        #print(type(Tran_id),type(i),type(j),type(status_Date),type(down_time))
                        insert_into_cr_downtime(Tran_id2,i,j,status_Date,down_time,conn)
                        Tran_id2+=1  
                        #print(last_status)
                        if last_status_Date!=status_Date_check:
                            if last_status==0:
                                yesterday_p_start = datetime.now() - timedelta(days=1)
                                status_Date_p_start = yesterday_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                        if last_status==0:
                            #print("----inserted----")
                            insert_into_down_time_cr(down_time,i,j,last_status,status_time,ip_add,conn)
                            
                    #conn.close()
            except Exception as e:
                logging.error("Exception occurred", exc_info=True)

            if Blank==False:
                df4 = df2[(df2.EQUIP_ID == i) & (df2.DEV_ID == j)]
                #print("df4",df4)
                #print("length",len(df4))
                #print(len(df4))
                if len(df4)==1:
                    lst = df4.values.tolist()
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME,IP from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID={} AND EQUIP_ID={} order by TRAN_ID desc) where rownum =1".format(j,i))
                    cur.execute(query)
                    for row in cur:
                        #print(row)
                        down_time =0
                        last_status = row[0]
                        status_time = row[1]
                        ip_add = row[2]
                        yesterday_3 = datetime.now() - timedelta(days=1)
                        status_Date = yesterday_3.strftime('%Y%m%d')
                        #status_Date = status_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = status_time.strftime('%Y-%m-%d')
                        #print(type(last_status_Date),type(status_Date_check))
                        #print(last_status_Date,status_Date_check)
                        #print("date",last_status_Date,status_Date_check)
                        if last_status_Date==status_Date_check:
                            #print("hello")
                            #print(last_status,last_status_Date)
                            if last_status==0:
                                #print("done")
                                last_status_time =  status_time.strftime('%H:%M:%S')
                                #print("last_status_time",last_status_time)
                                try:
                                    t1 = sum(p*q for p, q in zip(map(int, last_status_time.split(':')), factors))
                                    #print("t1",t1)
                                    last_status_time_t1 = t1%1440
                                    down_time = 1440-last_status_time_t1
                                    #print(i,j,"down",down_time)
                                except Exception as e:
                                    logging.error("Exception occurred", exc_info=True)
                            if last_status ==1:
                                down_time  = 0
                    yesterday_prev = datetime.now() - timedelta(days=1)
                    dt_prev = yesterday_prev.strftime('%d-%b-%y').upper()
                    query_4 = ("select * from ATMS_CR_ASSET_DOWN_TIME where DEV_ID={} and EQUIP_ID={} and to_date(END_TIME,'DD-MON-YY')='{}'".format(j,i,dt_prev))
                    df_convert = pd.read_sql(query_4, con=conn)
                    #print(df_convert.shape)
                    def test(a,b,c):
                        factors = (60, 1, 1/60)
                        dt11 = a.strftime('%y-%m-%d')
                        dt12 = b.strftime('%y-%m-%d')
                        #print(dt11,dt12)
                        if dt11==dt12:
                            return c
                        else:
                            last_down_time =  a.strftime('%H:%M:%S')
                            #print(last_down_time)
                            t_2 = sum(j*k for j, k in zip(map(int, last_down_time.split(':')), factors))
                            #print("t1",t1)
                            last_down_time_t_2 = t_2%1440
                            #print(last_down_time_t_2)
                            return last_down_time_t_2
                    #print("converted",df_convert)      
                    df_convert["cal_time"] = df_convert.apply(lambda x: test(x.END_TIME,x.START_TIME,x.DOWN), axis=1)
                    Total = df_convert['cal_time'].sum()
                    down_time_copy = down_time
                    down_time = Total+down_time
                    down_time = int(down_time)
                    #print("df2 blank",Tran_id,lst[0][0],lst[0][1],lst[0][2],lst[0][3])
                    #print(Tran_id,i,j,status_Date,down_time)
                    insert_into_cr_downtime(Tran_id2,lst[0][0],lst[0][1],lst[0][2],down_time,conn)
                    Tran_id2+=1
                    if last_status_Date!=status_Date_check:
                        if last_status==0:
                            yesterday_p_start = datetime.now() - timedelta(days=1)
                            status_Date_p_start = yesterday_p_start.strftime('%Y/%m/%d')
                            status_Date_p_start = status_Date_p_start+" 00:00:01"
                            status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")

                    if last_status==0:
                        insert_into_down_time_cr(down_time_copy,i,j,last_status,status_time,ip_add,conn)
                         
                if len(df4)==0:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME,IP from ATMS.ATMS_CONTROL_ROOM_ASSET_STATUS where DEV_ID={} AND EQUIP_ID={} order by TRAN_ID desc) where rownum =1".format(j,i))
                    cur.execute(query)
                    for row in cur:
                        #print(row)
                        last_status = row[0]
                        status_time = row[1]
                        ip_add = row[2]
                        yesterday_3 = datetime.now() - timedelta(days=1)
                        status_Date = yesterday_3.strftime('%Y%m%d')
                        #status_Date = status_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        #status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = status_time.strftime('%Y-%m-%d')
                        #print(type(status_Date),type(status_Date_check))
                        #print(type(last_status_Date),type(status_Date_check))
                        #print(last_status_Date,status_Date_check)
                        if last_status_Date==status_Date_check:
                            #print("hello")
                            #print(last_status,last_status_Date)
                            if last_status==0:
                                #print("done")
                                last_status_time =  status_time.strftime('%H:%M:%S')
                                #print("last_status_time",last_status_time)
                                try:
                                    t1 = sum(p*q for p, q in zip(map(int, last_status_time.split(':')), factors))
                                    #print("t1",t1)
                                    last_status_time_t1 = t1%1440
                                    down_time = 1440-last_status_time_t1
                                    #print(i,j,"down",down_time)
                                except Exception as e:
                                    logging.error("Exception occurred", exc_info=True)
                            if last_status ==1:
                                down_time  = 0
                        if last_status_Date!=status_Date_check:
                            if last_status==0:
                                down_time=1440
                            if last_status==1:
                                down_time=0
                        down_time = int(down_time)
                        #print("df4 not blank",Tran_id,i,j,status_Date,down_time)
                        insert_into_cr_downtime(Tran_id2,i,j,status_Date,down_time,conn)
                        Tran_id2+=1
                        
                        if last_status_Date!=status_Date_check:
                            if last_status==0:
                                yesterday_p_start = datetime.now() - timedelta(days=1)
                                status_Date_p_start = yesterday_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")

                        if last_status==0:
                            insert_into_down_time_cr(down_time,i,j,last_status,status_time,ip_add,conn)
                    #conn.close()
    conn.close()
  
                
def field_equipment():
    factors = (60, 1, 1/60)
    yesterday = datetime.now() - timedelta(days=1)
    dt = yesterday.strftime('%d-%b-%y').upper()
    Blank = False
    #df2 = 0
    #dtt = str(dtt)
    #print(dt)
    try:
        conn = cx_Oracle.connect('%s'%login_parameter)
        query = ("select * from ATMS.DOWN_TIME_RECORD  where to_date(END_TIME,'DD-MON-YY')='{}'".format(dt))
        #query = ("SELECT * From ATMS.DOWN_TIME_RECORD WHERE to_char(ATMS.DOWN_TIME_RECORD.END_TIME,'MM/dd/yyyy') =%s"%dtt)
        df = pd.read_sql(query, con=conn)
        #conn.close()
        #print(df.shape)
    except Exception as e:
        logging.error(e, exc_info=True)

    try:
        Tran_id = None
        #conn = cx_Oracle.connect('%s'%login_parameter)
        cur = conn.cursor()
        cur.execute("SELECT MAX(TRANSACTION_ID) FROM ATMS.ATMS_DOWNTIME_DAY")
        for row in cur:
            Tran_id = row[0]
        if Tran_id is None:
            Tran_id = 0
        #conn.close()
    except Exception as e:
        logging.error(e, exc_info=True)
    
    Tran_id+=1
    try:
        #print(len(df),Tran_id)
        if len(df)!=0:
            df['Date'] = df['END_TIME'].dt.date
            df2 = df.groupby(['EQUIP_ID',"DEV_ID","Date"], as_index=False)['DOWN'].sum()
            #print("df2",df2.shape)
        if len(df)==0:
            Blank = True 
    except Exception as e:
        logging.error(e, exc_info=True)
                
    Equipment_list = [1,2,3,11,12,13]
    #Equipment_list = [1]

    for i in Equipment_list:
        device_list = []
        if i==1:
            for k in range(1,26):
                i_str = str(k)
                device_list.append(i_str)
        if i==2:
            for k in range(100,188):
                i_str = str(k)
                device_list.append(i_str)
            #for l in range(500,504):
                #i_str = str(l)
                #device_list.append(i_str)
        if i==3:
            for k in range(1,4):
                i_str = str(k)
                device_list.append(i_str)
        #if i==4:
         #   for k in range(1,11):
          #      i_str = str(k)
           #     device_list.append(i_str)
        #if i==5:
         #   for k in range(1,11):
          #      i_str = str(k)
           #     device_list.append(i_str)
        if i==11:
            for k in range(1,11):
                i_str = str(k)
                device_list.append(i_str)
        if i==12:
            for k in range(1,11):
                i_str = str(k)
                device_list.append(i_str)
        if i==13:
            for k in range(1,5):
                i_str = str(k)
                device_list.append(i_str)
        for j in device_list:
            try:
                if Blank==True:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME,IP from ATMS.TBL_UP_DOWN where DEV_ID={} AND EQUIP_ID={} order by TRAN_ID desc) where rownum =1".format(j,i))
                    cur.execute(query)
                    for row in cur:
                        #print(row)
                        down_time = 0
                        last_status = row[0]
                        status_time = row[1]
                        ip_add = row[2]
                        yesterday_3 = datetime.now() - timedelta(days=1)
                        status_Date = yesterday_3.strftime('%Y%m%d')
                        #status_Date = status_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        #status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = status_time.strftime('%Y-%m-%d')
                        #print(type(status_Date),type(status_Date_check))
                        #print(type(last_status_Date),type(status_Date_check))
                        #print(last_status_Date,status_Date_check)
                        if last_status_Date==status_Date_check:
                            #print("hello")
                            #print(last_status,last_status_Date)
                            if last_status==0:
                                #print("done")
                                last_status_time =  status_time.strftime('%H:%M:%S')
                                #print("last_status_time",last_status_time)
                                try:
                                    t1 = sum(p*q for p, q in zip(map(int, last_status_time.split(':')), factors))
                                    #print("t1",t1)
                                    last_status_time_t1 = t1%1440
                                    down_time = 1440-last_status_time_t1
                                    #print(i,j,"down",down_time)
                                except Exception as e:
                                    logging.error("Exception occurred", exc_info=True)
                            if last_status ==1:
                                down_time  = 0
                        if last_status_Date!=status_Date_check:
                            if last_status==0:
                                down_time=1440
                            if last_status==1:
                                down_time=0
                        down_time = int(down_time)
                        #print(type(Tran_id),type(i),type(j),type(status_Date),type(down_time))
                        #print("1",Tran_id,i,j,status_Date,down_time)
                        #print(type(status_Date))
                        #print("whole blank",Tran_id,i,j,status_Date,down_time)
                        #print(type(Tran_id),type(i),type(j),type(status_Date),type(down_time))
                        insert_into_database_3(Tran_id,i,j,status_Date,down_time,conn)
                        Tran_id+=1
                        if last_status_Date!=status_Date_check:
                            if last_status==0:
                                yesterday_p_start = datetime.now() - timedelta(days=1)
                                status_Date_p_start = yesterday_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                        if last_status==0:
                            #print("----inserted----")
                            insert_into_down_time__equipment(down_time,i,j,last_status,status_time,ip_add,conn)

                    #conn.close()
            except Exception as e:
                logging.error("Exception occurred", exc_info=True)
            
            if Blank==False:
                df4 = df2[(df2.EQUIP_ID == i) & (df2.DEV_ID == j)]
                #print(df4)
                #print(len(df4))
                if len(df4)==1:
                    lst = df4.values.tolist()
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME,IP from ATMS.TBL_UP_DOWN where DEV_ID={} AND EQUIP_ID={} order by TRAN_ID desc) where rownum =1".format(j,i))
                    cur.execute(query)
                    for row in cur:
                        #print(row)
                        down_time =0
                        last_status = row[0]
                        status_time = row[1]
                        ip_add = row[2]
                        yesterday_3 = datetime.now() - timedelta(days=1)
                        status_Date = yesterday_3.strftime('%Y%m%d')
                        #status_Date = status_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = status_time.strftime('%Y-%m-%d')
                        #print(type(last_status_Date),type(status_Date_check))
                        #print(last_status_Date,status_Date_check)
                        #print("date",last_status_Date,status_Date_check)
                        if last_status_Date==status_Date_check:
                            #print("hello")
                            #print(last_status,last_status_Date)
                            if last_status==0:
                                #print("done")
                                last_status_time =  status_time.strftime('%H:%M:%S')
                                #print("last_status_time",last_status_time)
                                try:
                                    t1 = sum(p*q for p, q in zip(map(int, last_status_time.split(':')), factors))
                                    #print("t1",t1)
                                    last_status_time_t1 = t1%1440
                                    down_time = 1440-last_status_time_t1
                                    #print(i,j,"down",down_time)
                                except Exception as e:
                                    logging.error("Exception occurred", exc_info=True)
                            if last_status ==1:
                                down_time  = 0
    
                        #print(lst)
                        #print(lst[0][2])
                        #print(type(lst[0][2]))
                    yesterday_prev = datetime.now() - timedelta(days=1)
                    dt_prev = yesterday_prev.strftime('%d-%b-%y').upper()
                    query_4 = ("select * from ATMS.DOWN_TIME_RECORD where DEV_ID={} and EQUIP_ID={} and to_date(END_TIME,'DD-MON-YY')='{}'".format(j,i,dt_prev))
                    df_convert = pd.read_sql(query_4, con=conn)
                    def test(a,b,c):
                        factors = (60, 1, 1/60)
                        dt11 = a.strftime('%y-%m-%d')
                        dt12 = b.strftime('%y-%m-%d')
                        #print(dt11,dt12)
                        if dt11==dt12:
                            return c
                        else:
                            last_down_time =  a.strftime('%H:%M:%S')
                            #print(last_down_time)
                            t_2 = sum(j*k for j, k in zip(map(int, last_down_time.split(':')), factors))
                            #print("t1",t1)
                            last_down_time_t_2 = t_2%1440
                            #print(last_down_time_t_2)
                            return last_down_time_t_2
                    df_convert["cal_time"] = df_convert.apply(lambda x: test(x.END_TIME,x.START_TIME,x.DOWN), axis=1)
                    Total = df_convert['cal_time'].sum()
                    #previous_down_time = previous_down_time%1440
                    down_time_copy = down_time
                    down_time = Total+down_time
                    down_time = int(down_time)
                    #down_time = down_time%1440
                    #print("2",Tran_id,lst[0][0],lst[0][1],lst[0][2],type(lst[0][3]))
                    #print("2",Tran_id,lst[0][0],lst[0][1],lst[0][2],down_time)
                    #print(Tran_id,i,j,status_Date,down_time)
                    insert_into_database_3(Tran_id,lst[0][0],lst[0][1],lst[0][2],down_time,conn)
                    Tran_id+=1

                    if last_status_Date!=status_Date_check:
                        if last_status==0:
                            yesterday_p_start = datetime.now() - timedelta(days=1)
                            status_Date_p_start = yesterday_p_start.strftime('%Y/%m/%d')
                            status_Date_p_start = status_Date_p_start+" 00:00:01"
                            status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                    if last_status==0:
                        #print("----inserted----")
                        insert_into_down_time__equipment(down_time_copy,i,j,last_status,status_time,ip_add,conn)

                    
                if len(df4)==0:
                    #conn = cx_Oracle.connect('%s'%login_parameter)
                    cur = conn.cursor()
                    query = ("select * from (select STATUS,STATUS_TIME,IP from ATMS.TBL_UP_DOWN where DEV_ID={} AND EQUIP_ID={} order by TRAN_ID desc) where rownum =1".format(j,i))
                    cur.execute(query)
                    for row in cur:
                        #print(row)
                        last_status = row[0]
                        status_time = row[1]
                        ip_add = row[2]
                        yesterday_3 = datetime.now() - timedelta(days=1)
                        status_Date = yesterday_3.strftime('%Y%m%d')
                        #status_Date = status_time.strftime('%Y%m%d')
                        status_Date = datetime.strptime(status_Date, "%Y%m%d").date()
                        status_Date_check = status_Date.strftime('%Y-%m-%d')
                        last_status_Date = status_time.strftime('%Y-%m-%d')
                        #print(type(last_status_Date),type(status_Date_check))
                        #print(last_status_Date,status_Date_check)
                        #print("date",last_status_Date,status_Date_check)
                        if last_status_Date==status_Date_check:
                            #print("Hello")
                            #print(last_status,last_status_Date)
                            if last_status==0:
                                #print("done")
                                last_status_time =  status_time.strftime('%H:%M:%S')
                                #print("last_status_time",last_status_time)
                                try:
                                    t1 = sum(p*q for p, q in zip(map(int, last_status_time.split(':')), factors))
                                    #print("t1",t1)
                                    last_status_time_t1 = t1%1440
                                    down_time = 1440-last_status_time_t1
                                    #print(i,j,"down",down_time)
                                except Exception as e:
                                    logging.error("Exception occurred", exc_info=True)
                            if last_status ==1:
                                down_time  = 0
                        if last_status_Date!=status_Date_check:
                            if last_status==0:
                                down_time=1440
                            if last_status==1:
                                down_time=0
                        down_time = int(down_time)
                        #print("3",Tran_id,i,j,status_Date,down_time)
                        #print(i,j,last_status,down_time)
                        #print("df4 not blank",Tran_id,i,j,status_Date,down_time)
                        insert_into_database_3(Tran_id,i,j,status_Date,down_time,conn)
                        Tran_id+=1
                        if last_status_Date!=status_Date_check:
                            if last_status==0:
                                yesterday_p_start = datetime.now() - timedelta(days=1)
                                status_Date_p_start = yesterday_p_start.strftime('%Y/%m/%d')
                                status_Date_p_start = status_Date_p_start+" 00:00:01"
                                status_time = datetime.strptime(status_Date_p_start, "%Y/%m/%d %H:%M:%S")
                        if last_status==0:
                            #print("----inserted----")
                            insert_into_down_time__equipment(down_time,i,j,last_status,status_time,ip_add,conn)

    conn.close()
          




  
        

while True:
    #field_equipment()
    #control_room_daywise()
    run_time = datetime.now().strftime('%H')
    run_time = int(run_time)
    run_time_m = datetime.now().strftime('%M')
    run_time_m = int(run_time_m)
    time.sleep(20)
    if run_time==0 and run_time_m==1:
        control_room_daywise()
        #control_room_daywise()
        field_equipment()
        time.sleep(10000)
