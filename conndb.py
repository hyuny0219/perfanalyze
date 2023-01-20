import cx_Oracle

cx_Oracle.init_oracle_client(lib_dir=r"./lib")

def connectDB(a, b, c):
    conn_string = a
    conn_id = b
    conn_pwd = c
    try:
        conn = cx_Oracle.connect(conn_id, conn_pwd, conn_string)
        return conn
    except cx_Oracle.DatabaseError as result:
        return str(result)

def connectDBINFO(a, b, c, d):
    conn_string = a
    conn_id = b
    conn_pwd = c
    conn_instid = d
    try:
        conn = cx_Oracle.connect(conn_id, conn_pwd, conn_string)
        cursor = conn.cursor()
        #sql = "select b.dbid, a.instance_number, a.instance_name, a.host_name, a.version, b.platform_name  from gv$instance a, gv$database b where a.inst_id = :instid"
        sql = """select b.dbid,
                       a.instance_number,
                       a.instance_name,
                       a.host_name,
                       a.version,
                       b.platform_name, 
                       c.snap_id,
                       to_char(c.BEGIN_INTERVAL_TIME,'YYYY-MM-DD HH24:MI') as begin_dt,
                       to_char(c.END_INTERVAL_TIME,'YYYY-MM-DD HH24:MI') as end_dt 
                  from gv$instance a,
                       gv$database b,
                       dba_hist_snapshot c
                 where a.inst_id = b.inst_id
                   and a.inst_id = c.instance_number
                   and c.INSTANCE_NUMBER = :instid
                   order by c.snap_id"""
        cursor.execute(sql, instid=conn_instid)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except cx_Oracle.DatabaseError as result:
        cursor.close()
        conn.close()
        return str(result)


def isSingle(a, b, c):
    conn_string = a
    conn_id = b
    conn_pwd = c
    try:
        conn = cx_Oracle.connect(conn_id, conn_pwd, conn_string)
        cursor = conn.cursor()
        sql = "select count(1) from gv$instance"
        cursor.execute(sql)
        result = cursor.fetchall()
        row = int(result[0][0])

        if row >= 2:
            resultText = "Single 인스턴스가 아닙니다."
            cursor.close()
            conn.close()
            return str(resultText)
        else:
            cursor.close()
            conn.close()
            return row
    except cx_Oracle.DatabaseError as result:
        cursor.close()
        conn.close()
        return str(result)

def isRAC(a, b, c):
    conn_string = a
    conn_id = b
    conn_pwd = c
    try:
        conn = cx_Oracle.connect(conn_id, conn_pwd, conn_string)
        cursor = conn.cursor()
        sql = "select instance_number from gv$instance order by 1"
        cursor.execute(sql)
        result = cursor.fetchall()

        if len(result) >=2:
            cursor.close()
            conn.close()
            return result
        else:
            resultText = "RAC 인스턴스가 아닙니다."
            cursor.close()
            conn.close()
            return str(resultText)
    except cx_Oracle.DatabaseError as result:
        cursor.close()
        conn.close()
        return str(result)

