import argparse
import paramiko
import re
import psycopg2
import os

parser = argparse.ArgumentParser()

parser.add_argument('--host', dest='host', help='Host to connect to')
parser.add_argument('--port', dest='port', default=22, help="Port to connect on", type=int)
parser.add_argument('--user', dest='user', help="User name to authenticate as")
parser.add_argument('--password', dest='password', help="User password")
parser.add_argument('--dbname', dest='dbname', help="Database name")
parser.add_argument('--dbuser', dest='dbuser', help="Database user name")
parser.add_argument('--dbpassword', dest='dbpassword', help="Database password")
parser.add_argument('--keypath', dest='keypath', help="path to ppk")
parser.add_argument('--locallogpath', dest='locallogpath', help="Local directory path logs downloaded from remote server")
args = parser.parse_args()


# step 1 ssh connection with remote server and copy logs
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname=args.host, port=args.port, username=args.user, key_filename=args.keypath)
sftp = ssh.open_sftp()
# remote_log_root_path = "/var/www/logs/"
remote_log_root_path = "/opt/cb-fn-connector/cb_fn_connector/logs/"
stdin, stdout, stderr = ssh.exec_command("ls /opt/cb-fn-connector/cb_fn_connector/logs/")
remotefiles = stdout.readlines()
locallogpath = args.locallogpath
if not os.path.exists(locallogpath):
    os.mkdir(locallogpath)
for fl in remotefiles:
    fl = fl[:-1]
    if fl != "starweb_plus.log":
        continue
    remotefilepath = remote_log_root_path + fl
    if not os.path.exists(locallogpath + fl):
        print("+ Downloading... file: " + fl)
        sftp.get(remotefilepath, locallogpath + fl)
        print("= Download finished. file: " + fl)
sftp.close()
ssh.close()


#step 2 initialize connection with postgresql database and create table
print("+ connecting database...")
conn = psycopg2.connect(
       database=args.dbname,
        user=args.dbuser,
        password=args.dbpassword,
        host='localhost',
        port= '5432'
    )

print("= connected")
conn.autocommit = True
cursor = conn.cursor()
checktbl = 'select * from WEBPLUS;'
try:
    res = cursor.execute(checktbl)
except:
    sql = 'CREATE TABLE WEBPLUS(id SERIAL PRIMARY KEY, user_id char(255), server_id varchar(255), server_ip varchar(20), servername varchar(255), appname varchar(255), logdate varchar(20), logtime varchar(20), logtype varchar(20), logentry varchar(4096), traceback varchar(65535));'
    cursor.execute(sql)


#step 3 parse logs and insert into database

logfiles = [lf for lf in os.listdir(locallogpath) if os.path.isfile(os.path.join(locallogpath, lf))]
for lf in logfiles:
    if lf != "starweb_plus.log":
        continue
    try:
        fp = open(locallogpath + lf, "r")
        lines = fp.readlines()
    except:
        continue
    LOG_LINE_REGEX = r"^\[(?P<time>\d{2}\/[a-zA-Z]{3}\/\d{4}\s\d{2}\:\d{2}\:\d{2})\]\s(?P<type>[a-zA-Z]+)\[(?P<appname>.*?)\:(?P<username>.*?)\]\s(?P<entry>.*)"
    pattern = re.compile(LOG_LINE_REGEX)
    new_array = []
    tracebackline = -100
    tracebackline_index_innewarray = -100
    print("+ parsing file: " + lf)
    for i in range(len(lines)):
        m = pattern.match(lines[i])
        if m:
            res = m.groupdict()
            res["traceback"] = ""
            new_array.append(res)
            if res["type"] == "ERROR" and "Traceback" in res["entry"]:
                tracebackline = i
                tracebackline_index_innewarray = len(new_array)-1
        else:
            if i == tracebackline + 1:
                new_array[tracebackline_index_innewarray]["traceback"] += lines[i] + "\n"
                tracebackline = i

    if len(new_array) == 0:
        print("= no matched pattern, file: " + lf)
        continue
    dictionary = {}
    i = 0
    for item in new_array:
        i += 1
        dictionary["entry_" + str(i)] = (item['username'], "server_id", "server_ip", "servername", item["appname"], item["time"][:11], item["time"][11:], item["type"], item["entry"], item["traceback"])

    print("+ writing into database, file: " + lf)
    columns = dictionary.keys()
    for i in dictionary.values():
        # sql2 = """INSERT INTO LOGDUMPS (user_id, server_id, server_ip, servername, appname, logdate, logtime, logtype, logentry) VALUES {};""".format(i)
        cursor.execute("""INSERT INTO WEBPLUS (user_id, server_id, server_ip, servername, appname, logdate, logtime, logtype, logentry, traceback) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9]))

    if 0:
        sql3 = 'select * from WEBPLUS;'
        cursor.execute(sql3)
        for i in cursor.fetchall():
            print(i)

    conn.commit()
    print("= finshed to write into db, file: " + lf)
conn.close()
print("Done")