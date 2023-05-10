# -*- coding: UTF-8 -*-
import re
import subprocess
import sys
import os
import config_parser
from const import *
from datetime import datetime, timedelta
from ptarchive import DataPtArchive
from argparserget import *
from tenacity import retry, wait_fixed, stop_after_attempt
import multiprocessing
import alerts
import pymysql

def recordinfo(log_file,info):
    with open(log_file, "a") as f:
        f.write(f"""{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  {info} \n""")


#mydumper 导出表结构
def schemadump(host, port, db_name, tb_name, timestr, log_file,task_mode=None):
    time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    recordinfo(log_file,f"[SCHEMA_DUMP][info] {host}:{port}/{db_name} {tb_name} 正在导出表结构")
    dump_schema_file = f"""{EXPORT_DIR}/{port}/{db_name}/{tb_name}/{timestr}/schema"""
    if not os.path.exists(dump_schema_file):
        os.makedirs(dump_schema_file)
    dump_cmd = f"""{DUMPER_TOOL} -h {host} -P {port} -u {DUMP_USER} -p {DUMP_PWD}  -v 3 -B {db_name} -T {tb_name} -d -e  --less-locking --skip-tz-utc -o {dump_schema_file} """
    schemafile = f"{dump_schema_file}/{db_name}.{tb_name}-schema.sql"
    try:
        exec_code, output = subprocess.getstatusoutput(dump_cmd)
        if exec_code != 0:
            if task_mode == 'incr-db':
                errinfo = f"Failed to export {host}:{port}/{db_name} {tb_name} schema using mydumper. Error: {output}"
                alerts.info(errinfo, time_str)
            raise Exception(
                f"Failed to export {host}:{port}/{db_name} {tb_name} schema using mydumper. Error: {output}")
        else:
            with open(schemafile, 'r') as f:
                data = f.read()
            data = data.replace('InnoDB', 'RocksDB').replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
            with open(schemafile, 'w') as f:
                f.write(data)

            print(f"[SCHEMA_DUMP][info] {host}:{port}/{db_name} {tb_name} 成功导出表结构，文件路径:{dump_schema_file}")
            recordinfo(log_file,
                       f"[SCHEMA_DUMP][info] {host}:{port}/{db_name} {tb_name} 成功导出表结构，文件路径:{dump_schema_file}")

    except Exception as e:
        print(f"[SCHEMA_DUMP][error] {e}")
        recordinfo(log_file, f"[SCHEMA_DUMP][error] {e}")
        if task_mode != 'incr-db':
            sys.exit()

    return dump_schema_file

#mydumper 导出表数据
def datadump(host, port, db_name, tb_name, timestr ,row , log_file):
    recordinfo(log_file, f"[DATA_DUMP][info] 正在导出表数据")
    dump_data_file = f"""{EXPORT_DIR}/{port}/{db_name}/{tb_name}/{timestr}/data"""
    if not os.path.exists(dump_data_file):
        os.makedirs(dump_data_file)
    dump_cmd = f"""mydumper -h {host} -P {port} -u {DUMP_USER} -p {DUMP_PWD} -t 16 -r {row} -v 3 -B {db_name} -T {tb_name} -C -c  -m --less-locking --skip-tz-utc -o {dump_data_file} """
    try:
        exec_code, output = subprocess.getstatusoutput(dump_cmd)
        if exec_code != 0:
            raise Exception(
                f"Failed to export {host}:{port}/{db_name}.{tb_name} data using mydumper. Error: {output}")
        else:
            print(f"[DATA_DUMP][info] 成功导出数据,保存路径：{dump_data_file}")
            recordinfo(log_file, f"[DATA_DUMP][info] 成功导出数据,保存路径：{dump_data_file}")
    except Exception as e:
        print(f"[DATA_DUMP][error] {e}")
        recordinfo(log_file, f"[DATA_DUMP][error] {e}")


    return dump_data_file


#myloader 导入表结构和数据
def dataload(host, port, db_name, tb_name, retore_file, log_file, task_mode = None):
    time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    recordinfo(log_file, f"[DATA_LOAD][info] 正在导入文件{retore_file}到归档数据库{host}:{port}/{db_name}.{tb_name}")
    load_cmd = f"""{LOAD_TOOL} -h {host} -u {DUMP_USER} -p {DUMP_PWD} -P {port}  -B {db_name} -d {retore_file} -e """
    try:
        exec_code, output = subprocess.getstatusoutput(load_cmd)
        if exec_code != 0:
            if task_mode == 'incr-db':
                errinfo = f"Failed to load {host}:{port}/{db_name}.{tb_name}  using myloader. Error: {output}"
                alerts.info(errinfo, time_str)
            raise Exception(
                f"Failed to load {host}:{port}/{db_name}.{tb_name}  using myloader. Error: {output}")
        else:
            print(f"[DATA_LOAD][info] 成功导入文件：{retore_file} 到{host}:{port}/{db_name}.{tb_name}中")
            recordinfo(log_file, f"[DATA_LOAD][info] 成功导入文件：{retore_file} 到{host}:{port}/{db_name}.{tb_name}中")

    except Exception as e:
        print(f"[DATA_LOAD][error]导入归档数据失败： {e}")
        recordinfo(log_file, f"[DATA_LOAD][error]导入归档数据失败： {e}")
        if task_mode != 'incr-db':
            sys.exit()

def myindex(host, port, dbname, tbname, ix):
    port = int(port)
    db = pymysql.connect(host=host, port=port, user=DUMP_USER, password=DUMP_PWD, database=dbname)
    cursor = db.cursor()
    sql = f"show index from {tbname} where Key_name = '{ix}'"
    cursor.execute(sql)
    data = cursor.fetchone()
    db.close()
    return data

#pt-archive 存量归档
def ptarchive(host, port, db_name, tb_name, ix_name, dest_dbname, cdt_sql, tnx , archvie_host, archive_port, task_mode, log_file):
    time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    recordinfo(log_file, f"[SYNC][info] Syncing {host}:{port}/{db_name}.{tb_name} to {archvie_host}:{archive_port}/{dest_dbname}.{tb_name} using pt-archiver tool")

    getindex = myindex(host, port, db_name, tb_name, ix_name)
    try:
        if getindex is None:
            if task_mode == "incr-db":
                ixerrinfo = f"{host}:{port}/{db_name}.{tb_name} 归档失败!!\n\n Error:{ix_name} 索引不存在！"
                alerts.info(time_str, ixerrinfo)
            raise Exception(
                f"{host}:{port}/{db_name}.{tb_name} 归档失败!! Error:{ix_name} 索引不存在！")
        else:
            recordinfo(log_file, f"[SYNC][info] Index {ix_name} exists..  continuing ")
            ptarexec = DataPtArchive(host, port, db_name, tb_name, ix_name, dest_dbname, cdt_sql, tnx , archvie_host,archive_port)
            try:
                exec_code, output =ptarexec.Data_Archive()
                if exec_code != 0:
                    if task_mode == "incr-db":
                        errinfo = f"{host}:{port}/{db_name}.{tb_name} 归档失败!!\n\n Error: {output}"
                        alerts.info(time_str, errinfo)
                    raise Exception(f"{host}:{port}  {db_name}.{tb_name}  archive failed using pt-archiver tool. Error: {output}")
                else:
                    recordinfo(log_file, f"[SYNC][info] pt-archiver同步成功！")
                    print(f"[SYNC][info] pt-archiver同步成功！")
            except Exception as e:
                recordinfo(log_file,f"[SYNC][error] {e}")
                print(e)
                if task_mode != 'incr-db':
                    sys.exit()

    except Exception as e:
        recordinfo(log_file,f"[SYNC][error] {e}")
        print(e)
        if task_mode != 'incr-db':
            sys.exit()


#pt-archive增量归档
def criarchive(arhost, arport, row , cfg_file):
    task_mode = 'incr-db'
    time_str = datetime.now().strftime("%Y%m%d%H%M%S")
    daystr = datetime.now().strftime("%Y%m%d")
    data_arcihve_cfg = config_parser.DataArchiveConfig(cfg_file)
    archive_cfg_list = data_arcihve_cfg.get_configs()
    pool = multiprocessing.Pool(processes=5)
    for archive_cfg in archive_cfg_list:
        source_host = archive_cfg.get("source_host")
        source_port = archive_cfg.get("source_port")
        db_name = archive_cfg.get("db_name")
        ardbname = f"{db_name}_dw"
        tb_names = archive_cfg.get("tb_name")
        ix_name = archive_cfg.get("ix_name")
        cdt_sql = archive_cfg.get("condition_sql")
        txn = int(row if archive_cfg.get("txn") == None else archive_cfg.get("txn"))

        tb_name_list = tb_names.replace(' ', '').split(',')
        for tb_name in tb_name_list:
            log_file = f"{LOG_DIR}/{task_mode}_{daystr}_{db_name}_{tb_name}.log"
            schefile = schemadump(source_host, source_port, db_name, tb_name, time_str, log_file, task_mode)
            dataload(arhost, arport, ardbname, tb_name, schefile, log_file, task_mode)
            pool.apply_async(ptarchive, (source_host, source_port, db_name, tb_name,  ix_name, ardbname,  cdt_sql, txn, arhost, arport, task_mode,log_file))
    pool.close()
    pool.join()


def cleardata(host, port, db_name, tb_name, ix_name, cdt_sql, tnx ,  task_mode, log_file):
    getindex = myindex(host, port, db_name, tb_name, ix_name)
    try:
        if getindex is None:
            if task_mode == "incr-db":
                ixerrinfo = f"{host}:{port}/{db_name}.{tb_name} 归档失败!!\n\n Error:{ix_name} 索引不存在"
                alerts.info(ixerrinfo, time_str)
            raise Exception(
                f"{host}:{port}/{db_name}.{tb_name} archive failed!! Error:Index {ix_name} does not exist")
        else:
            recordinfo(log_file, f"[SYNC][info] Index {ix_name} exists..")

            ptdelexec = DataPtArchive(host, port, db_name, tb_name, ix_name, '', cdt_sql, tnx)
            try:
                exec_code, output =ptdelexec.Data_Purge()
                if exec_code != 0:
                    if task_mode == "incr-db":
                        errinfo = f"{host}:{port}/{db_name}.{tb_name} 清理归档数据失败!!\n\n Error: {output}"
                        alerts.info(errinfo, time_str)
                    raise Exception(f"{host}:{port}  {db_name}.{tb_name}  delete data failed using pt-archiver tool. Error: {output}")
                else:
                    recordinfo(log_file, f"[DEL][info] pt-archiver清理数据完成！")
                    print("[DEL][info] pt-archiver清理数据完成！")
            except Exception as e:
                recordinfo(log_file,f"[DEL][error] {e}")
                print(e)
                if task_mode != 'incr-db':
                    sys.exit()
    except Exception as e:
        recordinfo(log_file,f"[SYNC][error] {e}")
        print(e)
        if task_mode != 'incr-db':
            sys.exit()



if __name__ == '__main__':

    time_str =  datetime.now().strftime("%Y%m%d%H%M%S")
    daystr = datetime.now().strftime("%Y%m%d")
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    log_file = f"{LOG_DIR}/{taskmode}_{daystr}_{db_name}_{tb_name}.log"
    if taskmode == 'all-file':
        if all((source_host, source_port, db_name, tb_name)):
            schemadump(source_host, source_port, db_name, tb_name, time_str, log_file)
            datadump(source_host, source_port, db_name, tb_name, time_str, row, log_file)
        else:
            print("请输入源端数据库连接信息：python data_archive_ctl.py -h {host} -p {port} -D {dbname} -t {tbname} " )
            sys.exit()

    if taskmode == 'all-db':
        if all((source_host, source_port, db_name, tb_name)):
            schefile = schemadump(source_host, source_port, db_name, tb_name, time_str, log_file)
            datafile = datadump(source_host, source_port, db_name, tb_name, time_str, row, log_file)
            dataload(archive_host, archive_port, archvie_db, tb_name, schefile, log_file)
            dataload(archive_host, archive_port, archvie_db, tb_name, datafile, log_file)
        else:
            print("请输入源端数据库连接信息：ip:port 、 dbname 、tbname " )
            sys.exit()

    if taskmode == 'full-db':
        if all((source_host, source_port, db_name, tb_name, index_name,cdt_sql)):
            schefile = schemadump(source_host, source_port, db_name, tb_name, time_str, log_file)
            dataload(archive_host, archive_port, archvie_db, tb_name, schefile, log_file)
            ptarchive(source_host, source_port, db_name, tb_name, index_name, archvie_db, cdt_sql, row, archive_host, archive_port,taskmode,log_file)
        else:
            print("请输入源端数据库连接信息：ip:port 、 dbname 、tbname 、archive_sql 、 index_name " )
            sys.exit()
    if  taskmode == 'incr-db':
        criarchive(archive_host, archive_port, row , cfg)

    if taskmode == 'del-data':
        if all((source_host, source_port, db_name, tb_name, index_name, cdt_sql)):
            deldata = cleardata(source_host, source_port, db_name, tb_name, index_name, cdt_sql, row, taskmode, log_file)
        else:
            print("请输入源端数据库连接信息：ip:port 、 dbname 、tbname 、archive_sql 、 index_name " )
            sys.exit()
    rmlogcmd = f''' find {LOG_DIR} -name "*.log" -mtime +30  -exec rm -f {{}} \; '''
    subprocess.run(rmlogcmd, shell=True)
