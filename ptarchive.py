# -*- coding: UTF-8 -*-
'''

'''
import argparse
import subprocess
from datetime import datetime, timedelta
import os
from const import *

class DataPtArchive:
    def __init__(self, host, port, db_name, tb_name, ix_name, dest_dbname = '', condition_sql = '1=1', tnx = 1000, archvie_host = ARCHIVE_HOST,archive_port = ARCHIVE_PORT):
        self.source_host = host
        self.source_port = port
        self.db_name = db_name
        self.tb_name = tb_name
        self.ix_name = ix_name
        self.tnx = tnx
        self.condition_sql = condition_sql
        self.dest_dbname = dest_dbname
        self.archvie_host = archvie_host
        self.archive_port = archive_port


    def Data_Archive(self):
        pt_cmd = f"""{ARCHIVE_TOOL} --source h={self.source_host},P={self.source_port},u={DUMP_USER},p={DUMP_PWD},D={self.db_name},t={self.tb_name},i={self.ix_name},A=utf8mb4  --dest h={self.archvie_host},P={self.archive_port},u={DUMP_USER},p={DUMP_PWD},D={self.dest_dbname},t={self.tb_name},A=utf8mb4 --progress {self.tnx} --sleep 1  --where "{self.condition_sql}" --statistics  --limit={self.tnx} --txn-size {self.tnx} --max-lag=5 --no-delete"""
        exec_code, output = subprocess.getstatusoutput(pt_cmd)
        # if exec_code != 0:
        #     raise Exception(f"table {self.db_name}.{self.tb_name} dump failed. Error: {output}")
        return exec_code, output

    def Data_Purge(self):
        pt_cmd = f"""{ARCHIVE_TOOL} --source h={self.source_host},P={self.source_port},u={DUMP_USER},p={DUMP_PWD},D={self.db_name},t={self.tb_name},i={self.ix_name},A=utf8mb4   --progress {self.tnx} --sleep 1  --where "{self.condition_sql}" --statistics  --limit={self.tnx} --txn-size {self.tnx} --max-lag=5 --purge """
        exec_code, output = subprocess.getstatusoutput(pt_cmd)
        return exec_code, output

# aa = DataPtArchive('localhost', 3306, 'testdb', 'testtb', 'ix_textcol', 'testdb_dw')
# aa.Data_Archive()
