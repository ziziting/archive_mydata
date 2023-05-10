# -*- coding: UTF-8 -*-
'''

'''
from const import *
import argparse


#Parameters
parent_parser = argparse.ArgumentParser(add_help=False)
parent_parser.add_argument("--help", action="help", help="Table Archive")
parent_parser.add_argument('-h', dest='source_host', action='store', help='source host')
parent_parser.add_argument('-p', dest='source_port', action='store', help='source port')
parent_parser.add_argument('-D', dest='source_dbname', action='store', help='source db name')
parent_parser.add_argument('-t', dest='tbname', action='store', help='archive table name')
parent_parser.add_argument('-n', '--row', dest='row', action='store', default=10000, help='Batch Rows default=10000')

#parent_parser2
parent_parser2 = argparse.ArgumentParser(add_help=False)
parent_parser2.add_argument('-H', '--arhost', dest='archive_host', action='store',default=ARCHIVE_HOST, help=f'archive host,default = {ARCHIVE_HOST}')
parent_parser2.add_argument('-P', '--arport', dest='archive_port', action='store',default=ARCHIVE_PORT, help=f'archive port = {ARCHIVE_PORT}')
parent_parser2.add_argument('-d', '--ardb', dest='archive_dbname', action='store',default='', help='archive db name ,default = {db}_dw')

##parent_parser3
parent_parser3 = argparse.ArgumentParser(add_help=False)
parent_parser3.add_argument('-q', '--sql', dest='sql', action='store', default='1=1', help='condition sql,default = "1=1"')
parent_parser3.add_argument('-i', '--idx', dest='idx', action='store',  help='index_name')

#Main Parser
parser = argparse.ArgumentParser(add_help=False,conflict_handler='resolve')
parser.add_argument("--help", action="help", help="Table Archive")
subparsers = parser.add_subparsers(title='task-mode', dest='taskmode', help='task-mode:[all-file,all-db,full-db,incr-db]')

#task-mode
allfile_parser = subparsers.add_parser('all-file', help='Archiving of entire table data as files',parents=[parent_parser], conflict_handler='resolve')
alldb_parser = subparsers.add_parser('all-db',help='Remote archiving of entire tables',parents=[parent_parser,parent_parser2], conflict_handler='resolve')
fulldb_parser = subparsers.add_parser('full-db', help='Remote archiving of partial data',parents=[parent_parser,parent_parser2,parent_parser3], conflict_handler='resolve')
incrdb_parser = subparsers.add_parser('incr-db', help='Remote archiving of incremental data',parents=[parent_parser,parent_parser2,parent_parser3], conflict_handler='resolve')
incrdb_parser.add_argument('-c', '--cfg', dest='cfg', action='store', default=CFG_FILE, help=f'cfg file path,default = {CFG_FILE}')
purge_parser = subparsers.add_parser('del-data', help='Delete data with pt tool',parents=[parent_parser,parent_parser3], conflict_handler='resolve')
args = parser.parse_args()

taskmode = args.taskmode
source_host = args.source_host
source_port = args.source_port
db_name = args.source_dbname
tb_name = args.tbname
row = args.row

if taskmode == 'del-data':
    cdt_sql = args.sql
    index_name = args.idx

if  taskmode != "all-file" and taskmode != "del-data":
    archive_host = args.archive_host
    archive_port = args.archive_port
    if args.archive_dbname == '':
        archvie_db = f"{db_name}_dw"
    else:
        archvie_db = args.archive_dbname

    if taskmode != 'all-db':
        cdt_sql = args.sql
        index_name = args.idx

        if taskmode != 'full-db':
            cfg = args.cfg









