# archive_mydata
归档mysql数据

修改
alerts.py : 告警url

const.py : 变量参数

increment.cfg : 设置增量定时归档规则

---------------------------------
执行：
1\ mydumper导出整个表
python3 archive_ctl.py all-file -h {host} -p {port} -D dba_crtdb_test -t account_history_1 -n 10000

2\mydumper导出整个表，并导入到归档库，可传入归档库，也可在const.py文件中配置
python3 archive_ctl.py all-db -h {host} -p {port} -D dba_crtdb_test -t account_history_1 -n 10000


3\用pt进行归档数据，，可传入归档库，也可在const.py文件中配置
python3 archive_ctl.py full-db  -h {host} -p {port} -D dba_crtdb_test -t account_history_2 -q "create_time<'2020-08-01 00:5:00.009'" -i idx_happend_time

python3 archive_ctl.py full-db  -h {host} -p {port} -D dba_crtdb_test -t account_history_2 -q "create_time<'2020-08-01 00:7:00.009' and create_time>'2020-08-01 00:5:00.009'" -i idx_happend_time

4\在increment.cfg配置文件中配置归档表信息
python3 archive_ctl.py incr-db

5\用pt工具清理数据
python3 archive_ctl.py del-data -h {host} -p {port}  -D dba_crtdb_test_dw -t account_history_1 -i idx_happend_time
