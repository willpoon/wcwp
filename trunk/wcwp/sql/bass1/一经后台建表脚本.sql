--一经数据导出数据日志
drop   table APP.G_RUNLOG;
create table APP.G_RUNLOG
(   
time_id     integer     not null,   --抽取日期yyyymm[dd]
unit_code 　varchar(10) not null,   --接口编号[G_BUS_CHECK_ALL_DAY 每日提供的业务指标校验文件 (编号99991),G_BUS_CHECK_BILL_MONTH  每月10日前提供的业务指标校验文件(编号99993)]
coarse      integer     not null,   --接口单元粒度:0 表示日，1 表示月,9 初始化
export_time timestamp,              --抽取时间
export_num  integer,                --重复抽取次数 初始为0
return_flag integer,                --0 未通过校验，1 通过验证
data_file   varchar(100),　         --数据文件名
verf_file   varchar(100),           --校验文件名
primary key(time_id,unit_code,coarse)
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( time_id,unit_code ) USING HASHING
NOT LOGGED INITIALLY;

--一经导出数据单元配置
drop   table APP.G_UNIT_INFO;
create table APP.G_UNIT_INFO
(
unit_code  varchar(10)  not null,  --接口单元编码
coarse     integer      not null,  --接口单元粒度:0 表示日，1 表示月,9 初始化
unit_name  varchar(100) not null,  --接口单元名称
table_name varchar(100) not null,  --带模式表名[唯一]
put_flag   integer      not null,  --接口单元是否允许上传：0 拒绝，1 允许
put_mode   integer      not null,  --接口单元上传方式：0 重传方式
put_zero   integer      not null,  --接口单元是否允许上传空文件：0 拒绝，1 允许
primary key (unit_code,coarse)
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( unit_code,coarse ) USING HASHING
NOT LOGGED INITIALLY;
create unique index app.ind_unit_tablename on APP.G_UNIT_INFO(unit_code,coarse,table_name);


--文件级校验报告信息表
drop   table APP.G_FILE_REPORT;
create table APP.G_FILE_REPORT
(
FILENAME       VARCHAR(40)  , --文件名 
DEAL_TIME      VARCHAR(20)  , --处理时间 
ERR_CODE       CHARACTER(2)   --错误代码
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( FILENAME ) USING HASHING
NOT LOGGED INITIALLY; 

--记录及外键信息
drop   table APP.G_REC_REPORT;
create table APP.G_REC_REPORT
(
FILENAME    VARCHAR(40),   --文件名  
REC_NUM     BIGINT,        --记录行号           
ERR_CODE    CHARACTER(8)   --错误代码
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( FILENAME ) USING HASHING
NOT LOGGED INITIALLY;
   
--业务指标校验报告
drop   table APP.G_TARGET_REPORT;
create table APP.G_TARGET_REPORT
( 
  filename    varchar(40),--文件名
  target_code char(2),    --指标编号
  value_2     bigint,     --二级经营分析值
  value_1     bigint,     --一级经营分析值
  value_diff  bigint      --差额值
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( filename ) USING HASHING
NOT LOGGED INITIALLY;

--存放集团下发的USER_ID
drop   TABLE BASS1.G_USER_LST;
CREATE TABLE BASS1.G_USER_LST
(
  TIME_ID  INTEGER,
  USER_ID  VARCHAR(20)
)
DATA CAPTURE NONE
IN TBS_APP_BASS1
INDEX IN TBS_INDEX
PARTITIONING KEY(TIME_ID,USER_ID) USING HASHING
NOT LOGGED INITIALLY;


------------------------------------------------维表信息---------------------------------------------------------------

--文件级校验报告错误维表
drop   table APP.G_FILE_REPORT_ERR;
create table APP.G_FILE_REPORT_ERR
(
err_code   char(2),         --错误代码
err_desc   varchar(100),    --错误信息
err_reason varchar(100)     --错误原因 
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( err_code ) USING HASHING
NOT LOGGED INITIALLY;
   
--记录级校验和外键校验报告错误信息维表
drop   table APP.G_REC_REPORT_ERR;
create table APP.G_REC_REPORT_ERR
(
unit_code char(5),      --接口单元编号  
attr_code char(2),      --属性编号      
attr_name varchar(100), --属性名称      
attr_desc varchar(600), --属性描述      
err_code  char(8),      --错误代码      
err_desc  varchar(600)  --错误信息      
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( unit_code,attr_code ) USING HASHING
NOT LOGGED INITIALLY;

                       