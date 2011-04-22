--һ�����ݵ���������־
drop   table APP.G_RUNLOG;
create table APP.G_RUNLOG
(   
time_id     integer     not null,   --��ȡ����yyyymm[dd]
unit_code ��varchar(10) not null,   --�ӿڱ��[G_BUS_CHECK_ALL_DAY ÿ���ṩ��ҵ��ָ��У���ļ� (���99991),G_BUS_CHECK_BILL_MONTH  ÿ��10��ǰ�ṩ��ҵ��ָ��У���ļ�(���99993)]
coarse      integer     not null,   --�ӿڵ�Ԫ����:0 ��ʾ�գ�1 ��ʾ��,9 ��ʼ��
export_time timestamp,              --��ȡʱ��
export_num  integer,                --�ظ���ȡ���� ��ʼΪ0
return_flag integer,                --0 δͨ��У�飬1 ͨ����֤
data_file   varchar(100),��         --�����ļ���
verf_file   varchar(100),           --У���ļ���
primary key(time_id,unit_code,coarse)
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( time_id,unit_code ) USING HASHING
NOT LOGGED INITIALLY;

--һ���������ݵ�Ԫ����
drop   table APP.G_UNIT_INFO;
create table APP.G_UNIT_INFO
(
unit_code  varchar(10)  not null,  --�ӿڵ�Ԫ����
coarse     integer      not null,  --�ӿڵ�Ԫ����:0 ��ʾ�գ�1 ��ʾ��,9 ��ʼ��
unit_name  varchar(100) not null,  --�ӿڵ�Ԫ����
table_name varchar(100) not null,  --��ģʽ����[Ψһ]
put_flag   integer      not null,  --�ӿڵ�Ԫ�Ƿ������ϴ���0 �ܾ���1 ����
put_mode   integer      not null,  --�ӿڵ�Ԫ�ϴ���ʽ��0 �ش���ʽ
put_zero   integer      not null,  --�ӿڵ�Ԫ�Ƿ������ϴ����ļ���0 �ܾ���1 ����
primary key (unit_code,coarse)
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( unit_code,coarse ) USING HASHING
NOT LOGGED INITIALLY;
create unique index app.ind_unit_tablename on APP.G_UNIT_INFO(unit_code,coarse,table_name);


--�ļ���У�鱨����Ϣ��
drop   table APP.G_FILE_REPORT;
create table APP.G_FILE_REPORT
(
FILENAME       VARCHAR(40)  , --�ļ��� 
DEAL_TIME      VARCHAR(20)  , --����ʱ�� 
ERR_CODE       CHARACTER(2)   --�������
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( FILENAME ) USING HASHING
NOT LOGGED INITIALLY; 

--��¼�������Ϣ
drop   table APP.G_REC_REPORT;
create table APP.G_REC_REPORT
(
FILENAME    VARCHAR(40),   --�ļ���  
REC_NUM     BIGINT,        --��¼�к�           
ERR_CODE    CHARACTER(8)   --�������
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( FILENAME ) USING HASHING
NOT LOGGED INITIALLY;
   
--ҵ��ָ��У�鱨��
drop   table APP.G_TARGET_REPORT;
create table APP.G_TARGET_REPORT
( 
  filename    varchar(40),--�ļ���
  target_code char(2),    --ָ����
  value_2     bigint,     --������Ӫ����ֵ
  value_1     bigint,     --һ����Ӫ����ֵ
  value_diff  bigint      --���ֵ
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( filename ) USING HASHING
NOT LOGGED INITIALLY;

--��ż����·���USER_ID
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


------------------------------------------------ά����Ϣ---------------------------------------------------------------

--�ļ���У�鱨�����ά��
drop   table APP.G_FILE_REPORT_ERR;
create table APP.G_FILE_REPORT_ERR
(
err_code   char(2),         --�������
err_desc   varchar(100),    --������Ϣ
err_reason varchar(100)     --����ԭ�� 
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( err_code ) USING HASHING
NOT LOGGED INITIALLY;
   
--��¼��У������У�鱨�������Ϣά��
drop   table APP.G_REC_REPORT_ERR;
create table APP.G_REC_REPORT_ERR
(
unit_code char(5),      --�ӿڵ�Ԫ���  
attr_code char(2),      --���Ա��      
attr_name varchar(100), --��������      
attr_desc varchar(600), --��������      
err_code  char(8),      --�������      
err_desc  varchar(600)  --������Ϣ      
)
DATA CAPTURE NONE
IN TBS_APP_OTHER  
INDEX IN TBS_INDEX
PARTITIONING KEY( unit_code,attr_code ) USING HASHING
NOT LOGGED INITIALLY;

                       