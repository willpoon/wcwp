-- 429	�����ӿ�22307���ƶ�400ҵ��������ϸ��	1.6.8	2010-10-22	����������201010����Ч�ϴ�


--  22307���ƶ�400ҵ��������ϸ��
drop table BASS1.G_S_22307_MONTH;
CREATE TABLE BASS1.G_S_22307_MONTH (
	 TIME_ID           INTEGER         NOT NULL
	,ENTERPRISE_ID 			 CHARACTER(20) 	 NOT NULL
	,NUM4001					 CHARACTER(10) 	 NOT NULL
	,INCOME						 CHARACTER(12) 	 NOT NULL
) DISTRIBUTE BY HASH( ENTERPRISE_ID )
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;



insert into app.sch_control_before values ('BASS1_G_S_22307_MONTH.tcl','BASS2_Dw_acct_shoulditem_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22307_MONTH.tcl','BASS2_Dw_enterprise_sub_ds.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22307_MONTH.tcl','BASS2_Dw_enterprise_msg_ds.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22307_MONTH.tcl','BASS2_Dw_enterprise_member_ds_his.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22307_MONTH.tcl','BASS2_Dwd_group_order_featur_ds.tcl');
insert into app.sch_control_before values ('BASS1_EXP_G_S_22307_MONTH','BASS1_G_S_22307_MONTH.tcl');

insert into app.sch_control_task values ('BASS1_G_S_22307_MONTH.tcl',2,2,'int -s BASS1_G_S_22307_MONTH.tcl',0,-1,'�ƶ�400ҵ��������ϸ','app','BASS1',1,'/bassapp/bass1/tcl/');
insert into app.sch_control_task values ('BASS1_EXP_G_S_22307_MONTH',2,2,'bass1_export BASS1.BASS1_G_S_22307_MONTH LASTMONTH()',0,-1,'�ƶ�400ҵ��������ϸ����','app','BASS1',1,'/bassapp/backapp/bin/bass1_export/');

insert into app.sch_control_map values (2,'G_S_22307_MONTH.tcl','BASS1_G_S_22307_MONTH.tcl');
insert into bass1.int_program_data (PROGRAM_TYPE,PROGRAM_NAME,SOURCE_DATA,OBJECTIVE_DATA,FINAL_DATA) values ('d','G_S_22307_MONTH.tcl','22307.bass1','22307_e','22307_f');
insert into app.g_unit_info values ('22307',1,'�ƶ�400ҵ��������ϸ����','bass1.g_s_22307_month',1,0,1);


/*
430
����BASS_STD1_0108 �� ����ҵ�����ͣ�άֵ����M2M������ͨ����M2M���������� ɾ��άֵ����M2M����ҵӦ�ÿ�����
���02054��02058��02059��02062�ӿڶ�Ӧ��ԭ��M2M����ҵӦ�ÿ�����������ϵ��¼��
����һ������ϵͳ11��2�ս���ʧЧ����ͳһ��Ϊ20101031��
����������20101101�������ӿڰ�����άֵ�����ϱ�M2M��ҵ�񶩹���ϵ����ע���״��ϴ�����״̬Ϊ����������ȫ����¼
����������20101101����Ч���״��ϴ�M2M��ҵ�񶩹�״̬Ϊ����������ȫ��������ϵ��¼��
*/


update BASS1.ALL_DIM_LKP_160 
set bass1_value='1241' , bass1_value_desc='M2M������ͨ��'
where bass1_tbid='BASS_STD1_0108' and bass1_value='1240'
and xzbas_value='942' ;

update BASS1.ALL_DIM_LKP_160 
set bass1_value='1249' , bass1_value_desc='M2M��������'
where bass1_tbid='BASS_STD1_0108' and bass1_value='1240'
and xzbas_value not in ('942') ;

--���ݴ���
1.������4��(�����޸ĳ���M2Mҵ��״̬����ȫ������) ��20101101���� 
G_A_02054_DAY.tcl
G_A_02058_DAY.tcl
G_A_02059_DAY.tcl
G_A_02062_DAY.tcl

2. ����20101101���ݺ� �����´��뻹ԭ��ȥ 
G_A_02054_DAY - ԭ����.tcl
G_A_02058_DAY - ԭ����.tcl
G_A_02059_DAY - ԭ�����޸ĺ�.tcl
G_A_02062_DAY - ԭ�����޸ĺ�.tcl


--��������

CREATE TABLE BASS1.G_A_02054_DAY_20101101bak
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    TIME_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_A_02054_DAY_20101101bak
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;


insert into BASS1.G_A_02054_DAY_20101101bak
select * from BASS1.G_A_02054_DAY;
commit;

delete from BASS1.G_A_02054_DAY
where enterprise_busi_type='1240';
commit;



CREATE TABLE BASS1.G_A_02058_DAY_20101101bak
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  SUB_BUSI_TYPE         CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    TIME_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_A_02058_DAY_20101101bak
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;



insert into BASS1.G_A_02058_DAY_20101101bak
select * from BASS1.G_A_02058_DAY;
commit;

delete from BASS1.G_A_02058_DAY
where enterprise_busi_type='1240';
commit;



CREATE TABLE BASS1.G_A_02059_DAY_20101101bak
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  USER_ID               CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  ORDER_DATE            CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_ID,
    TIME_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_A_02059_DAY_20101101bak
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;


insert into BASS1.G_A_02059_DAY_20101101bak
select * from BASS1.G_A_02059_DAY;
commit;

delete from BASS1.G_A_02059_DAY
where enterprise_busi_type='1240';
commit;



CREATE TABLE BASS1.G_A_02062_DAY_20101101bak
 (TIME_ID               INTEGER,
  ENTERPRISE_ID         CHARACTER(20),
  USER_ID               CHARACTER(20),
  ENTERPRISE_BUSI_TYPE  CHARACTER(4),
  MANAGE_MODE           CHARACTER(1),
  PRODUCT_NO            CHARACTER(15),
  INDUSTRY_ID           CHARACTER(2),
  GPRS_TYPE             CHARACTER(1),
  DATA_SOURCE           CHARACTER(60),
  CREATE_DATE           CHARACTER(8),
  STATUS_ID             CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (USER_ID,
    TIME_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_A_02062_DAY_20101101bak
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;


insert into BASS1.G_A_02062_DAY_20101101bak
select * from BASS1.G_A_02062_DAY;
commit;

delete from BASS1.G_A_02062_DAY
where enterprise_busi_type='1240';
commit;




