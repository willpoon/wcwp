-- 429	新增接口22307（移动400业务收入明细）	1.6.8	2010-10-22	自数据日期201010起生效上传


--  22307（移动400业务收入明细）
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

insert into app.sch_control_task values ('BASS1_G_S_22307_MONTH.tcl',2,2,'int -s BASS1_G_S_22307_MONTH.tcl',0,-1,'移动400业务收入明细','app','BASS1',1,'/bassapp/bass1/tcl/');
insert into app.sch_control_task values ('BASS1_EXP_G_S_22307_MONTH',2,2,'bass1_export BASS1.BASS1_G_S_22307_MONTH LASTMONTH()',0,-1,'移动400业务收入明细导出','app','BASS1',1,'/bassapp/backapp/bin/bass1_export/');

insert into app.sch_control_map values (2,'G_S_22307_MONTH.tcl','BASS1_G_S_22307_MONTH.tcl');
insert into bass1.int_program_data (PROGRAM_TYPE,PROGRAM_NAME,SOURCE_DATA,OBJECTIVE_DATA,FINAL_DATA) values ('d','G_S_22307_MONTH.tcl','22307.bass1','22307_e','22307_f');
insert into app.g_unit_info values ('22307',1,'移动400业务收入明细导出','bass1.g_s_22307_month',1,0,1);


/*
430
新增BASS_STD1_0108 （ 集团业务类型）维值编码M2M（车务通）、M2M（其它）； 删除维值编码M2M（行业应用卡）。
针对02054、02058、02059、02062接口对应的原“M2M（行业应用卡）”订购关系记录，
将由一级经分系统11月2日将其失效日期统一置为20101031。
自数据日期20101101起，上述接口按最新维值重新上报M2M类业务订购关系（并注意首次上传订购状态为“正常”的全量记录
自数据日期20101101起生效（首次上传M2M类业务订购状态为“正常”的全量订购关系记录）
*/


update BASS1.ALL_DIM_LKP_160 
set bass1_value='1241' , bass1_value_desc='M2M（车务通）'
where bass1_tbid='BASS_STD1_0108' and bass1_value='1240'
and xzbas_value='942' ;

update BASS1.ALL_DIM_LKP_160 
set bass1_value='1249' , bass1_value_desc='M2M（其它）'
where bass1_tbid='BASS_STD1_0108' and bass1_value='1240'
and xzbas_value not in ('942') ;

--两份代码
1.用以下4份(代码修改成了M2M业务状态正常全量数据) 跑20101101数据 
G_A_02054_DAY.tcl
G_A_02058_DAY.tcl
G_A_02059_DAY.tcl
G_A_02062_DAY.tcl

2. 出完20101101数据后 用以下代码还原回去 
G_A_02054_DAY - 原代码.tcl
G_A_02058_DAY - 原代码.tcl
G_A_02059_DAY - 原代码修改后.tcl
G_A_02062_DAY - 原代码修改后.tcl


--备份数据

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




