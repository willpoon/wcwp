rename BASS1.G_S_22049_MONTH to G_S_22049_MONTH_20110101;

--22049空中充值点情况
CREATE TABLE BASS1.G_S_22049_MONTH
 (TIME_ID        INTEGER         NOT NULL,
  STATMONTH      CHARACTER(6)    NOT NULL,--月份
  CMCC_ID        CHARACTER(5)    NOT NULL,--所属CMCC运营公司标识
  CHONGZHITYPE   CHARACTER(1)    NOT NULL,--空中充值点类型
  WANGDIANCOUNT  CHARACTER(8),            --网点数量
  CHONGZHICOUNT  CHARACTER(8),            --充值次数
  CHONGZHIMONEY  CHARACTER(14),           --充值金额
  CHOUJIN        CHARACTER(10)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22049_MONTH
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;


--配置调度
insert into app.sch_control_before
values('BASS1_G_S_22049_MONTH.tcl','REP_stat_channel_reward_0007.tcl');
commit;


---此接口已处理上线
--22064实体渠道重点增值业务办理信息
CREATE TABLE BASS1.G_S_22064_MONTH
 (TIME_ID        INTEGER         NOT NULL,
  STATMONTH      CHARACTER(6)    NOT NULL,--月份
  CHANNEL_ID     CHARACTER(25)   NOT NULL,--实体渠道标识
  ACCEPT_TYPE    CHARACTER(1)    NOT NULL,--办理类型(人工前台/自助终端)
  IMP_ACCEPTTYPE CHARACTER(2)    NOT NULL,--重点增值业务类型
  CNT            CHARACTER(10)            --业务办理量
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22064_MONTH
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;


--配置调度
insert into app.sch_control_before values ('BASS1_G_S_22064_MONTH.tcl','BASS2_Dw_channel_info_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22064_MONTH.tcl','BASS2_Dw_cm_busi_radius_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22064_MONTH.tcl','BASS2_Dw_product_ord_so_log_dm.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22064_MONTH.tcl','BASS2_Dw_product_ord_cust_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22064_MONTH.tcl','BASS2_Dw_product_ord_offer_dm.tcl');
insert into app.sch_control_before values ('BASS1_EXP_G_S_22064_MONTH','BASS1_G_S_22064_MONTH.tcl');

insert into app.sch_control_task values ('BASS1_G_S_22064_MONTH.tcl',2,2,'int -s G_S_22064_MONTH.tcl',0,512,'实体渠道重点增值业务办理信息','app','BASS1',1,'/bassapp/bass1/tcl/');
insert into app.sch_control_task values ('BASS1_EXP_G_S_22064_MONTH',2,2,'bass1_export BASS1.G_S_22064_MONTH LASTMONTH()',0,-1,'实体渠道重点增值业务办理信息导出','app','BASS1',1,'/bassapp/backapp/bin/bass1_export/');

insert into app.sch_control_map values (2,'G_S_22064_MONTH.tcl','BASS1_G_S_22064_MONTH.tcl');

insert into bass1.int_program_data (PROGRAM_TYPE,PROGRAM_NAME,SOURCE_DATA,OBJECTIVE_DATA,FINAL_DATA) values ('d','G_S_22064_MONTH.tcl','22064.bass1','22064_e','22064_f');
insert into app.g_unit_info values ('22064',1,'实体渠道重点增值业务办理信息导出','bass1.g_s_22064_month',1,0,0);
commit;



----------------------------------



insert into app.sch_control_before values ('BASS1_G_S_22062_MONTH.tcl','BASS1_G_S_22064_MONTH.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22062_MONTH.tcl','BASS2_Dw_product_ord_cust_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22062_MONTH.tcl','BASS2_Dw_product_ord_offer_dm.tcl');
commit;



--22065 电子渠道情况
CREATE TABLE BASS1.G_S_22065_MONTH
 (TIME_ID        INTEGER         NOT NULL,
  STATMONTH      CHARACTER(6)    NOT NULL,--月份
  CMCC_ID        CHARACTER(5)    NOT NULL,--所属CMCC运营公司标识
  ZZHI_CNT       CHARACTER(10),           --增值业务办理量
  TERM_CNT       CHARACTER(8),            --终端销售量
  CARD_CNT       CHARACTER(8),            --卡号销售量
  PAYMENT_CNT    CHARACTER(10),           --电子充值缴费笔数
  OTHER_CNT      CHARACTER(10),           --其它业务办理量
  E_PAY_AMOUNT   CHARACTER(10),           --电子交费金额
  O_TERM_AMOUNT  CHARACTER(10),           --其中自助终端交费金额
  ZHI_CUST_CNT   CHARACTER(8),            --自有电子渠道登陆客户数
  E_CUST_CNT     CHARACTER(8),            --电子渠道登陆客户数
  TX_CUST_CNT    CHARACTER(10)            --通信客户数
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,CMCC_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22065_MONTH
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;


--配置调度
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_acct_payment_dm.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_product_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_kf_sms_cmd_receive_dm.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_product_ord_cust_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_product_ord_offer_dm.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_kf_cmd_hint_def_dm.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_custsvc_agent_tele_dm.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','REP_stat_channel_reward_0007.tcl');

insert into app.sch_control_before values ('BASS1_EXP_G_S_22065_MONTH','BASS1_G_S_22065_MONTH.tcl');

insert into app.sch_control_task values ('BASS1_G_S_22065_MONTH.tcl',2,2,'int -s G_S_22065_MONTH.tcl',0,512,'电子渠道情况','app','BASS1',1,'/bassapp/bass1/tcl/');
insert into app.sch_control_task values ('BASS1_EXP_G_S_22065_MONTH',2,2,'bass1_export BASS1.G_S_22065_MONTH LASTMONTH()',0,-1,'电子渠道情况导出','app','BASS1',1,'/bassapp/backapp/bin/bass1_export/');

insert into app.sch_control_map values (2,'G_S_22065_MONTH.tcl','BASS1_G_S_22065_MONTH.tcl');

insert into bass1.int_program_data (PROGRAM_TYPE,PROGRAM_NAME,SOURCE_DATA,OBJECTIVE_DATA,FINAL_DATA) values ('d','G_S_22065_MONTH.tcl','22065.bass1','22065_e','22065_f');
insert into app.g_unit_info values ('22065',1,'电子渠道情况导出','bass1.g_s_22065_month',1,0,0);
commit;






