rename BASS1.G_S_22049_MONTH to G_S_22049_MONTH_20110101;

--22049���г�ֵ�����
CREATE TABLE BASS1.G_S_22049_MONTH
 (TIME_ID        INTEGER         NOT NULL,
  STATMONTH      CHARACTER(6)    NOT NULL,--�·�
  CMCC_ID        CHARACTER(5)    NOT NULL,--����CMCC��Ӫ��˾��ʶ
  CHONGZHITYPE   CHARACTER(1)    NOT NULL,--���г�ֵ������
  WANGDIANCOUNT  CHARACTER(8),            --��������
  CHONGZHICOUNT  CHARACTER(8),            --��ֵ����
  CHONGZHIMONEY  CHARACTER(14),           --��ֵ���
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


--���õ���
insert into app.sch_control_before
values('BASS1_G_S_22049_MONTH.tcl','REP_stat_channel_reward_0007.tcl');
commit;


---�˽ӿ��Ѵ�������
--22064ʵ�������ص���ֵҵ�������Ϣ
CREATE TABLE BASS1.G_S_22064_MONTH
 (TIME_ID        INTEGER         NOT NULL,
  STATMONTH      CHARACTER(6)    NOT NULL,--�·�
  CHANNEL_ID     CHARACTER(25)   NOT NULL,--ʵ��������ʶ
  ACCEPT_TYPE    CHARACTER(1)    NOT NULL,--��������(�˹�ǰ̨/�����ն�)
  IMP_ACCEPTTYPE CHARACTER(2)    NOT NULL,--�ص���ֵҵ������
  CNT            CHARACTER(10)            --ҵ�������
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


--���õ���
insert into app.sch_control_before values ('BASS1_G_S_22064_MONTH.tcl','BASS2_Dw_channel_info_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22064_MONTH.tcl','BASS2_Dw_cm_busi_radius_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22064_MONTH.tcl','BASS2_Dw_product_ord_so_log_dm.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22064_MONTH.tcl','BASS2_Dw_product_ord_cust_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22064_MONTH.tcl','BASS2_Dw_product_ord_offer_dm.tcl');
insert into app.sch_control_before values ('BASS1_EXP_G_S_22064_MONTH','BASS1_G_S_22064_MONTH.tcl');

insert into app.sch_control_task values ('BASS1_G_S_22064_MONTH.tcl',2,2,'int -s G_S_22064_MONTH.tcl',0,512,'ʵ�������ص���ֵҵ�������Ϣ','app','BASS1',1,'/bassapp/bass1/tcl/');
insert into app.sch_control_task values ('BASS1_EXP_G_S_22064_MONTH',2,2,'bass1_export BASS1.G_S_22064_MONTH LASTMONTH()',0,-1,'ʵ�������ص���ֵҵ�������Ϣ����','app','BASS1',1,'/bassapp/backapp/bin/bass1_export/');

insert into app.sch_control_map values (2,'G_S_22064_MONTH.tcl','BASS1_G_S_22064_MONTH.tcl');

insert into bass1.int_program_data (PROGRAM_TYPE,PROGRAM_NAME,SOURCE_DATA,OBJECTIVE_DATA,FINAL_DATA) values ('d','G_S_22064_MONTH.tcl','22064.bass1','22064_e','22064_f');
insert into app.g_unit_info values ('22064',1,'ʵ�������ص���ֵҵ�������Ϣ����','bass1.g_s_22064_month',1,0,0);
commit;



----------------------------------



insert into app.sch_control_before values ('BASS1_G_S_22062_MONTH.tcl','BASS1_G_S_22064_MONTH.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22062_MONTH.tcl','BASS2_Dw_product_ord_cust_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22062_MONTH.tcl','BASS2_Dw_product_ord_offer_dm.tcl');
commit;



--22065 �����������
CREATE TABLE BASS1.G_S_22065_MONTH
 (TIME_ID        INTEGER         NOT NULL,
  STATMONTH      CHARACTER(6)    NOT NULL,--�·�
  CMCC_ID        CHARACTER(5)    NOT NULL,--����CMCC��Ӫ��˾��ʶ
  ZZHI_CNT       CHARACTER(10),           --��ֵҵ�������
  TERM_CNT       CHARACTER(8),            --�ն�������
  CARD_CNT       CHARACTER(8),            --����������
  PAYMENT_CNT    CHARACTER(10),           --���ӳ�ֵ�ɷѱ���
  OTHER_CNT      CHARACTER(10),           --����ҵ�������
  E_PAY_AMOUNT   CHARACTER(10),           --���ӽ��ѽ��
  O_TERM_AMOUNT  CHARACTER(10),           --���������ն˽��ѽ��
  ZHI_CUST_CNT   CHARACTER(8),            --���е���������½�ͻ���
  E_CUST_CNT     CHARACTER(8),            --����������½�ͻ���
  TX_CUST_CNT    CHARACTER(10)            --ͨ�ſͻ���
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


--���õ���
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_acct_payment_dm.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_product_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_kf_sms_cmd_receive_dm.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_product_ord_cust_ms.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_product_ord_offer_dm.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_kf_cmd_hint_def_dm.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','BASS2_Dw_custsvc_agent_tele_dm.tcl');
insert into app.sch_control_before values ('BASS1_G_S_22065_MONTH.tcl','REP_stat_channel_reward_0007.tcl');

insert into app.sch_control_before values ('BASS1_EXP_G_S_22065_MONTH','BASS1_G_S_22065_MONTH.tcl');

insert into app.sch_control_task values ('BASS1_G_S_22065_MONTH.tcl',2,2,'int -s G_S_22065_MONTH.tcl',0,512,'�����������','app','BASS1',1,'/bassapp/bass1/tcl/');
insert into app.sch_control_task values ('BASS1_EXP_G_S_22065_MONTH',2,2,'bass1_export BASS1.G_S_22065_MONTH LASTMONTH()',0,-1,'���������������','app','BASS1',1,'/bassapp/backapp/bin/bass1_export/');

insert into app.sch_control_map values (2,'G_S_22065_MONTH.tcl','BASS1_G_S_22065_MONTH.tcl');

insert into bass1.int_program_data (PROGRAM_TYPE,PROGRAM_NAME,SOURCE_DATA,OBJECTIVE_DATA,FINAL_DATA) values ('d','G_S_22065_MONTH.tcl','22065.bass1','22065_e','22065_f');
insert into app.g_unit_info values ('22065',1,'���������������','bass1.g_s_22065_month',1,0,0);
commit;






