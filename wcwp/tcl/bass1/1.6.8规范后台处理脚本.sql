DROP TABLE BASS1.G_I_06021_MONTH;
DROP TABLE BASS1.G_I_06022_MONTH;
DROP TABLE BASS1.G_I_06023_MONTH;
DROP TABLE BASS1.G_S_22061_MONTH;
DROP TABLE BASS1.G_S_22062_MONTH;
DROP TABLE BASS1.G_S_22063_MONTH;


--06021�ӿڣ�ʵ������������Ϣ
CREATE TABLE BASS1.G_I_06021_MONTH (
	TIME_ID            INTEGER         NOT NULL,   
  CHANNEL_ID         CHARACTER(25)   NOT NULL,   --ʵ��������ʶ
  CHANNEL_TYPE       CHARACTER(1)    NOT NULL,   --ʵ����������
  CMCC_ID            CHARACTER(5)    NOT NULL,   --����CMCC��Ӫ��˾��ʶ
  COUNTRY_NAME       CHARACTER(30)   NOT NULL,   --��������
  THORPE_NAME        CHARACTER(50)   NOT NULL,   --����/Ƭ������
  CHANNEL_NAME       CHARACTER(100)  NOT NULL,   --��������
  CHANNEL_ADDR       CHARACTER(100)  NOT NULL,   --������ַ
  POSITION           CHARACTER(1)    NOT NULL,   --����λ������
  REGION_INFO        CHARACTER(1)    NOT NULL,   --������̬
  CHANNEL_B_TYPE     CHARACTER(1)    NOT NULL,   --������������
  CHANNEL_STAR       CHARACTER(1),               --�����Ǽ�
  CHANNEL_STATUS     CHARACTER(1)    NOT NULL,   --����״̬
  BUSINESS_BEGIN     CHARACTER(4)    NOT NULL,   --Ӫҵ��ʼʱ��
  BUSINESS_END       CHARACTER(4)    NOT NULL,   --Ӫҵ����ʱ��
  VALID_DATE         CHARACTER(8),               --Э��ǩ����Ч����
  EXPIRE_DATE        CHARACTER(8),               --Э���ֹ����
  TIMES              CHARACTER(9),               --�Ѻ�������
  LONGITUDE          CHARACTER(18),              --����
  LATITUDE           CHARACTER(18),              --γ��
  FITMENT_PRICE      CHARACTER(10),              --װ���ۼ�Ͷ���ܶ�
  EQUIP_PRICE        CHARACTER(10),              --�豸�ۼ�Ͷ���ܶ�
  PRICES             CHARACTER(10),              --�칫��Ӫҵ�Ҿ��ۼ�Ͷ���ܶ�
  CHARGE             CHARACTER(10)               --һ������ͷ����
) DISTRIBUTE BY HASH(TIME_ID,CHANNEL_ID)
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;


--06022�ӿڣ�ʵ������������������Ϣ
CREATE TABLE BASS1.G_I_06022_MONTH (
	TIME_ID            INTEGER         NOT NULL,   
  CHANNEL_ID         CHARACTER(25)   NOT NULL,   --ʵ��������ʶ
  OWNER_TYPE         CHARACTER(1)    NOT NULL,   --��ҵ��Դ����
  BUY_MONTH          CHARACTER(6),               --�����·ݡ���������ҵ��Դ����Ϊ"1�����й�˾����"ʱ��������д�������000101����
  FC_LIC             CHARACTER(50),              --����֤�š���������ҵ��Դ����Ϊ��1�����й�˾������ʱ��������д��������ա���
  TD_LIC             CHARACTER(50),              --����֤�š���������ҵ��Դ����Ϊ��1�����й�˾������ʱ��������д��������ա���
  GS_LIC             CHARACTER(50),              --���̺š�������������ҵ��Դ����Ϊ��1�����й�˾������ʱ��������д��������ա���
  BUY_CHARGE         CHARACTER(10),              --�����ܼۡ���������ҵ��Դ����Ϊ��1�����й�˾������ʱ��������д��������ա���λ��Ԫ��
  RENT_BEGIN_DATE    CHARACTER(8),               --���޿�ʼ���ڡ���������ҵ��Դ����Ϊ2��3ʱ��������д�������000101������
  RENT_END_DATE      CHARACTER(8),               --���޽�ֹ���ڡ���������ҵ��Դ����Ϊ2��3ʱ��������д�������000101������
  AV_PRICES          CHARACTER(8)                --��ƽ����𡰵�����������ҵ��Դ����Ϊ2��3ʱ��������д��������ա���
) DISTRIBUTE BY HASH(TIME_ID,CHANNEL_ID)
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;


--06023�ӿڣ�ʵ��������Դ������Ϣ
CREATE TABLE BASS1.G_I_06023_MONTH (
	TIME_ID            INTEGER         NOT NULL,   
  CHANNEL_ID         CHARACTER(25)   NOT NULL,   --ʵ��������ʶ
  BUILD_AREA         CHARACTER(5),               --�������
  USE_AREA           CHARACTER(5),               --ʹ�����
  STORE_AREA         CHARACTER(5),               --ǰ̨Ӫҵ���
  SEAT_NUM           CHARACTER(3),               --̨ϯ����
  STORE_EMPLOYE      CHARACTER(3),               --Ӫҵ��Ա����
  GUARD_EMPLOYE      CHARACTER(3),               --��������
  CLEAR_EMPLOYE      CHARACTER(3),               --��������
  IF_WAIT_MARK       CHARACTER(1),               --�����ŶӽкŻ�
  IF_POS_MARK        CHARACTER(1),               --����POS��
  IF_VIP_SEAT        CHARACTER(1),               --����VIPרϯ
  IF_VIP_ROOM        CHARACTER(1),               --����VIP��
  PRINT_NUM          CHARACTER(3),               --���굥��ӡ��̨��
  TERM_NUM           CHARACTER(3),               --�ۺ��������ն�̨��
  G3_AREA            CHARACTER(5),               --G3���������
  TV_NUM             CHARACTER(3),               --����������
  NEW_BUSITERM_NUM   CHARACTER(3),               --��ҵ������Ӫ��ƽ̨�ն˸���
  HEART_TERM_NUM     CHARACTER(3),               --�Ļ�����ƽ̨�ն˸���
  NET_TERM_NUM       CHARACTER(3),               --����Ӫҵ�������ն˸���
  AREA               CHARACTER(5),               --�������
  ACCEPT_AREA        CHARACTER(5),               --�ƶ����������
  MAIN_NET_TYPE      CHARACTER(1),               --��Ҫ������ʽ
  SUB_NET_TYPE       CHARACTER(1),               --��Ҫ������ʽ
  IF_CZ              CHARACTER(1)                --�ܷ������г�ֵҵ��
) DISTRIBUTE BY HASH(TIME_ID,CHANNEL_ID)
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;



--22061�ӿڣ�ʵ��������Ӫ�ɱ���Ϣ
CREATE TABLE BASS1.G_S_22061_MONTH (
	TIME_ID            INTEGER         NOT NULL,   
	STATMONTH          CHARACTER(6)    NOT NULL,   --�·�
  CHANNEL_ID         CHARACTER(25)   NOT NULL,   --ʵ��������ʶ
  WATER_FEE          CHARACTER(6),               --����ˮ��
  ELE_FEE            CHARACTER(8),               --���µ��
  HEAT_FEE           CHARACTER(8),               --����ȡů��
  WORK_FEE           CHARACTER(8),               --�����˹��ɱ��������
  OFFICE_FEE         CHARACTER(8),               --���°칫��Ʒ���Ĳķ���
  OTHER_FEE          CHARACTER(8)                --���������ճ�����
) DISTRIBUTE BY HASH(TIME_ID,STATMONTH,CHANNEL_ID)
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;



--22062�ӿڣ�ʵ������ҵ�������Ϣ
CREATE TABLE BASS1.G_S_22062_MONTH (
	TIME_ID            INTEGER         NOT NULL,   
	STATMONTH          CHARACTER(6)    NOT NULL,   --�·�
  CHANNEL_ID         CHARACTER(25)   NOT NULL,   --ʵ��������ʶ
  ACCEPT_TYPE        CHARACTER(1),               --��������
  NEW_USERS          CHARACTER(8),               --�����ͻ���
  HAND_CNT           CHARACTER(10),              --�ɷѱ���
  HAND_FEE           CHARACTER(10),              --�ɷѽ���λ��Ԫ��
  CARD_SALE_CNT      CHARACTER(8),               --��ֵ����������
  CARD_SALE_FEE      CHARACTER(10),              --��ֵ�����۽���λ��Ԫ��
  ACCEPT_CNT         CHARACTER(10),              --��ֵҵ��������
  IMP_ACCEPT_CNT     CHARACTER(10),              --�ص���ֵҵ��������
  TERM_SALE_CNT      CHARACTER(10),              --�����ն����۱���
  OTHER_SALE_CNT     CHARACTER(10),              --���ж����ֻ����۱���
  ACCEPT_BAS_CNT     CHARACTER(10),              --����������������
  QUERY_BAS_CNT      CHARACTER(10),              --��ѯ������������
  OFF_ACCEPT_CNT     CHARACTER(8)                --�ײͰ������
) DISTRIBUTE BY HASH(TIME_ID,STATMONTH,CHANNEL_ID)
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;



--22063�ӿڣ�ʵ��������𼰲�����Ϣ
CREATE TABLE BASS1.G_S_22063_MONTH (
	TIME_ID            INTEGER         NOT NULL,   
	STATMONTH          CHARACTER(6)    NOT NULL,   --�·�
  CHANNEL_ID         CHARACTER(25)   NOT NULL,   --ʵ��������ʶ
  FH_REWARD          CHARACTER(10),              --�źų��
  BASIC_REWARD       CHARACTER(10),              --����ҵ����������
  INCR_REWARD        CHARACTER(10),              --��ֵҵ�������
  INSPIRE_REWARD     CHARACTER(10),              --�������
  TERM_REWARD        CHARACTER(10),              --�ն˳��
  RENT_CHARGE        CHARACTER(8)                --���ⲹ��
) DISTRIBUTE BY HASH(TIME_ID,STATMONTH,CHANNEL_ID)
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;




----------------------------------------------------
--ȡ�����ϵĽӿڵ���
--06030(ʵ������)
--22044(��Ӫ�������)
--22045(����������)
--22046(��Ӫ����ҵ�������)
--22047(�������ҵ�������)
--22048(�ֻ��������)
--22051(�������������)
--22053(ʵ�������ջ���)
--22054(ʵ�������»���)

---����app.sch_control_task

update app.sch_control_task set cc_flag=2
where MO_GROUP_CODE='BASS1'
and control_code in
('BASS1_G_S_22054_MONTH.tcl'
,'BASS1_G_S_22053_DAY.tcl'
,'BASS1_G_A_06030_DAY.tcl'
,'BASS1_EXP_G_S_22054_MONTH'
,'BASS1_EXP_G_S_22053_DAY'
,'BASS1_EXP_G_S_22051_MONTH'
,'BASS1_EXP_G_S_22048_MONTH'
,'BASS1_EXP_G_S_22047_MONTH'
,'BASS1_EXP_G_S_22046_MONTH'
,'BASS1_EXP_G_S_22045_MONTH'
,'BASS1_EXP_G_S_22044_MONTH'
,'BASS1_EXP_G_A_06030_DAY');

commit;

update  app.sch_control_task set FUNCTION_DESC='[����]'||FUNCTION_DESC
where MO_GROUP_CODE='BASS1'
and control_code in
('BASS1_G_S_22054_MONTH.tcl'
,'BASS1_G_S_22053_DAY.tcl'
,'BASS1_G_A_06030_DAY.tcl'
,'BASS1_EXP_G_S_22054_MONTH'
,'BASS1_EXP_G_S_22053_DAY'
,'BASS1_EXP_G_S_22051_MONTH'
,'BASS1_EXP_G_S_22048_MONTH'
,'BASS1_EXP_G_S_22047_MONTH'
,'BASS1_EXP_G_S_22046_MONTH'
,'BASS1_EXP_G_S_22045_MONTH'
,'BASS1_EXP_G_S_22044_MONTH'
,'BASS1_EXP_G_A_06030_DAY');

commit;

--ȡ�����Ͻӿ��漰��У��(��ȷ��)
update app.sch_control_task set cc_flag=2
where MO_GROUP_CODE='BASS1'
and control_code in
('BASS1_INT_CHECK_R041_MONTH.tcl'
,'BASS1_INT_CHECK_R042_MONTH.tcl'
,'BASS1_INT_CHECK_R043_MONTH.tcl'
,'BASS1_INT_CHECK_R044_MONTH.tcl'
,'BASS1_INT_CHECK_R045_MONTH.tcl'
,'BASS1_INT_CHECK_R046_MONTH.tcl'
,'BASS1_INT_CHECK_R047_MONTH.tcl'
,'BASS1_INT_CHECK_R048_MONTH.tcl'
,'BASS1_INT_CHECK_R049_MONTH.tcl'
,'BASS1_INT_CHECK_R050_MONTH.tcl'
,'BASS1_INT_CHECK_R051_MONTH.tcl'
,'BASS1_INT_CHECK_R052_MONTH.tcl'
,'BASS1_INT_CHECK_R053_MONTH.tcl');

commit;


update  app.sch_control_task set FUNCTION_DESC='[����]'||FUNCTION_DESC
where MO_GROUP_CODE='BASS1'
and control_code in
('BASS1_INT_CHECK_R041_MONTH.tcl'
,'BASS1_INT_CHECK_R042_MONTH.tcl'
,'BASS1_INT_CHECK_R043_MONTH.tcl'
,'BASS1_INT_CHECK_R044_MONTH.tcl'
,'BASS1_INT_CHECK_R045_MONTH.tcl'
,'BASS1_INT_CHECK_R046_MONTH.tcl'
,'BASS1_INT_CHECK_R047_MONTH.tcl'
,'BASS1_INT_CHECK_R048_MONTH.tcl'
,'BASS1_INT_CHECK_R049_MONTH.tcl'
,'BASS1_INT_CHECK_R050_MONTH.tcl'
,'BASS1_INT_CHECK_R051_MONTH.tcl'
,'BASS1_INT_CHECK_R052_MONTH.tcl'
,'BASS1_INT_CHECK_R053_MONTH.tcl');












