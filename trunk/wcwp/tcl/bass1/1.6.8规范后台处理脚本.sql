DROP TABLE BASS1.G_I_06021_MONTH;
DROP TABLE BASS1.G_I_06022_MONTH;
DROP TABLE BASS1.G_I_06023_MONTH;
DROP TABLE BASS1.G_S_22061_MONTH;
DROP TABLE BASS1.G_S_22062_MONTH;
DROP TABLE BASS1.G_S_22063_MONTH;


--06021接口：实体渠道基础信息
CREATE TABLE BASS1.G_I_06021_MONTH (
	TIME_ID            INTEGER         NOT NULL,   
  CHANNEL_ID         CHARACTER(25)   NOT NULL,   --实体渠道标识
  CHANNEL_TYPE       CHARACTER(1)    NOT NULL,   --实体渠道类型
  CMCC_ID            CHARACTER(5)    NOT NULL,   --所属CMCC运营公司标识
  COUNTRY_NAME       CHARACTER(30)   NOT NULL,   --区县名称
  THORPE_NAME        CHARACTER(50)   NOT NULL,   --乡镇/片区名称
  CHANNEL_NAME       CHARACTER(100)  NOT NULL,   --渠道名称
  CHANNEL_ADDR       CHARACTER(100)  NOT NULL,   --渠道地址
  POSITION           CHARACTER(1)    NOT NULL,   --地理位置类型
  REGION_INFO        CHARACTER(1)    NOT NULL,   --区域形态
  CHANNEL_B_TYPE     CHARACTER(1)    NOT NULL,   --渠道基础类型
  CHANNEL_STAR       CHARACTER(1),               --渠道星级
  CHANNEL_STATUS     CHARACTER(1)    NOT NULL,   --渠道状态
  BUSINESS_BEGIN     CHARACTER(4)    NOT NULL,   --营业起始时间
  BUSINESS_END       CHARACTER(4)    NOT NULL,   --营业结束时间
  VALID_DATE         CHARACTER(8),               --协议签署生效日期
  EXPIRE_DATE        CHARACTER(8),               --协议截止日期
  TIMES              CHARACTER(9),               --已合作年限
  LONGITUDE          CHARACTER(18),              --经度
  LATITUDE           CHARACTER(18),              --纬度
  FITMENT_PRICE      CHARACTER(10),              --装修累计投资总额
  EQUIP_PRICE        CHARACTER(10),              --设备累计投资总额
  PRICES             CHARACTER(10),              --办公和营业家具累计投资总额
  CHARGE             CHARACTER(10)               --一次性门头补贴
) DISTRIBUTE BY HASH(TIME_ID,CHANNEL_ID)
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;


--06022接口：实体渠道购建或租赁信息
CREATE TABLE BASS1.G_I_06022_MONTH (
	TIME_ID            INTEGER         NOT NULL,   
  CHANNEL_ID         CHARACTER(25)   NOT NULL,   --实体渠道标识
  OWNER_TYPE         CHARACTER(1)    NOT NULL,   --物业来源类型
  BUY_MONTH          CHARACTER(6),               --购买月份“当渠道物业来源类型为"1：上市公司购建"时，才需填写，否则填’000101’”
  FC_LIC             CHARACTER(50),              --房产证号“当渠道物业来源类型为“1：上市公司购建”时，才需填写，否则填空。”
  TD_LIC             CHARACTER(50),              --土地证号“当渠道物业来源类型为“1：上市公司购建”时，才需填写，否则填空。”
  GS_LIC             CHARACTER(50),              --工商号“当自有渠道物业来源类型为“1：上市公司购建”时，才需填写，否则填空。”
  BUY_CHARGE         CHARACTER(10),              --购买总价“当渠道物业来源类型为“1：上市公司购建”时，才需填写，否则填空。单位：元”
  RENT_BEGIN_DATE    CHARACTER(8),               --租赁开始日期“当渠道物业来源类型为2或3时，才需填写，否则填’000101’。”
  RENT_END_DATE      CHARACTER(8),               --租赁截止日期“当渠道物业来源类型为2或3时，才需填写，否则填’000101’。”
  AV_PRICES          CHARACTER(8)                --年平均租金“当自有渠道物业来源类型为2或3时，才需填写。否则填空。”
) DISTRIBUTE BY HASH(TIME_ID,CHANNEL_ID)
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;


--06023接口：实体渠道资源配置信息
CREATE TABLE BASS1.G_I_06023_MONTH (
	TIME_ID            INTEGER         NOT NULL,   
  CHANNEL_ID         CHARACTER(25)   NOT NULL,   --实体渠道标识
  BUILD_AREA         CHARACTER(5),               --建筑面积
  USE_AREA           CHARACTER(5),               --使用面积
  STORE_AREA         CHARACTER(5),               --前台营业面积
  SEAT_NUM           CHARACTER(3),               --台席数量
  STORE_EMPLOYE      CHARACTER(3),               --营业人员数量
  GUARD_EMPLOYE      CHARACTER(3),               --保安人数
  CLEAR_EMPLOYE      CHARACTER(3),               --保洁人数
  IF_WAIT_MARK       CHARACTER(1),               --有无排队叫号机
  IF_POS_MARK        CHARACTER(1),               --有无POS机
  IF_VIP_SEAT        CHARACTER(1),               --有无VIP专席
  IF_VIP_ROOM        CHARACTER(1),               --有无VIP室
  PRINT_NUM          CHARACTER(3),               --帐详单打印机台数
  TERM_NUM           CHARACTER(3),               --综合性自助终端台数
  G3_AREA            CHARACTER(5),               --G3体验区面积
  TV_NUM             CHARACTER(3),               --电视屏个数
  NEW_BUSITERM_NUM   CHARACTER(3),               --新业务体验营销平台终端个数
  HEART_TERM_NUM     CHARACTER(3),               --心机体验平台终端个数
  NET_TERM_NUM       CHARACTER(3),               --网上营业厅接入终端个数
  AREA               CHARACTER(5),               --店面面积
  ACCEPT_AREA        CHARACTER(5),               --移动受理区面积
  MAIN_NET_TYPE      CHARACTER(1),               --主要联网方式
  SUB_NET_TYPE       CHARACTER(1),               --次要联网方式
  IF_CZ              CHARACTER(1)                --能否办理空中充值业务
) DISTRIBUTE BY HASH(TIME_ID,CHANNEL_ID)
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;



--22061接口：实体渠道运营成本信息
CREATE TABLE BASS1.G_S_22061_MONTH (
	TIME_ID            INTEGER         NOT NULL,   
	STATMONTH          CHARACTER(6)    NOT NULL,   --月份
  CHANNEL_ID         CHARACTER(25)   NOT NULL,   --实体渠道标识
  WATER_FEE          CHARACTER(6),               --当月水费
  ELE_FEE            CHARACTER(8),               --当月电费
  HEAT_FEE           CHARACTER(8),               --当月取暖费
  WORK_FEE           CHARACTER(8),               --当月人工成本及劳务费
  OFFICE_FEE         CHARACTER(8),               --当月办公用品及耗材费用
  OTHER_FEE          CHARACTER(8)                --当月其他日常费用
) DISTRIBUTE BY HASH(TIME_ID,STATMONTH,CHANNEL_ID)
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;



--22062接口：实体渠道业务办理信息
CREATE TABLE BASS1.G_S_22062_MONTH (
	TIME_ID            INTEGER         NOT NULL,   
	STATMONTH          CHARACTER(6)    NOT NULL,   --月份
  CHANNEL_ID         CHARACTER(25)   NOT NULL,   --实体渠道标识
  ACCEPT_TYPE        CHARACTER(1),               --办理类型
  NEW_USERS          CHARACTER(8),               --新增客户数
  HAND_CNT           CHARACTER(10),              --缴费笔数
  HAND_FEE           CHARACTER(10),              --缴费金额“单位：元”
  CARD_SALE_CNT      CHARACTER(8),               --充值卡销售张数
  CARD_SALE_FEE      CHARACTER(10),              --充值卡销售金额“单位：元”
  ACCEPT_CNT         CHARACTER(10),              --增值业务办理笔数
  IMP_ACCEPT_CNT     CHARACTER(10),              --重点增值业务办理笔数
  TERM_SALE_CNT      CHARACTER(10),              --定制终端销售笔数
  OTHER_SALE_CNT     CHARACTER(10),              --其中定制手机销售笔数
  ACCEPT_BAS_CNT     CHARACTER(10),              --办理类基础服务笔数
  QUERY_BAS_CNT      CHARACTER(10),              --查询类基础服务笔数
  OFF_ACCEPT_CNT     CHARACTER(8)                --套餐办理笔数
) DISTRIBUTE BY HASH(TIME_ID,STATMONTH,CHANNEL_ID)
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;



--22063接口：实体渠道酬金及补贴信息
CREATE TABLE BASS1.G_S_22063_MONTH (
	TIME_ID            INTEGER         NOT NULL,   
	STATMONTH          CHARACTER(6)    NOT NULL,   --月份
  CHANNEL_ID         CHARACTER(25)   NOT NULL,   --实体渠道标识
  FH_REWARD          CHARACTER(10),              --放号酬金
  BASIC_REWARD       CHARACTER(10),              --基础业务服务代理酬金
  INCR_REWARD        CHARACTER(10),              --增值业务代理酬金
  INSPIRE_REWARD     CHARACTER(10),              --激励酬金
  TERM_REWARD        CHARACTER(10),              --终端酬金
  RENT_CHARGE        CHARACTER(8)                --房租补贴
) DISTRIBUTE BY HASH(TIME_ID,STATMONTH,CHANNEL_ID)
   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ;




----------------------------------------------------
--取消作废的接口调度
--06030(实体渠道)
--22044(自营渠道情况)
--22045(社会渠道情况)
--22046(自营渠道业务量情况)
--22047(社会渠道业务量情况)
--22048(手机卖场情况)
--22051(社会渠道酬金情况)
--22053(实体渠道日汇总)
--22054(实体渠道月汇总)

---处理app.sch_control_task

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

update  app.sch_control_task set FUNCTION_DESC='[作废]'||FUNCTION_DESC
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

--取消作废接口涉及的校验(待确认)
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


update  app.sch_control_task set FUNCTION_DESC='[作废]'||FUNCTION_DESC
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












