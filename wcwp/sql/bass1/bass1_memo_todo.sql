1.核查20110523日离网一致性差16个的原因。
--2.规范开发。
--2.1 02022修改。
--2.2 02023修改。
--2.3 新接口口径确认。
--3.出数完成提示短信。

--4.在03018 1300 内关联用户表。去掉历史用户。
--修复订购关系？

5.调度问题：代码没在后台也正常执行。
5.调度问题：语法错误不捕捉。

--6.INT_CHECK_R193194_DAY 接口中，已上传的数据会校验吗？

--7. 需要修复一条数据：
--select * from   G_A_02060_DAY where     ENTERPRISE_ID = '89103000161144'
--1230	BLACK BERRY ！！！！！！！！！！！！！
-- 1230：集团订购关系没有的，在个人订购关系中有。
-- 89103000161144	89103000161144	测试集团	W*_6oJ	3	2	9	12	1	0	0	1	12800	0	0	0	891	891	1001	10010001	阿里移动分公司	859000	891	897	阿里移动分公司	859000	13989974044	[NULL]	12	测试集团	阿里移动	[NULL]	5	[NULL]	2008-06-19 	2010-12-14 	2010-09-20 	2099-12-31 	100005815649	11111111	1163431	10100120	[NULL]	4			15	91000033	0		1	1	9	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]


--8.需要修复数据：：02056 企业手机邮箱。！！！！！！！！！！

 
--9 传01005 接口！！！！！！！！！！！！！！！！

--10 屏蔽一次性02060,02054修复。

--11 请青峰检查校验。

--12 R232 要调数据！！！！！！




--检查：01007 ， 02058 ， 02059， 02054 ， 

--bass2.STAT_ZD_VILLAGE_USERS_201105
!!

R216 的数据问题！调度问题！日、月？


/**
理清22081 
10086977 退订
10086901 提醒
10086501 你正在开通一个什么梦网业务，回复就开通什么的。

你发1到10086102（移动账单查询）
中国移动统一将“１００８６７００”短信端口规划为窗口服务满意度短信测评的专用号码，客户回复此号码免费，河南移动公司于４月１日起将原来营业厅窗口服务满意度测评端口号“１００８６１”调整为“１００８６７００”，

10086	2816334
10086700	61188
10086977	33350
10086501	28494
10086901	3589
10086102	539
10086999	440
100861	267
10086106	189
10086100	178
100866	143
10086000	47
100860	44
1008612	40
10086503	38
10086104	35
100869	35
10086507	33
**/

R216 配置！
、、、、、、、、、、、
--与李2 确认校园市场
--22066 开发 ~ 1~n 日数据
过一遍空接口
集团客户数据01005

元数据

过一遍校验

--校园市场接口

--6.6还信用卡
--周四 

--加强电子渠道，，，

/**
关于下发两级经分元数据互通技术方案及联调测试安排的通知 
发布人： 谢帆 发布时间： 2011-6-10 10:22:00 
附 件： 两级元数据互通技术方案及参考文档.rar(2043.23KB) 
内 容： 
各省业务支撑部门：



   作为NG2-BASS3.0工程建设必选内容，两级经分元数据互通是保障经分系统数据质量的重要技术手段之一。自2010年9月起，总部在江苏、湖北、陕西、广西和内蒙五省启动先期建设与两级系统联调，共同对互通方案进行了完善并积累了联调测试经验，目前已完成工作总结（见近期OA发文）。在此基础上，并根据NG2-BASS3.0建设要求，总部计划于2011年7月31日之前组织其余26省完成两级经分元数据互通的联调测试。



   具体要求及说明如下：



   1、总体要求：7月31日前，各省按照技术方案的机制与格式要求完成规定元数据的上传并通过一经系统相关校验（文件级、元模型级、内容级）。7月5日起各省可试上报。



   2、技术方案及参考文档：见附件《中国移动经营分析系统两级元数据互通技术方案v0.98》、《两级经分元数据互通-问题自助手册》等文档。



   3、元数据范围：上传省端生成24个重点一经接口的数据链路所涉及的接口单元、维度、数据库表/视图以及数据处理过程等相关类型元数据。对于数据处理过程部分元数据，建议已具备SQL自动解析能力的省公司直接上报至FeatureMap级元数据并建立字段级映射关系，尚未具备相关能力的省公司可上报ClassifierMap级元数据并建立表级映射关系，以上两种情况各省均需确保相关元数据的完备性。



   4、各省应逐步建立互通元数据的完备性、准确性、及时性的保障机制，确保数据质量。



   5、各省可通过访问一经系统元数据管理子系统中的“常规应用/两级互通数据查询”模块，对本省已成功上传的元数据在完整性、准确性方面进一步自查。



    联系人：谢帆 13601352189



   具体联调测试联系人：



               技术方案    刘晨    15901188399

               文件级检查  宓亚州  13931981414

               模型及内容级检查  马占有  15010139558
 
**/

1.7.3 文档

--提单：
--a. 1.7.3
--b. 校验
--c. 核查
--d. 接口修复
--e. 校园市场
--f. 
--
--关于下发《中国移动一级经营分析系统省级数据接口规范V1.7.3》的通知
--透明消费月接口4月份数据调查
--请青海、西藏核查近日通信客户数、通话客户数问题
--请云南、西藏核查近日联通新增客户数、电信离网客户数问题
--请山东、西藏核查近日彩信计费量问题
--请西藏核查6月15日通信客户数、通话客户数问题
--请各公司尽快核查一级经分异常数据
--

--检查06001增量上传情况
--20110622 & 20110623 两天 
--核查20110621（应有数） & 20110622（应没数） 两天数据

--同向匹配的问题，等校验下来，待修复。


--检查近期接口前置
 
--22082 22083 核查校验 加入。

--集团怎么算月离网？


--核查，重传02020



select CONTROL_CODE,BEFORE_CONTROL_CODE,count(0) from    app.sch_control_before
where CONTROL_CODE like 'BASS1%'
group by  CONTROL_CODE,BEFORE_CONTROL_CODE
having count(0) > 1


处理：
-rw-r--r--    1 500      503       1055538 Jun 27 14:10 13100_01005_201105.dat
-rw-r--r--    1 500      503       4373325 Jun 27 14:10 13100_01006_20110626.dat
drwxr-xr-x   16 500      503          4096 Jun 27 14:10 .
-rw-r--r--    1 500      503        306657 Jun 27 14:10 13100_01007_20110626.dat


--同向匹配的问题，等校验下来，待修复。

/*
申请下发清单：
01004 , 02059	R182
R221
R222
R223
R224
R225
R226
R227
R228
涉及的TD中间表。

02047 for R229

21020 FOR R230
*/


提单：R216 相关接口修复

--02054
--02064

6月传校园市场月数据,检查。


--bass1_lst!!!!!!!!!!!!!

--修复 1520 收入！！

--确认r228

月调度调整


g_s_03017_month
g_s_03018_month
数据核查！

检查22204 修改后费用！！！



--todo:int 月表！！！补建！！ 方案-回复！！！

修复02021 ~ 02023 ！！



--22091 22092 校验！ 已修改，待完善。

--BASS1_G_I_02021_MONTH.tcl	int -s G_I_02021_MONTH.tcl	2	exception:1:27.000	2011-07-03 11:04:16.325149	[NULL]	-1	[NULL]
--已经修复02023，更新代码！




核查02059差异！
修改R237 取两值！


$ 
cms 201112 201111 |egrep '01005|02005|02014|02015|02016|02018|02019|02020|02021|02047|06021|06022|06023|06034|22009|22101|22103|22105|22106'




特别留意：
02007
02020
02047


02005	0
02019	0
02021	0
02047	0
06021	0
06022	0
06023	0




二、定期清理临时中间表
    BASS1_INT_02004_02008_YYYYMM.tcl
    BASS1_INT_22038_YYYYMM.tcl
    BASS1.INT_21007_YYYYMM.tcl
    BASS1_INT_0400810_YYYYMM.tcl
    BASS1_INT_210012916_YYYYMM.tcl
    

----02016 有重复号码！
--
--USER_ID	CITY_ID	SERVICE_ID	SPROM_ID	SPROM_TYPE	BUSI_TYPE	SPROM_PRIOR	PLAN_ID	SPROM_PARA	PAY_MODE	PROPERTY	CREATE_DATE	VALID_DATE	EXPIRE_DATE	SO_NBR	REMARK	PROD_ID	IS_PROM	PROD_NAME	SERVICE_ID	VALID_DATE	EXPIRE_DATE
--89157334068705	891	50015	90030024	[NULL]	[NULL]	[NULL]	89110011	[NULL]	[NULL]	[NULL]	2011-06-01 0:00:00.000000	2011-06-01 0:00:00.000000	2099-12-31 23:59:59.000000	300055405364	[NULL]	90030024	1	WLAN无线宽带上网套餐100元包月	50015	2008-03-01 0:00:00.000000	2030-01-01 0:00:00.000000
--89157334068705	891	50015	90030031	[NULL]	[NULL]	[NULL]	89110011	[NULL]	[NULL]	[NULL]	2011-06-01 0:00:00.000000	2011-06-01 0:00:00.000000	2099-12-31 23:59:59.000000	300053165060	[NULL]	90030031	1	WLAN无线宽带上网套餐50元包月	50015	2008-03-01 0:00:00.000000	2030-01-01 0:00:00.000000
--
----添加主键校验！！ for 02016 
--
写个清理表空间的脚本！


--egrep '02006|02007|02017|02052|03004|03005|03012|03015|03016|03017|03018|21003|21006|21008|21011|21012|21020|22036|22040|22072|22081|22083|22085|22086|22204|22303|22304|22305|22306|22307|22401'



    
    
0100
0200
!0205
0300
!0317
!0325
!0405
!0407
!0409
0500
!0516
!0524
0600
!0602
!0609
!0619
!0628
!0640
!0642
!0643
0700
!0701
!0702
!0800
0900

有很多一经账目没有和2经编码映射！！核查！！


核查维度指标定义表的修订！！

--i_13100_201106_21020_00_001

--插入校验报告内容成功!校验报告内容[row=2]:i_13100_201106_21020_00_001.dat         218632    21020011



铁通号码修复，关联的校验校验！！ (缓)

01006, 01007 ,01005 改造！




检查未生成校验！！
such as :R207 

2011-07-14 17:50:25


R001
R002
R011
R088
R095
R096
R127
R130
R133
R134
R135
R136
R137
R159
R160
R161
R162
R260
R261
筛选后：还有
R127	月	01_个人客户	离网用户数月变动率
R130	月	06_账务收入	增值业务收入

--R095	月	03_话单量收	计费时长月/日汇总关系（智能网平台） --作废
--R096	月	03_话单量收	总费用月/日汇总关系（智能网平台） --作废
--R088	日	03_话单量收	点对点短信计费量变动趋势 --C1
--R011	日	01_个人客户	一个电话号码不能同时对应多个非离网的用户ID --有修正脚本 ref:02008
--
select sum(bigint(M_OFF_USERS)) from G_S_22012_DAY
where time_id / 100 = 201105
63211

60747

西藏	R127	|(本月离网用户数 / 上月离网用户数 - 1) x 100%| ≤ 20%	60743.00	61041.00


select count(0),count(distinct user_id) from     BASS1.int_02004_02008_month_stage a
where a.usertype_id  IN ('2010','2020','2030','9000')
and test_flag = '0'
and time_id/100 = 201106

63198

63199
60743.00	61041.00

西藏	R127	|(本月离网用户数 / 上月离网用户数 - 1) x 100%| ≤ 20%	60743.00	61041.00



/**
1. 元数据
2. 集团的数 要修复。

3. 元数据材料准备
您好，我们这边提供集团要求的24个重点接口以及接口字段，由你们这边来梳理这些接口字段与对应的表字段之间的关系。

还有，我们还需要一经和二经的存储日志，由于这里是多线程处理，导致日志中间处理的不一致，因为您是主要是负责一经，所以可能要求你配合梳理一经24个接口的对应的处理日志。
**/


--TD 提供量收的中间表。

--对比结果：本地比集团只多了8个用户

--imei load
--400 核查 新统付表




--02004 增量 计算  ： 不 把 sim_code 的变化 纳入 增量 计算口径。
--只用  a.user_id,a.brand_id,usertype_id 来 辨别增量 

集团客户接口 -- 移动400 测试 剔除


--/bassapp/backapp/bin/bass1_export/bass1_export bass1.G_S_02024_DAY 2011-07-31 &


--关于核查一经电子渠道业务办理数据质量的通知

--1. 由于数据质量考核规则中将增加与往期数据比对稽核要求，如不按期对问题数据进行更新，将会影响一经数据质量考核成绩。
如何校验？ 1-7 ？ 2-8？
 江勇  010-66006688-2323



--2. 22065 22066 22067 
重传 怎么重传？
只要涉及的接口都可以申请重传？

1月差距比较大，想重传怎么重传？

--3. 22066 字段修改过。


select * from dw_PRODUCT_ORD_SRVPKG_dm_201009 a,dw_PRODUCT_ORD_CUST_dm_201009 b
where a.op_time='2010-09-21' and a.CUSTOMER_ORDER_ID=b.CUSTOMER_ORDER_ID
   and a.servicepkg_id in (select PRODUCT_ITEM_ID from DIM_PROD_UP_PRODUCT_ITEM where name like '%两城一家%' and item_type='SERVICE_PRICE')
   and b.business_id in (191000000007,191000000008);
   



--1.  修正 02008 取数代码
--and not ( userstatus_id = 4 and 

--2.  修复 已经上报的02008 , 在02004 中加入！！



--22066 17，18，19日数据   7.1 - 8.19 


--1. 22067 201107 是否需要重传？

--1. wap 登录 业务办理 

### 22091 inc 部分可以不重跑 ！！
day3
cms 201201 201112 |egrep '02049|02053|03001|03002|03003|06011|06012|06029'


day5
cms 201201 201112 |egrep '01005|02005|02014|02015|02016|02018|02019|02020|02021|02047\
|06021|06022|06023|22009|22101|22103|22105|22106'

cms 201201 201112 |egrep '01005|02005|02014|02015|02016|02018|02019|02020|02021|02047|\
06021|06022|06023|06034|22009|22101|22103|22105|22106'

day8
cms 201201 201112 |egrep '02006|02007|02031|02032|02033|02052|03004|03005|03012|03015|03016|03017|03018\
|06001|06002|06003|21003|21006|21008|21011|21012|21020|22040|22072|22081|22083|22085\
|22086|22204|22303|22304|22305|22306|22307|22403|22404'

day10
cms 201201 201112 |egrep '02034|02035|03007|03019|05001|05002|05003|21010|21013|21014|21015|22013|22021|\
22025|22032|22033|22039|22041|22042|22043|22049|22050|22052|22055|22056|22057\
|22059|22061|22067|22068|22069|22074|22075|22076|22077|22405|22406|22422'

cms 201109 201108 |egrep '02049|02053|03001|03002|03003|06011|06012|06029'


cms 201201 201112 |egrep '02026|02027|22058|22060|22062|22063'


02011 op_time , diaochahuifu
LIST
 *06029*dat \
 *06012*dat \
 *06011*dat \
 *03003*dat \
 *03002*dat \
 *03001*dat \
 *02053*dat \
 *02049*dat \


ls -alrt \
 *02049*dat \
 *02053*dat \
 *03001*dat \
 *03002*dat \
 *03003*dat \
 *06011*dat \
 *06012*dat \
 *06029*dat 
 




R255
R130
R146
