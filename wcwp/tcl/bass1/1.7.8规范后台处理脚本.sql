/**
514	修改接口02005（大客户/大用户）
接口单元说明：修改参照文档。	1.7.8	2011-12-15	自数据日期201201起生效
**/

--等待BOSS文档以确定修订情况！

/**
515	修改接口02006（用户积分情况）
1、	接口单元说明：修改参照文档，删除对T年等积分年度的定义，进一步明确上报数据范围。
2、	接口单元属性列表：
1）、增加以下字段：
当月品牌奖励积分
当月网龄奖励积分
专项转移积分
其他清零积分
2）、删除以下字段：
当月奖励积分
T年可兑换积分
T-1年可兑换积分
T-2年可兑换积分
历史可兑换积分
到期可兑换积分
转网清零积分
3）、修改字段名：
累计已兑现积分值 改为 累计已兑换积分值
3、	抽取方式及周期：
删除对上报数据范围的说明性文字，上报数据要求统一参见接口单元说明。	1.7.8	2011-12-15	1、2012年1月8日前仍按照原02006接口定义上传数据日期2011年12月的数据。原则上，2012年1月20日之后不再接收针对原02006接口定义的数据重传和更新操作。
2、2012年1月31日起按照新02006接口定义上传数据：
（1）2月5日前上传按照2012版积分管理办法对存量积分进行转换后的2011年末（2011年12月31日24时）用户积分全量快照。接口数据文件名保持不变，重传序号自 10开始（i_XXXXX_201112_02006_10_XXX）。
（2）在上述2011年末积分全量快照上传成功后，2月15日前按照新02006接口定义正常上传数据日期为2012年1月的数据。

**/

 G_I_02006_MONTH.tcl
 
 
 /**
 516	修改接口02007（用户积分回馈情况）
1、	接口单元说明：删除对T年等积分年度定义的注释。
2、	接口单元属性列表：
1）、删除以下字段：
T年已兑换积分值
T-1年已兑换积分值
T-2年已兑换积分值
历史已兑换积分
2）、修改字段名：
使用积分值 改为 兑换扣减积分
回馈次数 改为 兑换次数	1.7.8	2011-12-15	自数据日期201201起生效

 **/
 
 
 G_S_02007_MONTH.tcl
 
 
 
 /**
 517	04005（梦网短信话单）修改接口单元说明	1.7.8	2011-12-15	自数据日期20120101起生效
 **/
 
 
  G_S_04005_DAY.tcl
  
/**
  518	22012 （日KPI）:
修改字段“短信计费量”的字段名和属性描述
增加字段“行业网关短信计费量”	1.7.8	2011-12-21	自数据日期20120101起生效
 **/
 
 
 G_S_22012_DAY.tcl
 
 rename  bass1.G_S_22012_DAY to G_S_22012_DAY_20111231

 
 CREATE TABLE "BASS1   "."G_S_22012_DAY"  (
                  "TIME_ID" INTEGER , 
                  "BILL_DATE" CHAR(8) NOT NULL , 
                  "M_NEW_USERS" CHAR(10) NOT NULL , 
                  "M_DAO_USERS" CHAR(12) NOT NULL , 
                  "M_BILL_DURATION" CHAR(12) NOT NULL , 
                  "M_DATA_FLOWS" CHAR(12) NOT NULL , 
                  "M_BILL_SMS" CHAR(12) NOT NULL , 
                  "M_BILL_HANGYE_SMS" CHAR(12) NOT NULL , 
                  "M_OFF_USERS" CHAR(10) NOT NULL , 
                  "M_BILL_MMS" CHAR(12) NOT NULL )   
                 DISTRIBUTE BY HASH("TIME_ID")   
                   IN "TBS_APP_BASS1" ; 
		   





insert into G_S_22012_DAY
select 
         TIME_ID
        ,BILL_DATE
        ,M_NEW_USERS
        ,M_DAO_USERS
        ,M_BILL_DURATION
        ,M_DATA_FLOWS
        ,M_BILL_SMS
	,''
        ,M_OFF_USERS
        ,M_BILL_MMS
from 	G_S_22012_DAY_20111231




/**
519	修改了以下接口中全球通全网统一资费套餐的“基础套餐标识”的编码，参见附件一维度指标说明中的BASS_STD1_0114【全球通全网统一资费基础套餐标识】：
02018（基础资费套餐）
02020（用户选择基础资费套餐）
02022（用户选择全球通全网统一基础资费套餐）
02024（全球通基础资费套餐用户成功办理量）	1.7.8	2011-12-22	月接口自数据日期201112起生效；
日接口自数据日期20120101起生效。


**/

G_I_02018_MONTH.tcl
G_I_02020_MONTH.tcl
G_I_02022_DAY.tcl
G_S_02024_DAY.tcl


CREATE TABLE "BASS1   "."DIM_QW_QQT_PKGID"  (
                  NEW_PKG_ID VARCHAR(30) NOT NULL, 
                  OLD_PKG_ID VARCHAR(30) NOT NULL , 
                  PKG_NAME VARCHAR(200) )   
                 DISTRIBUTE BY HASH("NEW_PKG_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 


insert into bass1.DIM_QW_QQT_PKGID
values ('999914211020058001','QW_QQT_JC_SW58','全球通全网统一资费上网套餐58元')
,('999914211020088001','QW_QQT_JC_SW88','全球通全网统一资费上网套餐88元')
,('999914211020128001','QW_QQT_JC_SW128','全球通全网统一资费上网套餐128元')
,('999914211030058001','QW_QQT_JC_SL58','全球通全网统一资费商旅套餐58元')
,('999914211030088001','QW_QQT_JC_SL88','全球通全网统一资费商旅套餐88元')
,('999914211030128001','QW_QQT_JC_SL128','全球通全网统一资费商旅套餐128元')
,('999914211030158001','QW_QQT_JC_SL158','全球通全网统一资费商旅套餐158元')
,('999914211030188001','QW_QQT_JC_SL188','全球通全网统一资费商旅套餐188元')
,('999914211030288001','QW_QQT_JC_SL288','全球通全网统一资费商旅套餐288元')
,('999914211030388001','QW_QQT_JC_SL388','全球通全网统一资费商旅套餐388元')
,('999914211030588001','QW_QQT_JC_SL588','全球通全网统一资费商旅套餐588元')
,('999914211030888001','QW_QQT_JC_SL888','全球通全网统一资费商旅套餐888元')
,('999914211040058001','QW_QQT_JC_BD58','全球通全网统一资费本地套餐58元')
,('999914211040088001','QW_QQT_JC_BD88','全球通全网统一资费本地套餐88元')
,('999914211040128001','QW_QQT_JC_BD128','全球通全网统一资费本地套餐128元')
,('999912220440010001','QW_QQT_DJ_DX0001','全球通全网统一资费套餐专属数据包-短信包')
,('999912220450010001','QW_QQT_DJ_CX0001','全球通全网统一资费套餐专属数据包-彩信包')
,('999912221010006001','QW_QQT_DJ_ZX0001','全球通全网统一资费套餐专属数据包-全球通尊享包')
,('999912220630003001','QW_QQT_DJ_YD0001','全球通全网统一资费套餐专属数据包-全球通阅读包')
,('999912221010005001','QW_QQT_DJ_YY0001','全球通全网统一资费套餐专属数据包-全球通音乐包')
,('999912220470003001','QW_QQT_DJ_FHZX0001','全球通全网统一资费套餐专属数据包-全球通凤凰资讯包')
;



/**
520	修改了以下接口中全球通全网统一资费的“叠加套餐标识”的编码，参见附件一维度指标说明中BASS_STD1_0115【全球通全网统一资费专属叠加资费套餐标识】：
02019（叠加资费套餐）
02021（用户选择叠加资费套餐）
02023（用户选择全球通专属叠加资费套餐）
02025（全球通专属叠加资费套餐用户成功办理量）	1.7.8	2011-12-22	月接口自数据日期201112起生效；
日接口自数据日期20120101起生效。

**/

G_I_02019_MONTH.tcl
G_I_02021_MONTH.tcl
G_I_02023_DAY.tcl
G_S_02025_DAY.tcl


