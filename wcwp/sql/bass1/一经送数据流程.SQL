----===========================================================================================
--日接口数据(暂时56个)，具体见《一级经营分析系统省级数据接口及时性时限要求1.7.0.xls》文档
----===========================================================================================
--示例：20110118日传送17日的数据

--1、接口文件生成所在机器以及目录(目录随时间进行变动，月的就是export_201101)：
ip：172.16.5.44 
name/pwd：app/app
/bassapp/backapp/data/bass1/export/export_20110117

--如：
telnet 172.16.5.44
login: app
Password: app
Last login: Tue Jan 18 09:56:13 from 10.233.20.171
Sun Microsystems Inc.   SunOS 5.10      Generic January 2005
You have new mail.
$ ksh
$ cd /bassapp/backapp/data/bass1/export
$ ls -lrt
$ ...
$ cd export_20110117

--2、此目录下如全部生成，可用ls、wc等命令进行统计数据*.dat文件为56个(前提是调度未重跑)，早上7点也会给定制的人发短信，
--其中*.verf文件为检验文件

--3、ftp传送到集团一经接口机以及目录(注意：不管是一次性上传还是单个文件上传，都必须先mput *.dat，后mput *.verf，否则返回报错日志)
ip：172.16.5.130
name/pwd：bass/3jysjbx
/data1/asiainfo/interface/data
--put上去后，可看到，集团那边在自动提取数据，文件也就一会转移到了其它目录

--如：
$ pwd
/bassapp/backapp/data/bass1/export/export_20110117
$ ftp 172.16.5.130
Connected to 172.16.5.130.
220 (vsFTPd 2.0.1)
Name (172.16.5.130:app): bass
331 Please specify the password.
Password:3jysjbx
230 Login successful.
Remote system type is UNIX.
Using binary mode to transfer files.
ftp> cd data
250 Directory successfully changed.
ftp> bin
200 Switching to Binary mode.
ftp> prompt off
Interactive mode off.
ftp> mput *.dat
...
ftp> mput *.verf
...
ftp> pwd       
257 /data1/asiainfo/interface/data

--4、put完后日接口数据文件后，就可以检查文件级返回日志是否OK
--接口文件级返回检查(正常情况为56条记录)
select  * from APP.G_FILE_REPORT
where filename like '%20110117%' and err_code='00';

也可以检查44机器上的日志返回个数：
$ pwd  
/bassapp/backapp/data/bass1/report/report_20110117
$ ls -l f*|wc -l
      56
$...

---月接口文件级返回检查(对于接口重送，相应的进行增加)
select  * 
--distinct left(right(filename,16),5),err_code 
from APP.G_FILE_REPORT
where filename like '%_201101_%' 
and err_code='00'
and length(filename)=length('s_13100_201002_03014_01_001.dat')
order by deal_time desc;


--5、集团入库后，将会返回各个接口的记录级日志(需要几个小时才全部返回)
--接口记录集返回检查(正常情况为56条记录)
select * from app.g_runlog 
where time_id=20110117
and return_flag=1


--以上5步为日月接口生成无任何问题下送数据的基本流程。
--===========================================================================================
--下面侧重注意2点日数据经常遇到的问题(其它报错可问我，特别涉及早上9点之前的接口报错，请第一时间给我电话)：
--===========================================================================================
1、22073竞争KPI涉及的校验R163、R164、R165、R166、R167、R168、R169、R170、R171、R172(调度为BASS1_INT_CHECK_COMP_KPI_DAY.tcl)和
21007短信涉及的校验C1(调度BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl)超标，数据正常生成的，不要进行任何数据调整，调度直接点击完成后运行后续；

如：
select * from bass1.g_rule_check where rule_code in ('R171') order by time_id desc

2、22012日kpi接口涉及的一致性检查，如超标，必须进行调整(调度为BASS1_INT_CHECK_INDEX_SAME_DAY.tcl)，调整22012接口的指标，重跑报错调度，
一定不能点击完成(最频繁的是“离网用户数”这个指标，差一个用户就超标，暂没解决口径不一致情况，二经未改造用户资料表)；

如：
select 
 time_id,
 case when rule_code='R159_1' then '新增客户数'
      when rule_code='R159_2' then '客户到达数'
      when rule_code='R159_3' then '上网本客户数'
      when rule_code='R159_4' then '离网客户数'
 end,
 target1,
 target2,
 target3
from bass1.g_rule_check
where rule_code in ('R159_1','R159_2','R159_3','R159_4')
  and time_id=20110117;


--调整脚本，''里更新一定的值就是
--离网客户数
update bass1.g_s_22012_day set m_off_users='' 
where time_id=20110117;
commit;

--新增客户数
update bass1.g_s_22012_day set m_new_users='' 
where time_id=20110117;
commit;

--上网本客户数
update bass1.g_s_22201_day set mtl_td_3gbook_mark='' 
where time_id=20110117;
commit;

3、每日11点的波动性检查(调度BASS1_INT_CHECK_INDEX_WAVE_DAY.tcl)，这个主要是下午4点之前用于回复集团的每天指标的波动情况(具体见表哥邮件)，
如指标超标报错，不用处理，中午会自动更新调度状态；


--===========================================================================================
--月底31日以及月初1日应该做的事项：
--===========================================================================================
1、91002、91003老的imei接口需入库（勇哥清楚），
程序为/bassdb1/etl/L/imei /load_imei.sh 修改下日期，运行即可，另外新的91005接口同样也将进行入库，此接口为临时接口，暂时请手工入库,
91005的文件和91002、91003同一目录下：/data1/asiainfo/interface/imei/

---------------处理91005接口数据问题------------------------------
RENAME TABLE BASS2.DIM_TERM_TAC TO DIM_TERM_TAC_20110131BAK;

CREATE TABLE BASS2.DIM_TERM_TAC
 (ID             INTEGER,
  TAC_NUM        VARCHAR(15),
  TERM_ID        VARCHAR(10),
  TERM_MODEL     VARCHAR(50),
  TERMPROD_ID    VARCHAR(10),
  TERMPROD_NAME  VARCHAR(200),
  NET_TYPE       CHARACTER(1),
  TERM_TYPE      CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_DIM
  PARTITIONING KEY
   (ID
   ) USING HASHING;

ALTER TABLE BASS2.DIM_TERM_TAC
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

db2 connect to bassdb user bass1 using bass1
db2 "load client from c:\i_30000_201101_91005_001.dat of del modified by coldel, insert into bass2.dim_term_tac"

--
insert into BASS2.DIM_TERM_TAC
select 
ID,
TAC_NUM,
TERM_ID,
TERM_MODEL,
TERMPROD_ID,
TERMPROD_NAME,
NET_TYPE,
TERM_TYPE from BASS2.DIM_TERM_TAC_20110131BAK
where net_type <>'2';
coMMit;

--检查唯一
select tac_nuM,count(*) from BASS2.DIM_TERM_TAC
group by tac_nuM
having count(*)>1


2、月底调度BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl中R107/R108超标，此调度只有在月初1号早上送月底那天的日数据时必须调整，让校验通过，
其它日期调度报错直接点击运行完成；

--调整脚本(数据量大，update有些慢，要几分钟，调了以后如还报错，再调其中“400”和“5”的值，注意微调)
---R107
update (select * from  BASS1.G_S_04008_DAY where time_id = 20110131  ) t 
set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+400) with ur;

commit;

---R108
update (select * from  BASS1.G_S_04008_DAY where time_id = 20110131  ) t 
set BASE_BILL_DURATION = char(bigint(BASE_BILL_DURATION)-5);
commit;

如R109也报错，请通知我来处理。


3、1号日接口数据送完之后，中午左右集团将下发抽样用户数据，进行数据入库(就如下月份)，不然第二天的日接口数据将出问题
以app用户登录 172.16.5.44 
$ cd /bassapp/backapp/bin/bass1_lst
$ ./bass1_lst.sh 2011-02


--===========================================================================================
--月接口数据注意的事项(总共81个)：具体见《一级经营分析系统省级数据接口及时性时限要求1.7.0.xls》文档
--===========================================================================================
1、有几个接口数据需盯紧前置是否OK：
7号的接口02052依赖报表的手工入库数据bass2.stat_zd_village_users_yyyymm(农村客户统计表用户基础表),春节期间找谁，请问报表相关人，
4-5号应该就盯是否完成；
9号的接口05001、05002依赖局方提供的《西藏移动互联互通结算月报表》，春节找张阳，sheet页中一个语音，一个短信，根据模板，
调整数据位置，进行入库操作，入库后，启动其导出调度BASS1_EXP_G_S_05001_MONTH、BASS1_EXP_G_S_05002_MONTH进行文件生成；
9号的接口22039依赖重入网的数据bass2.dmrn_user_ms ，需检查；

2、21003接口的数据切割处理，现此接口数据必须切成3个文件

示例如：

  --1>分割21003 : split -7000000 s_13100_201101_21003_00_001.dat
  --2>重命名21003数据文件 : s_13100_201101_21003_00_001.dat 
                            s_13100_201101_21003_00_002.dat
                            s_13100_201101_21003_00_003.dat
  
  --3>修改21003VERF文件 ：s_13100_201101_21003_00.verf
  
  ^M  ctrl+V+M 
 
xaa  xab  xac 
mv xaa s_13100_201101_21003_00_001.dat
mv xab s_13100_201101_21003_00_002.dat
mv xac s_13100_201101_21003_00_003.dat

$ more s_13100_201101_21003_00.verf
s_13100_201101_21003_01_001.dat         1750000000          7000000             201101  20110106142416
s_13100_201101_21003_01_002.dat         1750000000          7000000             201101  20110106142416
s_13100_201101_21003_01_003.dat             ?                   ?               201101  20110106142416

注意：其中“1750000000”和“7000000”是体积大小和行数，体积直接看得到，行数用wc -l s_13100_201101_21003_01_001.dat 进行查看，
第3个文件需要自行统计进行填写，并且和长度和上一行对齐，总体长度一致。






月数据：
3号前接口：
ls -alrt *02049*dat *02053*dat *03001*dat *03002*dat *03003*dat *06011*dat *06012*dat *06029*dat

5号前接口：
ls -alrt *01005*dat *02005*dat *02014*dat *02015*dat *02016*dat *02047*dat *06021*dat *06022*dat *06023*dat *22009*dat *22101*dat *22103*dat *22105*dat *22106*dat *06002*dat
8号前接口：
ls -alrth *02006*dat *02007*dat *02052*dat *03004*dat *03005*dat *03012*dat \
*03015*dat *03016*dat *03017*dat *03018*dat *21003*dat *21006*dat *21008*dat \
*21011*dat *21012*dat *21020*dat *22204*dat *22036*dat *22040*dat *22072*dat \
*22303*dat *22304*dat *22305*dat *22306*dat *22307*dat *02017*dat *22401*dat 

10号前接口：
ls -alrt  *03007*dat *21010*dat *21013*dat *21014*dat *21015*dat *05001*dat \
*05002*dat *05003*dat *22013*dat *22021*dat *22025*dat *22032*dat *22033*dat \
*22039*dat *22041*dat *22042*dat *22043*dat  

15号前接口：
ls -lart *22049*dat *22050*dat *22052*dat *22055*dat *22056*dat *22061*dat \
*22062*dat *22063*dat *22064*dat *22065*dat  



11点前接口

ls -l *02063*dat *02064*dat *04004*dat *04005*dat *04014*dat *04017*dat *21001*dat *21002*dat *21004*dat *21005*dat *21007*dat *21009*dat *21016*dat *22012*dat *22201*dat *22202*dat *22203*dat *22301*dat *22302*dat 


update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = '每月10日前'
)

--update check
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select control_code from app.sch_control_runlog 
where control_code in (
select distinct control_code from   app.sch_control_before 
where  before_control_code in (
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = '每月10日前'
)
and control_code like '%CHECK%'
and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
)
and flag = 0
and date(endtime) < current date
)


--update exp
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select control_code from app.sch_control_runlog 
where control_code in (
select distinct control_code from   app.sch_control_before 
where  before_control_code in (
    select b.CONTROL_CODE from    
    BASS1.MON_ALL_INTERFACE a
    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
    and a.TYPE = 'm'
    and b.control_code like '%MONTH%'
    and upload_time = '每月10日前'
    )
    and control_code like 'BASS1%EXP%'
)
and flag = 0
and date(endtime) < current date
)
--and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')

BASS1_G_S_05001_MONTH.tcl
BASS1_G_S_05002_MONTH.tcl

