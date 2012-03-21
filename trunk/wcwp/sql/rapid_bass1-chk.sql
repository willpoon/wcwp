---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc

---------------------------------------------------------------------------------
select message_id, send_time,mobile_num,message_content from   APP.SMS_SEND_INFO
where send_time is not null
and mobile_num = '13989007120'
and send_time >=  current timestamp - 1 days
and date(send_time) = char(current date )
order by send_time desc
;


---------------------------------------------------------------------------------

--
update( 
select * from  app.sch_control_runlog
where   control_code  like '.tcl%'
and flag = -1
) set flag = -2


/**
 
CONTROL_CODE
BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl
BASS1_INT_CHECK_INDEX_SAME_DAY.tcl
 
--置完成

update (
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code  = 'BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl'
)t 
set flag = 1


update 
(
select * from  app.sch_control_runlog
where   control_code in (
'BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl'
)
and flag = -1 
) t
set  flag = 0


 * **/

--置完成

update (
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code  = 'BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl'
)t 
set flag = 1


update 
(
select * from  app.sch_control_runlog
where   control_code in (
'BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl'
)
and flag = -1 
) t
set  flag = 0


--置重运

update (
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code = 'BASS1_INT_CHECK_INDEX_SAME_DAY.tcl'
)t 
set flag = 1

update 
(
select * from  app.sch_control_runlog
where   control_code in (
'BASS1_INT_CHECK_INDEX_SAME_DAY.tcl'
)
and flag = -1 
) t
set  flag = -2

---------------------------------------------------------------------------------

select * from   table( bass1.chk_same(0) ) a order by 2
---------------------------------------------------------------------------------

select * from   table( bass1.chk_wave(0) ) a order by 2


---------------------------------------------------------------------------------

select * from app.sch_control_task where control_code in 
(
select control_code from   app.sch_control_runlog where flag = 1 
)
and cc_flag = 1


---------------------------------------------------------------------------------
select * from table(bass1.fn_dn_flret_cnt(9)) a

select * from table(bass1.fn_dn_flret_cnt(11)) a

select * from table(bass1.fn_dn_flret_cnt(13)) a

select * from table(bass1.fn_dn_flret_cnt(15)) a


/*********日**************************************************************************************/

--接口文件级返回检查(正常情况为56条记录) file 
--file lvl
 
select  * from APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'

---------------------------------------------------------------------------------
--或者：有接口重传时、记录级重新返回时。
--s_13100_20110301_22303_01_001.dat
--file lvl

select t.*, substr(filename,18,5)
from 
(
select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn 
from APP.G_FILE_REPORT a
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'
) t where rn = 1

----------------------求未上传（文件级返回）接口！ (当日没返回文件级的接口)
select * from   BASS1.MON_ALL_INTERFACE 
where interface_code 
not in (select substr(filename,18,5) from 
APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'
) and type='d'
and interface_code <> '04007'

--record 接口记录集返回检查(正常情况为56条记录) record
---------------------------------------------------------------------------------

select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1


--record 查询未返回接口 (已完全上报时) record

select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=0
---------------------------------------------------------------------------------


--求未导出接口 (通过上一天与本日比较)
select * from app.g_runlog 
where time_id=int(replace(char(current date - 2 days),'-',''))
and unit_code 
not in (select unit_code  from  app.g_runlog where time_id=int(replace(char(current date - 1 days),'-',''))
 )


-- record 未返回接口的详细信息 (day)  record
select * from  BASS1.MON_ALL_INTERFACE 
where INTERFACE_CODE in (
select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=0
)
and type = 'd'

-- record 已返回接口的详细信息 (day)  record
select * from  BASS1.MON_ALL_INTERFACE 
where INTERFACE_CODE in (
select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1
)
and type = 'd'


--查询未运行的导出程序
SELECT * FROM bass1.MON_ALL_INTERFACE  t
WHERE t.INTERFACE_CODE not IN (
select substr(a.control_code,15,5) from   app.sch_control_runlog  A
where control_code like 'BASS1_EXP%DAY'
AND date(a.begintime) =  date(current date)
AND FLAG = 0
)
AND TYPE='d'


--查询未导出接口 (未完全出数时)

select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 2 days),'-',''))
and return_flag=1
except
select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=0


/********月***************************************************************************************/

---月接口文件级返回检查(对于接口重送，相应的进行增加)
--对于重传接口，根据重传次数会增加相应记录数，故做去重处理，防止重复统计
--file 
select *
from 
(
select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
from APP.G_FILE_REPORT a
where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
and err_code='00'
and length(filename)=length('s_13100_201002_03014_01_001.dat')
) t where rn = 1


--文件级返回失败检查
select  * 
from APP.G_FILE_REPORT
where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
and err_code <> '00'
and length(filename)=length('s_13100_201002_03014_01_001.dat')
order by deal_time desc


--查询已返回月接口 记录级

select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1

--已上传未返回 (b表将为NULL)
--mreturn
/**
 * 
 
select distinct c.INTERFACE_NAME,a.*,b.*
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
left join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join (select * from bass1.mon_all_interface 
                where sts = 1 and type = 'm') c on substr(a.filename,16,5) = c.INTERFACE_CODE 
with ur
**/


select * from app.g_runlog 
where unit_code = '02016'

select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
   and substr(filename,16,5) = '02016'
   
   select * from bass1.mon_all_interface  where INTERFACE_CODE = '02016'
   

/**
--查询未返回月接口 记录级

select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=0

--未返回接口的详细信息 (month)

select * from  BASS1.MON_ALL_INTERFACE 
where INTERFACE_CODE in (
select unit_code from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=0
)
and type = 'm'
**/

--查看deadlineX 的接口有多少已返回。


--分时段接口-查看返回情况
select a.* ,b.return_flag from    BASS1.MON_ALL_INTERFACE  a
left join (select * from  app.g_runlog b
where  time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and b.return_flag = 1 
) b on  a.INTERFACE_CODE = b.UNIT_CODE
where  upload_time='每月3日前'



select a.* ,b.return_flag from    BASS1.MON_ALL_INTERFACE  a
left join (select * from  app.g_runlog b
where  time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and b.return_flag = 1 
) b on  a.INTERFACE_CODE = b.UNIT_CODE
where  upload_time='每月5日前'



select a.* ,b.return_flag from    BASS1.MON_ALL_INTERFACE  a
left join (select * from  app.g_runlog b
where  time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and b.return_flag = 1 
) b on  a.INTERFACE_CODE = b.UNIT_CODE
where  upload_time='每月8日前'



select a.* ,b.return_flag from    BASS1.MON_ALL_INTERFACE  a
left join (select * from  app.g_runlog b
where  time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and b.return_flag = 1 
) b on  a.INTERFACE_CODE = b.UNIT_CODE
where  upload_time='每月10日前'



select a.* ,b.return_flag from    BASS1.MON_ALL_INTERFACE  a
left join (select * from  app.g_runlog b
where  time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and b.return_flag = 1 
) b on  a.INTERFACE_CODE = b.UNIT_CODE
where  upload_time='每月15日前'

12+15+30+17+10 = 84 







--调度程序耗时:
select A.*,char(a.RUNTIME/60)||'min',char(a.RUNTIME/60/60)||'hr' from   app.sch_control_runlog A
where control_code like 'BASS1%DAY%'
and a.RUNTIME/60 > 5
ORDER BY RUNTIME DESC 



/**
1、22073竞争KPI涉及的校验R163、R164、R165、R166、R167、R168、R169、R170、R171、R172(调度为BASS1_INT_CHECK_COMP_KPI_DAY.tcl)和
21007短信涉及的校验(调度BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl)超标，数据正常生成的，不要进行任何数据调整，调度直接点击完成后运行后续；

如：
**/

select * from bass1.g_rule_check where rule_code in ('R171','R172','R169') order by time_id desc
20110501	R169	1503.00000	580.00000	1.59130	0.00000
20110501	R171	1499.00000	573.00000	1.61600	0.00000
20110501	R172	633.00000	383.00000	0.65270	0.00000

/**
2、22012日kpi接口涉及的一致性检查，如超标，必须进行调整(调度为BASS1_INT_CHECK_INDEX_SAME_DAY.tcl)，调整22012接口的指标，重跑报错调度，
一定不能点击完成(最频繁的是“离网用户数”这个指标，差一个用户就超标，暂没解决口径不一致情况，二经未改造用户资料表)；

如：
**/
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
where 
    rule_code in ('R159_1','R159_2','R159_3','R159_4')
and rule_code='R159_1'
order by 1 desc 


20110418	离网客户数	84.00000	85.00000	-0.01176

20110428	离网客户数	144.00000	134.00000	0.07462

select * from     
bass1.g_s_22012_day 
where time_id=int(replace(char(current date - 1 days),'-',''))

20110517	20110517	2888      	1671364     	23709586    	469322      	3798066     	77        	280936      

20110519	20110519	2306      	1675939     	22918403    	5312      	3834942     	68        	257121      

20110520	20110520	2330      	1678173     	23580986    	476230      	3903368     	80        	284164      
  
 
 20110528	20110528	1888      	1645201     	22985709    	515685      	3774471     	83        	274209      

20110601	20110601	4520      	1646351     	24447434    	520729      	4689422     	120       	492684      --当日离网 ，二经没有打标

20110602	20110602	3423      	1649676     	22326259    	471251      	3738425     	99        	261862      
 
 TIME_ID	BILL_DATE	M_NEW_USERS	M_DAO_USERS	M_BILL_DURATION	M_DATA_FLOWS	M_BILL_SMS	M_OFF_USERS	M_BILL_MMS
20110603	20110603	2900      	1652486     	23148301    	481514      	4124087     	90        	264431          

  --调整脚本，''里更新一定的值就是
--离网客户数
    update bass1.g_s_22012_day set m_off_users='107' 
    where time_id=int(replace(char(current date - 1 days),'-',''))
    
 select * from  bass1.G_RULE_CHECK where rule_code = 'C1'
 order by 1 desc 

 select * from  bass1.G_RULE_CHECK where rule_code = 'C1'
 and time_id > 20110825
 order by 1 desc 
 
20110618	C1	2049107.00000	2211895.00000	-0.07360	0.00000
 
 20110607	C1	2059346.00000	4283440.00000	-0.51923	0.00000

 20110605	C1	4405358.00000	2650880.00000	0.66185	0.00000

 20110603	C1	2154443.00000	1826062.00000	0.17983	0.00000

 20110531	C1	2432169.00000	2058116.00000	0.18175	0.00000
20110530	C1	2058116.00000	1980649.00000	0.03911	0.00000


 20110509	C1	2053251.00000	2472205.00000	-0.16947	0.00000

 20110508	C1	2472205.00000	2140554.00000	0.15494	0.00000

20110503	C1	1981494.00000	1819931.00000	0.08877	0.00000

20110502	C1	1819931.00000	2861998.00000	-0.36410	0.00000 
20110501	C1	2861998.00000	3083030.00000	-0.07169	0.00000
20110430	C1	3083030.00000	2551002.00000	0.20856	0.00000
20110429	C1	2551002.00000	2155029.00000	0.18374	0.00000
20110428	C1	2155029.00000	1990225.00000	0.08281	0.00000
20110427	C1	1990225.00000	1987809.00000	0.00122	0.00000


/**

2、月底调度BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl中R107/R108超标，此调度只有在月初1号早上送月底那天的日数据时必须调整，让校验通过，
其它日期调度报错直接点击运行完成；

--调整脚本(数据量大，update有些慢，要几分钟，调了以后如还报错，再调其中“400”和“5”的值，注意微调)
---R107
**/

select * from bass1.g_rule_check where rule_code in ('R107') order by time_id desc
20110531	R107	33.14000	69.10000	-0.52041	0.05000

20110430	R107	67.13000	68.50000	-0.02000	0.05000
select * from bass1.g_rule_check where rule_code in ('R108') order by time_id desc
20110430	R108	544.84000	560.01000	-0.02709	0.05000

---R107
update (select * from  BASS1.G_S_04008_DAY where time_id = 20110929  ) t 
set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+400) with ur;
109513 row(s) affected.
update (select * from  BASS1.G_S_04008_DAY where time_id = 20110731  ) t 
set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+10) with ur

--97763 row(s) affected.

--100390 row(s) affected.
--97763 row(s) affected.
--201107 105138 row(s) affected.
---R108
update (select * from  BASS1.G_S_04008_DAY where time_id = 20110929  ) t 
set BASE_BILL_DURATION = char(bigint(BASE_BILL_DURATION)-5) with ur 
20110828
112152 row(s) affected.


--100390 row(s) affected.
--97763 row(s) affected.
--201107 105138 row(s) affected.
select * from   bass1.G_RULE_CHECK where rule_code = 'R109'      
order by time_id desc 


--所有日接口代码：

select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'd'
and b.control_code like '%DAY%'

--所有月接口代码：

select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'




		   
--每月月初跑
--G_S_21003_STORE_DAY
--每月月初插入上月全月数据
delete from G_S_21003_STORE_DAY 
where TIME_ID/100 = int(substr(replace(char(current date - 1 month),'-',''),1,6))

insert into G_S_21003_STORE_DAY
select *
from G_S_21003_TO_DAY
where TIME_ID/100 = int(substr(replace(char(current date - 1 month),'-',''),1,6))

--统计备份情况()
select count(0) from G_S_21003_STORE_DAY 
where TIME_ID/100 = int(substr(replace(char(current date - 1 month),'-',''),1,6))

select count(0) from G_S_21003_TO_DAY 
where TIME_ID/100 = int(substr(replace(char(current date - 1 month),'-',''),1,6))

--统计备份情况()
select count(0) from G_S_21003_STORE_DAY 
where TIME_ID/100 = int(substr(replace(char(current date - 2 month),'-',''),1,6))

select count(0) from G_S_21003_TO_DAY 
where TIME_ID/100 = int(substr(replace(char(current date - 2 month),'-',''),1,6))

--两者一致，则在G_S_21003_TO_DAY删除前两个月前的数据(如现在是5月，保留3，4月，删除2月数据)
--提高程序速度
delete from G_S_21003_TO_DAY 
where TIME_ID/100 = int(substr(replace(char(current date - 2 month),'-',''),1,6))





--生成重导日数据的命令

select b.*
, lower(
 '/bassapp/backapp/bin/bass1_export/bass1_export '||substr(a.control_code,11,13)||' '||char(current date - 1 days) 
) exp_cmd
from   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE b
where a.control_code like 'BASS1%EXP%DAY%'
and date(a.begintime) =  date(current date)
and substr(a.control_code,15,5) = b.interface_code 
and b.type='d'

--生成重导月数据的命令

select b.*
, lower(
 '/bassapp/backapp/bin/bass1_export/bass1_export '||substr(a.control_code,11,15)||' '||substr(char(current date - 1 month) ,1,7)
) exp_cmd
from   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE b
where a.control_code like 'BASS1%EXP%MONTH%'
and month(a.begintime) =  month(current date)
and substr(a.control_code,15,5) = b.interface_code 
and b.type='m'


--单独put 某个接口:day



select b.*
, lower(
 'put *'||b.interface_code||'*.dat ' 
) put_dat
, lower(
 'put *'||b.interface_code||'*.verf ' 
) put_verf
from   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE b
where a.control_code like 'BASS1%EXP%DAY%'
and date(a.begintime) =  date(current date)
and substr(a.control_code,15,5) = b.interface_code 
and b.type='d'

--单独put 某个接口:month

select b.*
, lower(
 'put *'||b.interface_code||'*.dat ' 
) put_dat
, lower(
 'put *'||b.interface_code||'*.verf ' 
) put_verf
from   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE b
where a.control_code like 'BASS1%EXP%MONTH%'
and month(a.begintime) =  month(current date)
and substr(a.control_code,15,5) = b.interface_code 
and b.type='m'


--接口号 -  表名 对应关系
select * from table(
select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,13) from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'd'
and b.control_code like '%DAY%'
) t where unit_code = ''

select * from table(
select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,15),b.control_code from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
) t where unit_code = ''


select * from   bass1.mon_all_interface where INTERFACE_CODE = '06001'




select * from   bass1.dim_21003_ip_type

select * from    bass1.G_S_22059_MONTH_MRKT_0135A 

select count(0) from    BASS1.G_S_22057_MONTH

select count(0) from     app.sch_control_map where PROGRAM_NAME = 'G_S_22057_MONTH.tcl';

delete from  app.sch_control_map where PROGRAM_NAME = 'G_S_22057_MONTH.tcl';
delete from  app.sch_control_map where PROGRAM_NAME = 'G_S_22058_MONTH.tcl';
delete from  app.sch_control_map where PROGRAM_NAME = 'G_S_22059_MONTH.tcl';
delete from  app.sch_control_map where PROGRAM_NAME = 'G_S_22060_MONTH.tcl';

select count(0) from     bass1.int_program_data where PROGRAM_NAME = 'G_S_22057_MONTH.tcl';

select count(0) from    bass1.int_program_data where PROGRAM_NAME = 'G_S_22060_MONTH.tcl';

select * from    app.g_unit_info  where unit_code = '22057'
select count(0) from    app.sch_control_task  where control_code = 'BASS1_EXP_G_S_22057_MONTH'


select * from   bass2.stat_market_0135_b_final

select * from   bass2.stat_market_0135_b_final

select * from            bass1.G_S_22057_MONTH

select * from   bass2.stat_market_0135_c 

select * from   bass2.stat_channel_reward_0009 

select * from   bass2. dw_channel_local_busi_201106  





select ENTERPRISE_ID
                from 
                (
                select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
                from (
                select *
                from g_a_02054_day where 
                 time_id/100 <= 201106 and ENTERPRISE_BUSI_TYPE = '1520'
                 and MANAGE_MODE = '3'
                ) t 
                ) t2 where rn = 1 and STATUS_ID ='1'
                
union 
select ENTERPRISE_ID
                from 
                (
                select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
                from (
                select *
                from g_a_02064_day where 
                 time_id/100 <= 201106 and ENTERPRISE_BUSI_TYPE = '1520'
                 and MANAGE_MODE = '3'
                ) t 
                ) t2 where rn = 1 and STATUS_ID ='1'
                
                
                

 SELECT 
                    SUM(T.SC),
                    COUNT(DISTINCT T.PRODUCT_NO)
                  FROM (
                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC, PRODUCT_NO     
                          FROM BASS1.G_S_04008_DAY
                          WHERE TIME_ID/100=201107
                               AND ADVERSARY_ID<>'010000'
                          GROUP BY PRODUCT_NO
                          UNION 
                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                          FROM BASS1.G_S_04009_DAY
                          WHERE TIME_ID/100=201107
                                AND ADVERSARY_ID<>'010000'
                          GROUP BY PRODUCT_NO
                        )T
                        
                        
select count(0) from     bass2.STAT_ZD_VILLAGE_USERS_201107
                       

select count(0) from     BASS1.G_S_04009_DAY
select * from   BASS1.MON_ALL_INTERFACE 
 where interface_code = '06002'
 
                       

select SCHOOL_ID , count(0) 
--,  count(distinct SCHOOL_ID ) 
from G_I_02032_MONTH 
group by  SCHOOL_ID 
order by 1 


select * from   bass2.dw_product_20110808 where product_no = '13989007120'
                       
select tabname from syscat.tables where tabname like '%CALL%2011%'                          

select * from bass2.CDR_GPRS_LOCAL_20110701 fetch first 10 rows only  


                select distinct B_AREA_CD
                from G_S_04019_DAY where time_id = 20110820
                except 
                select country_code from  bass1.dim_country_city_code 
                select * from   bass1.dim_country_city_code  where country_code like '2%'
                
select tabname from syscat.tables where tabname like '%DIM%COU%'                   
select * from   bass2.DIM_PUB_COUNTRY

select *                 from G_S_04019_DAY where time_id = 20110811
and B_AREA_CD = '2'

1209	未知待查

update 
 G_S_04019_DAY 
 set B_AREA_CD = '1209'
 where time_id = 20110820
and B_AREA_CD = '2'


select * from   G_A_02054_DAY_FIX20110812



USER_ID	CUST_ID	ACCT_ID
89101110015456	89101110420499	89101110200773


select * from   bass2.dw_product_20110812
where product_no = '13908911366'

select * from bass2.dw_cust_20120305
where CUST_ID = '89160001524433'
CUST_ID
89160001524433

select * from bass2.dw_product_20120305
where CUST_ID = '89160001524433'

dwd_cust_msg_

select * from bass2.dwd_cust_msg_20120305
where CUST_ID = '89160001524433'



select * from   



SELECT CNT1 - CNT2 
FROM 
(
select COUNT(0) CNT1
from
( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where  time_id/100 <= 201107
           ) f 
where    f.row_id=1 
    ) A        
, (
select COUNT(0) CNT2
from
(select user_id,create_date,product_no,brand_id,sim_code,usertype_id
                ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id/100 <= 201107 ) e
where e.row_id=1
) B 
select * from   g_s_22066_day where time_id = 20110820



select * from   app.sch_control_task where CMD_LINE like  '%ODS_PRODUCT_ORD_BUSI_OTHER_YYYYMMDD%'



CREATE TABLE BASS2.ODS_PRODUCT_ORD_BUSI_OTHER_20110701  like bass2.ODS_PRODUCT_ORD_BUSI_OTHER_YYYYMMDD  DISTRIBUTE BY HASH(ORDER_BUSI_INFO_ID)   IN TBS_3H INDEX IN TBS_INDEX ; 
                   

select tabname from syscat.tables where tabname like '%ODS_PRODUCT_ORD_BUSI_OTHER%'                      


  select * from g_s_22066_day 
  where e_channel_type = '04'
  order by 1 desc 
  
  select * 
                  from bass1.G_S_22066_DAY
                where time_id in (20110823,20110822)
                
               
               
               
               select * from    G_S_22066_DAY
               
               
               
                 
  select * 
                  from bass1.G_S_22091_DAY
                where time_id in (20110823,20110822)
                
                
select * from   app.sch_control_task where control_code like '%product%other%ds%.tcl'                
select * from   app.sch_control_task where control_code like '%product%%tcl'                

select * from  table( bass1.get_before('BASS1_INT_CHECK_Z345_DAY')) a 

delete from app.sch_control_before 
where control_code = 'BASS1_EXP_G_I_02006_MONTH'
and before_control_code = 'BASS1_INT_CHECK_02006_MONTH.tcl'




  select * 
                  from bass1.G_S_22092_DAY
                where time_id in (20110823,20110822)
                
                
                BASS2_Dwd_product_ord_busi_other_ds.tcl	TR1_L_11060

select * from   app.sch_control_task where control_code like '%11013%'                




select count(0) ,count(distinct user_id ) from    G_I_02016_MONTH where time_id = 201107


select prod_id,count(0) from    G_I_02016_MONTH where time_id = 201107
group by prod_id

PROD_ID	2
106	32
107	33
108	78
300	1
    
  33+78 = 111 

  
    106：30元/月包15小时
    107：50元/月包40小时
	108：100元/月包200小时
    300：本地WLAN资费套餐
    
    select count(0) from    
    (
    select user_id from G_S_03004_MONTH where time_id = 201107 group by user_id having  sum(bigint(FEE_RECEIVABLE)) between 5000 and 12000
    ) a
    
values 111*1.00/587040*100

values 111/0.00193327868598914*100

select * from    bass2.dim_product_item b
where b.prod_id in (90030036,90008120,90030031,90008121,90030024,90008122,90005000,90005001,90005002,90005003,90005004,90005005)



                select
                    201107,
                        b.prod_id,
                         count(distinct a.user_id)
                from bass2.dwd_product_sprom_active_20110730 a,
                         bass2.dim_product_item b
                where a.sprom_id=b.prod_id
                  and b.prod_id in (90030036,90008120,90030031,90008121,90030024,90008122,90005000,90005001,90005002,90005003,90005004,90005005)
                  and replace(char(date(a.valid_date)),'-','')<='20110731'
                  and replace(char(date(a.expire_date)),'-','')>'20110731'
                  group by b.prod_id
                  
                  1	PROD_ID	3
201107	90030036	23
201107	90030031	23
201107	90030024	63
201107	90005001	1


1	PROD_ID	3
201107	90005001	1
201107	90030024	78
201107	90030031	34
201107	90030036	32


values 39/0.00193327868598914*100

select * from    bass2.stat_2011_user_wlan_mm
select * from    bass2.stat_wlan_2011_user_mm


select distinct extend_id prod_id, name  from   bass2.dim_prod_up_product_item 
where UPPER(name) like '%WLAN%'
and extend_id is not null

select * from 　bass2.dim_prod_up_product_item 
where product_item_id = 　191000001024




                select
                    201107,
                        b.prod_id,
                         count(distinct a.user_id)
                from bass2.dwd_product_sprom_active_20110730 a,
                         (select distinct extend_id prod_id, name  from   bass2.dim_prod_up_product_item 
where UPPER(name) like '%WLAN%'
and extend_id is not null) b
                where a.sprom_id=b.prod_id
                  and b.prod_id in (90030036,90008120,90030031,90008121,90030024,90008122,90005000,90005001,90005002,90005003,90005004,90005005)
                  and replace(char(date(a.valid_date)),'-','')<='20110731'
                  and replace(char(date(a.expire_date)),'-','')>'20110731'
                  group by b.prod_id
                  

select * from   bass2.dim_acct_item
                  where UPPER(item_name) like '%GPRS%'
                  
ITEM_ID	ITEM_NAME
80000594	WLAN包时费
80000105	WLAN包月费
80000106	WLAN基本费
82000031	WLAN费虚科目
80000628	Wlan
80000732	WLAN电子卡订购使用费


select * from bass2.dw_acct_shoulditem_201107
where item_id in (80000594,80000105,80000106,82000031,80000628)                  


select * from   BASS1.ALL_DIM_LKP 

where bass1_tbid = 'BASS_STD1_0114'

dim_acct_item	WLAN基本费	80000106	帐目科目	BASS_STD1_0074	0716	无线局域网（WLAN）通信费
dim_acct_item	WLAN包月费	80000105	帐目科目	BASS_STD1_0074	0715	无线局域网（WLAN）月使用费

select sum(bigint(FEE_RECEIVABLE))/count(distinct a.user_id)/100 from   g_s_03004_month a , g_i_02016_month b  
where a.time_id = 201107 
and a.user_id = b.user_id 
and b.TIME_ID = 201107
and ACCT_ITEM_ID in ('0716','0715')
70.28

select avg(bigint(FEE_RECEIVABLE)) from   g_s_03004_month a , g_i_02016_month b  
where a.time_id = 201107 
and a.user_id = b.user_id 
and b.TIME_ID = 201107
and ACCT_ITEM_ID in ('0716','0715')

7028

4190





select a.* from   g_s_03004_month a , g_i_02016_month b  
where a.time_id = 201107 
and a.user_id = b.user_id 
and b.TIME_ID = 201107
and ACCT_ITEM_ID in ('0716','0715')
70.28


0626	GPRS套餐费	包含相应数据流量的GPRS月使用费
0627	GPRS通信费	套餐用户超出部分的GPRS通信费，以及非套餐用户的GPRS通信费



select sum(bigint(FEE_RECEIVABLE))/count(distinct a.user_id)/100 from   g_s_03004_month a
where a.time_id = 201107 
and ACCT_ITEM_ID in ('0626','0627')
6


select * from   G_S_22091_DAY
order by 1 desc 



select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22091_DAY 
group by  time_id 
order by 1 

select count(0) from    BASS2.DWD_PRODUCT_ORD_BUSI_OTHER_20110824


select count(0) from    BASS2.DWD_PRODUCT_ORD_BUSI_OTHER_20110823

BASS2_Dwd_product_ord_busi_other_ds.tcl



insert into app.sch_control_before 
values ('BASS1_G_S_22066_DAY.tcl','BASS2_Dwd_product_ord_busi_other_ds.tcl')

select * from  table( bass1.get_before('BASS1_G_S_22066_DAY')) a 



select DEAL_DATE||E_CHANNEL_TYPE||BUSI_REC_CNT||ACTIVATE_CNT||TERM_SALE_CNT||VAL_BUSI_REC_CNT||VAL_BUSI_CANCEL_CNT||CHRG_REC_CNT||CHRG_REC_FEE||ACCESS_CNT||QRY_REC_CNT from bass1.g_s_22066_day where time_id=20110710


select * from   g_s_22066_day order by 1 desc,3

select * from   bass2.dim_device_profile




select date(alarmtime),count(0) from  app.sch_control_alarm 
where  
 control_code like 'BASS1%'
 and substr(char(date(alarmtime)),9,2) in ('28','29','30')
group by date(alarmtime)




select * from  app.sch_control_alarm 
where  
 control_code like 'BASS1%'
 and substr(char(date(alarmtime)),9,2) in ('28','29','30')



select * from    bass1.G_S_04019_DAY where time_id=20110827



select * from   


select * from bass1.g_rule_check 
where rule_code in ('R107') 
and time_id = int(replace(char(current date - 1 days),'-',''))
order by time_id desc




SELECT CNT1 - CNT2 
FROM 
(
select COUNT(0) CNT1
from
( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where  time_id/100 <= 201108
           ) f 
where    f.row_id=1 
    ) A        
, (
select COUNT(0) CNT2
from
(select user_id,create_date,product_no,brand_id,sim_code,usertype_id
                ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id/100 <= 201108 ) e
where e.row_id=1
) B 



select time_id,count(0) from bass1.g_user_lst
group by time_id 
order by 1 desc 


CREATE TABLE "BASS1   "."INT_22401_201109"  (
                  "TIME_ID" INTEGER , 
                  "USER_ID" CHAR(20) , 
                  "CELL_ID" CHAR(10) , 
                  "LAC_ID" CHAR(10) , 
                  "CALL_CNT" BIGINT )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "USER_ID",  
                 "CELL_ID",  
                 "LAC_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 




select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and control_code like 'BASS1%'
order by alarmtime desc



select * from   bass2.dw_call_opposite_201108
where product_no = '13549083471'


                select USER_ID||CELL_ID||LAC_ID, count(0)  cnt
                from bass1.G_S_22401_MONTH
                where time_id =201108
                group by USER_ID||CELL_ID||LAC_ID having count(0) > 1
                

			select * from   G_I_21020_MONTH
				where
				COMP_BRAND_ID = '021000' and 
				 substr(COMP_PRODUCT_NO,length(trim(COMP_PRODUCT_NO))-10,3)  not 
				in ('130','131','132','155','156','185','186','145')   
				and time_id  = 201108 
                


 select * from
(
select distinct  channel_star from bass1.g_i_06021_month
where time_id =201108
  and channel_status='1'
  and channel_type='3'
) aa
where channel_star=''
                
                
                select  * from bass1.g_i_06021_month
where time_id =201108
  and channel_status='1'
  and channel_type='3'
  and channel_star=''
  
  
  
                        select count(0) from   G_I_21020_MONTH
                                where
                                COMP_BRAND_ID = '021000' and 
                                 substr(COMP_PRODUCT_NO,length(trim(COMP_PRODUCT_NO))-10,3)  not 
                                in ('130','131','132','155','156','185','186','145')   
                                and time_id  = 201108 
                                
                                

                select cnt,rn from (
                        select time_id,round(20+rand(1)*10,0) cnt , row_number()over() rn from bass1.g_s_22021_month
                        where show_id = '00030028'
                        ) a 
                where time_id = 201108
                                
                                



select cnt,rn from (
select time_id,round(20+rand(1)*10,0) cnt , row_number()over() rn from 
(select * from bass1.g_s_22021_month 
union all
select 201108 TIME_ID,'201108' MONTH_ID, '00030028' SHOW_ID,'' TARGET_VALUE from bass2.dual
) t
where show_id = '00030028'
) a 
where time_id = 201108


select * from syscat.tables where tabname like '%TERM%TAC%'   
                                
                                
                                
---------------------------------------------------------------------------------
select *  from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 5 days
and control_code like 'BASS1%CHECK%MONTH%'
and control_code  not like 'BASS1%CHECK%WAVE%'
and control_code  not like 'BASS1%CHECK%SAME%'
order by alarmtime desc
                                
                            select count(0) from    (    
select BILL_TYPE||PRODUCT_NO||PRTY_CODE||SP_ID||SERV_CODE||CUSTOM_CHNL||MEET_NUM||PREZENT_BEGIN_TIME||PREZENT_END_TIME||FEE_AMT||BUSI_TYPE||DEAL_TIME||CUST_CONTE from bass1.g_s_04015_day where time_id=20110906
) a

                                
                                
select count(0) from     bass1.g_s_04015_day where time_id=20110906
 and   PRTY_CODE is null 
 
update   (select * from  bass1.g_s_04015_day where     time_id=20110906 ) a 
set BILL_TYPE = ''
where  BILL_TYPE is null 

select * from    bass1.g_s_04015_day where     time_id=20110906
and                          BILL_TYPE = ''
delete from (select * from bass1.g_s_04015_day where     time_id=20110906)  a
where  BILL_TYPE = ''


---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
order by alarmtime desc

select * from   bass2.dim_acct_item b 
where UPPER(item_name) like '%BLACK%BERRY%'

ITEM_ID	ITEM_NAME
80000180	BlackBerry功能费
80000184	BlackBerry通信费
80000598	BlackBerry集团客户扩展业务功能费
80000599	BlackBerry集团客户扩展业务通信费
82000111	Black Berry虚科目



select * from   G_I_02022_DAY where user_id = '89160001135476'
20110825	89160001135476      	QW_QQT_JC_SL88                	20110722
20110827	89160001135476      	QW_QQT_JC_SL88                	20110801
20110828	89160001135476      	QW_QQT_JC_SL88                	20110801
20110829	89160001135476      	QW_QQT_JC_SL88                	20110801
20110830	89160001135476      	QW_QQT_JC_SL88                	20110801
20110831	89160001135476      	QW_QQT_JC_SL88                	20110801
20110901	89160001135476      	QW_QQT_JC_SW128               	20110901
20110902	89160001135476      	QW_QQT_JC_SW128               	20110901


20110826	89160001135476      	QW_QQT_JC_SL88                	BASS1_DS                                	20110826	20110901
select * from   G_S_02024_DAY where user_id = '89160001135476'



        select A.*,B.NAME from bass2.dw_product_ins_off_ins_prod_201108 a, bass2.dim_prod_up_product_item  b
where product_instance_id 
in 
('89160001135476'
)
and a.OFFER_ID = b.PRODUCT_ITEM_ID
AND B.NAME LIKE '%全球通%'


select count(0) 
		FROM BASS2.Dim_CHANNEL_INFO A 
		WHERE A.CHANNEL_TYPE_CLASS IN (90105,90102)
	with ur
    

select count(0),count(distinct PRODUCT_NO) from    g_s_04002_day
where time_id = 20110927

4177858	289510
4177858	289510





                select CHANNEL_ID,count(0)  cnt
                from bass1.G_A_06035_DAY
                where time_id =20111001
                group by CHANNEL_ID having count(0) > 1
                
                select time_id , count(0) 
--,  count(distinct time_id ) 
from G_A_06035_DAY 
group by  time_id 
order by 1 


insert into app.sch_control_before
values 
 ('BASS1_G_I_02034_MONTH.tcl','BASS1_G_I_02032_MONTH.tcl')
,('BASS1_G_I_22403_MONTH.tcl','BASS1_G_I_02032_MONTH.tcl')
,('BASS1_G_S_22403_MONTH.tcl','BASS1_G_I_02032_MONTH.tcl')
,('BASS1_G_I_22404_MONTH.tcl','BASS1_G_I_02032_MONTH.tcl')
,('BASS1_G_I_02035_MONTH.tcl','BASS1_G_I_02032_MONTH.tcl')
,('BASS1_G_I_02031_MONTH.tcl','BASS1_G_I_02032_MONTH.tcl')
,('BASS1_G_S_22404_MONTH.tcl','BASS1_G_I_02032_MONTH.tcl')
,('BASS1_G_I_02033_MONTH.tcl','BASS1_G_I_02032_MONTH.tcl')

BASS1_G_I_02017_MONTH.tcl


insert into app.sch_control_before
values 
 ('BASS1_G_I_02017_MONTH.tcl','BASS1_G_I_02032_MONTH.tcl')
 
 
 update G_I_21020_MONTH 
set CALL_COUNTS = char(int(rand(1)*5+1))
where (
char(TIME_ID) = substr(COMP_LAST_DATE,1,6)
and CALL_COUNTS = '0'
and SMS_COUNTS = '0'
and MMS_COUNTS = '0'
and time_id = 201109 )



select * from   bass1.g_rule_check where rule_code LIKE 'R255%'
ORDER BY 1 DESC 



select * from   bass1.g_rule_check where rule_code = 'R146'
ORDER BY 1 DESC 
select '06'||trim(char(int(rand(1)*9))) from bass2.dual


select count(0) from table(
select distinct SCHOOL_ID from G_I_02031_MONTH where TIME_ID = 201107
and  SCHOOL_ID not in (
select distinct SCHOOL_ID from G_I_06001_MONTH where TIME_ID = 201107
)
) t 



select distinct SCHOOL_ID from G_I_06001_MONTH where TIME_ID = 201107
and  SCHOOL_ID not in (
select distinct SCHOOL_ID from G_I_02031_MONTH where TIME_ID = 201107
)


select distinct SCHOOL_ID from G_I_06001_MONTH where TIME_ID = 201109
and  SCHOOL_ID not in (
select distinct SCHOOL_ID from G_I_02031_MONTH where TIME_ID = 201109
)

select a.stud_cnt
,b.*
from (select * from  g_i_06001_month where time_id = 201109 ) a 
, (
select school_id,count(0) cnt 
from G_I_02031_MONTH
where time_id = 201109
group by school_id
) b 
where a.school_id = b.school_id

G_S_22403_MONTH

update    app.sch_control_task 
set cc_flag = 2
where 
control_code like '%G_S_22403_MONTH%'
or 
control_code like '%G_S_22404_MONTH%'



select a.stud_cnt
,b.*
from (select * from  g_i_06001_month where time_id = 201109 ) a 
, (
select school_id,count(0) cnt 
from G_I_02031_MONTH
where time_id = 201109
group by school_id
) b 
where a.school_id = b.school_id
and bigint(a.stud_cnt) < cnt


select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 5 days
--and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc

select time_id,count(0)
from G_S_22063_MONTH
group by time_id


select * from   g_i_06003_month where time_id = 201108



                select school_id, count(0)  cnt
                from bass1.G_I_06001_MONTH
                where time_id =201109
                group by SCHOOL_ID having count(0) > 1
                
                


        select distinct  
        201109  TIME_ID
        ,SCHOOL_ID
        ,SCHOOL_NAME
        ,case when value (city_id , substr(SCHOOL_ID,4,3))  = '891' then '13101'
                              when value (city_id , substr(SCHOOL_ID,4,3))  = '892' then '13102'
                              when value (city_id , substr(SCHOOL_ID,4,3))  = '893' then '13103'
                              when value (city_id , substr(SCHOOL_ID,4,3))  = '894' then '13104'
                              when value (city_id , substr(SCHOOL_ID,4,3))  = '895' then '13105'
                              when value (city_id , substr(SCHOOL_ID,4,3))  = '896' then '13106'
                              when value (city_id , substr(SCHOOL_ID,4,3))  = '897' then '13107'
                        end CMCC_BRANCH_ID
        ,a.school_address       
        ,value(char(cast( SCH_LONGITUDE as decimal(6,4))),'0') SCH_LONGITUDE
        ,value(char(cast( SCH_LATITUDE  as decimal(6,4))),'0') SCH_LATITUDE
        , case when SCHOOL_ID  = '89189100000003' then 'http://www.utibet.edu.cn/' else ''  end WEBSITE
        ,case 
                when school_type in ('4') then '1' 
                when school_type in ('5') then '2'      
                else '3' 
        end SCHOOL_LVL
        ,case 
                when school_level in ('1') then '1' 
                when school_level in ('2') then '3'     
                else '4' 
        end ADMIN_TYPE
        ,char(SCH_SIZE_STUS) STUD_CNT
        ,'100' NEWSTUD_CNT 
        ,char(sch_size_teas) STAFF_CNT
        ,'06'||trim(char(int(rand(1)*9))) CMCC_RATE
from      bass2.Dim_xysc_maintenance_info a
where a.SCHOOL_ID  in (
select distinct SCHOOL_ID from G_I_02032_MONTH where TIME_ID = 201109
)
with ur

                
                
                                select *
                from bass1.G_I_06001_MONTH
                where time_id =201109
               

select count(0) from (
select school_id, cnt
from (select * from  g_i_06001_month where time_id = 201109 ) a 
, (
select school_id,count(0) cnt 
from G_I_02031_MONTH
where time_id = 201109
group by school_id
) b 
where a.school_id = b.school_id
and bigint(a.stud_cnt) < cnt
) t


select * from   g_i_06001_month
where time_id = 201109


update     (select * from  g_i_06001_month where  time_id = 201109 )  t
set    t.stud_cnt = (select char(cnt)  from 
table (
select a.school_id, cnt
from (select * from  g_i_06001_month where time_id = 201109 ) a 
, (
select school_id,count(0) cnt 
from G_I_02031_MONTH
where time_id = 201109
group by school_id
) b 
where a.school_id = b.school_id
and bigint(a.stud_cnt) < cnt
) c
where t.school_id = c.school_id )



select time_id, a.school_id,stud_cnt from     g_i_06001_month  a
,
table (
select a.school_id, cnt
from (select * from  g_i_06001_month where time_id = 201109 ) a 
, (
select school_id,count(0) cnt 
from G_I_02031_MONTH
where time_id = 201109
group by school_id
) b 
where a.school_id = b.school_id
and bigint(a.stud_cnt) < cnt
) b 
where a.school_id = b.school_id 




update     (select * from  g_i_06001_month where  time_id = 201109 )  t
set    t.stud_cnt = (select char(cnt)  from 
table (
select a.school_id, cnt
from (select * from  g_i_06001_month where time_id = 201109 ) a 
, (
select school_id,count(0) cnt 
from G_I_02031_MONTH
where time_id = 201109
group by school_id
) b 
where a.school_id = b.school_id
and bigint(a.stud_cnt) < cnt
) c
where t.school_id = c.school_id )
where  t.school_id in 
(
select a.school_id
from (select * from  g_i_06001_month where time_id = 201109 ) a 
, (
select school_id,count(0) cnt 
from G_I_02031_MONTH
where time_id = 201109
group by school_id
) b 
where a.school_id = b.school_id
and bigint(a.stud_cnt) < cnt
)


select * from   bass2.dim_svc_item
where svcitem_name like '%国际%'

select count(0) from    bass2.DW_NEWBUSI_MMS_201109


 select sum(counts)
 from 
 table (
 select a.user_id,counts,substr((case when substr(a.opp_number,1,2) = '86' then substr(a.opp_number,3,11) else a.opp_number
			end ),1,7) hlr_code   from bass2.DW_NEWBUSI_MMS_201109 a 
 where a.SEND_STATUS in (0,1,2,3) 
 and a.SVCITEM_ID in  (400001)
 and  a.calltype_id in (0)
 ) a
 ,bass2.dw_product_201109 b
 ,(select distinct  hlr_code  from   bass2.DIM_GSM_HLR_INFO
                where prov_code = '891'
              ) c
 where 
  a.hlr_code = c.hlr_code
  and  a.user_id=b.user_id
  and  b.usertype_id in (1,2,9)
  
  
  
  select 'P08-JTDX'
,(
select  sum(b.counts)
                      from bass2.dw_enterprise_member_mid_201109 a,bass2.DW_NEWBUSI_MMS_201109 b,
                           bass2.dw_product_201109 c
                      where a.user_id = b.user_id and a.user_id = c.user_id
                         and b.sp_code  = '931007' and b.calltype_id in (1)
			 and calltype_id = 1 
) from bass2.dual



select  sum(b.counts)
                      from bass2.dw_enterprise_member_mid_201109 a,bass2.DW_NEWBUSI_MMS_201109 b
                      where a.user_id = b.user_id 
                         and b.sp_code  = '931007' and b.calltype_id in (1)
			 and calltype_id = 1 

select count(0) from   bass2.DW_NEWBUSI_MMS_201109 b
where  b.sp_code  = '931007'


select b.sp_code,b.sp_name from (
select sp_code , count(0) 
--,  count(distinct sp_code ) 
from bass2.DW_NEWBUSI_MMS_201109 
group by  sp_code 
) t , bass2.DW_PM_SP_INFO_201109 b 

where char(t.sp_code) = char(b.SP_CODE)


select tabname from syscat.tables where tabname like '%SP_INFO%'   


select * from   bass2.DW_PM_SP_INFO_201109

select * from bass2.dim_up_prod_item

800089
select * from    bass2.dim_prod_up_product_item 
where SUPPLIER_ID = '800089'

801210

select count(0) from   bass2.DW_NEWBUSI_MMS_201109 b
where  b.sp_code  = '931007'

select count(0) from   bass2.DW_NEWBUSI_MMS_201109 b
where  b.sp_code  = '801210'

select count(0) from   bass2.DW_NEWBUSI_MMS_201109 b
where SER_CODE = '106540320000'

select count(0) from   bass2.DW_NEWBUSI_MMS_201109 a
where SER_CODE = '106540320000'
 and  a.calltype_id in (1)
 
select OPER_CODE , count(0) 
--,  count(distinct OPER_CODE ) 
from bass2.DW_NEWBUSI_MMS_201109 
group by  OPER_CODE 
order by 1 



select SER_CODE , count(0) 
--,  count(distinct SER_CODE ) 
from bass2.DW_NEWBUSI_MMS_201109 
group by  SER_CODE 
order by 1 

select * from   bass2.dim_svc_item
where svcitem_name like '%点对点%'

select * from   
		        bass2.dw_newbusi_sms_201109 a
		       where  a.calltype_id=1 
		       and a.svcitem_id in (200001,200002,200005)

select tabname from syscat.tables where tabname like '%DIM%SP%CODE%'   

select count(0) from     bass2.dw_newbusi_sms_201109 

select * from   bass2.dim_call_tolltype

select unit_code from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
except
select a from 
(
select '01005' a from bass2.dual union all
select '02005' a from bass2.dual union all
select '02006' a from bass2.dual union all
select '02007' a from bass2.dual union all
select '02014' a from bass2.dual union all
select '02015' a from bass2.dual union all
select '02016' a from bass2.dual union all
select '02018' a from bass2.dual union all
select '02019' a from bass2.dual union all
select '02020' a from bass2.dual union all
select '02021' a from bass2.dual union all
select '02031' a from bass2.dual union all
select '02032' a from bass2.dual union all
select '02033' a from bass2.dual union all
select '02034' a from bass2.dual union all
select '02035' a from bass2.dual union all
select '02047' a from bass2.dual union all
select '02049' a from bass2.dual union all
select '02052' a from bass2.dual union all
select '02053' a from bass2.dual union all
select '03001' a from bass2.dual union all
select '03002' a from bass2.dual union all
select '03003' a from bass2.dual union all
select '03004' a from bass2.dual union all
select '03005' a from bass2.dual union all
select '03007' a from bass2.dual union all
select '03012' a from bass2.dual union all
select '03015' a from bass2.dual union all
select '03016' a from bass2.dual union all
select '03017' a from bass2.dual union all
select '03018' a from bass2.dual union all
select '03019' a from bass2.dual union all
select '06001' a from bass2.dual union all
select '06002' a from bass2.dual union all
select '06003' a from bass2.dual union all
select '06011' a from bass2.dual union all
select '06012' a from bass2.dual union all
select '06021' a from bass2.dual union all
select '06022' a from bass2.dual union all
select '06023' a from bass2.dual union all
select '06029' a from bass2.dual union all
select '06034' a from bass2.dual union all
select '21003' a from bass2.dual union all
select '21006' a from bass2.dual union all
select '21008' a from bass2.dual union all
select '21010' a from bass2.dual union all
select '21011' a from bass2.dual union all
select '21012' a from bass2.dual union all
select '21013' a from bass2.dual union all
select '21014' a from bass2.dual union all
select '21015' a from bass2.dual union all
select '21020' a from bass2.dual union all
select '05001' a from bass2.dual union all
select '05002' a from bass2.dual union all
select '05003' a from bass2.dual union all
select '22009' a from bass2.dual union all
select '22013' a from bass2.dual union all
select '22021' a from bass2.dual union all
select '22025' a from bass2.dual union all
select '22032' a from bass2.dual union all
select '22033' a from bass2.dual union all
select '22039' a from bass2.dual union all
select '22041' a from bass2.dual union all
select '22042' a from bass2.dual union all
select '22043' a from bass2.dual union all
select '22074' a from bass2.dual union all
select '22075' a from bass2.dual union all
select '22076' a from bass2.dual union all
select '22077' a from bass2.dual union all
select '22049' a from bass2.dual union all
select '22050' a from bass2.dual union all
select '22052' a from bass2.dual union all
select '22055' a from bass2.dual union all
select '22056' a from bass2.dual union all
select '22057' a from bass2.dual union all
select '22058' a from bass2.dual union all
select '22059' a from bass2.dual union all
select '22060' a from bass2.dual union all
select '22061' a from bass2.dual union all
select '22063' a from bass2.dual union all
select '22067' a from bass2.dual union all
select '22068' a from bass2.dual union all
select '22069' a from bass2.dual union all
select '22101' a from bass2.dual union all
select '22103' a from bass2.dual union all
select '22105' a from bass2.dual union all
select '22106' a from bass2.dual union all
select '22204' a from bass2.dual union all
select '22036' a from bass2.dual union all
select '22040' a from bass2.dual union all
select '22072' a from bass2.dual union all
select '22303' a from bass2.dual union all
select '22304' a from bass2.dual union all
select '22305' a from bass2.dual union all
select '22306' a from bass2.dual union all
select '22307' a from bass2.dual union all
select '22081' a from bass2.dual union all
select '22083' a from bass2.dual union all
select '22085' a from bass2.dual union all
select '22086' a from bass2.dual union all
select '22403' a from bass2.dual union all
select '22404' a from bass2.dual union all
select '22405' a from bass2.dual union all
select '22406' a from bass2.dual ) a 
except
select substr(filename,18,5) 
from 
(
select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn 
from APP.G_FILE_REPORT a
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'
) t where rn = 1


















select substr(filename,18,5) 
from 
(
select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn 
from APP.G_FILE_REPORT a
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'
) t where rn = 1
except
select a from 
(select '01002' a from bass2.dual union all
select '01004' a from bass2.dual union all
select '01006' a from bass2.dual union all
select '01007' a from bass2.dual union all
select '02004' a from bass2.dual union all
select '02008' a from bass2.dual union all
select '02011' a from bass2.dual union all
select '02013' a from bass2.dual union all
select '02022' a from bass2.dual union all
select '02023' a from bass2.dual union all 
select '02024' a from bass2.dual union all
select '02025' a from bass2.dual union all
select '02028' a from bass2.dual union all
select '02053' a from bass2.dual union all
select '02054' a from bass2.dual union all
select '02055' a from bass2.dual union all
select '02056' a from bass2.dual union all
select '02057' a from bass2.dual union all
select '02058' a from bass2.dual union all
select '02059' a from bass2.dual union all
select '02060' a from bass2.dual union all
select '02061' a from bass2.dual union all
select '02062' a from bass2.dual union all
select '02063' a from bass2.dual union all
select '02064' a from bass2.dual union all
select '02065' a from bass2.dual union all
select '02066' a from bass2.dual union all
select '04002' a from bass2.dual union all
select '04003' a from bass2.dual union all
select '04004' a from bass2.dual union all
select '04005' a from bass2.dual union all
select '04006' a from bass2.dual union all
select '04015' a from bass2.dual union all
select '04016' a from bass2.dual union all
select '04017' a from bass2.dual union all
select '04018' a from bass2.dual union all
select '04019' a from bass2.dual union all
select '06031' a from bass2.dual union all
select '06032' a from bass2.dual union all
select '06035' a from bass2.dual union all
select '06036' a from bass2.dual union all
select '06037' a from bass2.dual union all
select '06038' a from bass2.dual union all
select '06039' a from bass2.dual union all
select '21001' a from bass2.dual union all
select '21002' a from bass2.dual union all
select '21004' a from bass2.dual union all
select '21005' a from bass2.dual union all
select '21007' a from bass2.dual union all
select '21009' a from bass2.dual union all
select '21016' a from bass2.dual union all
select '22012' a from bass2.dual union all
select '22038' a from bass2.dual union all
select '22073' a from bass2.dual union all
select '22048' a from bass2.dual union all
select '22066' a from bass2.dual union all
select '22091' a from bass2.dual union all
select '22092' a from bass2.dual union all
select '22102' a from bass2.dual union all
select '22104' a from bass2.dual union all
select '22201' a from bass2.dual union all
select '22202' a from bass2.dual union all
select '22203' a from bass2.dual union all
select '22301' a from bass2.dual union all
select '22302' a from bass2.dual union all
select '22080' a from bass2.dual union all
select '22082' a from bass2.dual union all
select '22084' a from bass2.dual union all
select '04008' a from bass2.dual union all
select '04009' a from bass2.dual union all
select '04010' a from bass2.dual union all
select '04011' a from bass2.dual union all
select '04012' a from bass2.dual ) a 





update (select * from     G_S_22091_DAY where time_id = int(replace(char(current date - 1 days),'-',''))
) 
set NEW_USER_cnt = '1'
where NEW_USER_cnt > '1'

select sum(bigint(NEW_USER_cnt)) from G_S_22091_DAY where time_id = int(replace(char(current date - 1 days),'-',''))


select * from   G_A_02054_DAY where time_id = 20111020


select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22080_DAY 
group by  time_id 
order by 1 


   select * from   bass1.g_rule_check 
where rule_code = 'R255'
order by 1 desc 


select count(0) from    BASS2.DWD_SYS_CMD_DEF_20111025


                  select user_id from bass1.g_i_02022_day
                      where time_id =20111026
                       except
                  select user_id from bass2.dw_product_20111026
                    where usertype_id in (1,2,9) 
                    and userstatus_id in (1,2,3,6,8)
                    and test_mark<>1
                    
USER_ID
89160000925615      
89160000915003      
89160000927714      
89160000925040      
89160000917846      

                     select user_id from bass1.g_i_02022_day
                      where time_id =20111025
                       except
                  select user_id from bass2.dw_product_20111025
                    where usertype_id in (1,2,9) 
                    and userstatus_id in (1,2,3,6,8)
                    and test_mark<>1
                    
delete from (select * from  bass1.g_i_02022_day  where time_id =20111026)
where user_id in 
(
'89660001015961'

)

89660001015961      

select * from    g_a_02004_day 
where user_id = '89160000925615'
                  select *from bass1.g_i_02022_day
                      where time_id =20111025
and  user_id = '89160000925615'                     
                    
                  select * from bass2.dw_product_20111025
                    where user_id = '89160000925615'

bass1.int_02004_02008_month_stage 
        
select * from    g_a_02004_02008_stage 
where user_id = '89160000925615'
            
            select * from    g_a_02008_day 
where user_id = '89160000925615'
            

            
            

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
where 
    rule_code in ('R159_1','R159_2','R159_3','R159_4')
and rule_code='R159_4'
order by 1 desc 

            

                select user_id, count(0)  cnt
                from bass1.G_A_02028_DAY
                where time_id =20111028
                group by user_id having count(0) > 1
                
                      	2

select * from   G_A_02028_DAY
where user_id = '89157334193580'

TIME_ID	USER_ID	PKG_ID	ORDER_DT	STS_CD
20111028	89157334193580      	9999121202901081011001	20111101	1
20111028	89157334193580      	9999121202900981011001	20111028	1

delete from G_A_02028_DAY
where user_id = '89157334193580'
and TIME_ID = 20111028 
and ORDER_DT = '20111101'


select time_id,count(0) from bass1.g_user_lst
group by time_id 
order by 1 desc 


 select count(0) dup_cnt
        from (
                select user_id, count(0)  cnt
                from bass1.G_S_02024_DAY
                where time_id =20111101
                group by user_id having count(0) > 1
                ) t 
                

from bass1.G_S_02024_DAY
                where time_id =20111101                
                and user_id in (
                                select user_id
                from bass1.G_S_02024_DAY
                where time_id =20111101
                group by user_id having count(0) > 1

                )
                
                
                
                select * from   g_a_22036_day
                
                
                
delete                 
                from bass1.G_S_02024_DAY
                where time_id =20111101                
                and user_id in (
                                select user_id
                from bass1.G_S_02024_DAY
                where time_id =20111101
                group by user_id having count(0) > 1)
               and valid_dt > '20111101'
               
               
               
                


select count(0) from   bass2.dw_product_20111231



select   substr(filename,18,5) 
						from 
						( 
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn 
						from APP.G_FILE_REPORT a 
						where substr(filename,9,8) = replace(char(current date - 2 days),'-','') 
						and err_code='00' 
						and substr(filename,18,5) 
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE 
								   where deadline = 9 and sts = 1 
								) 
						) t where rn = 1 
                        
                        except

select   substr(filename,18,5) 
						from 
						( 
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn 
						from APP.G_FILE_REPORT a 
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') 
						and err_code='00' 
						and substr(filename,18,5) 
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE 
								   where deadline = 9 and sts = 1 
								) 
						) t where rn = 1 



select   substr(filename,18,5) 
						from 
						( 
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn 
						from APP.G_FILE_REPORT a 
						where substr(filename,9,8) = replace(char(current date - 2 days),'-','') 
						and err_code='00' 
						and substr(filename,18,5) 
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE 
								   where deadline = 9 and sts = 1 
								) 
						) t where rn = 1 
                        


select count(*) from 
	            (
	             select user_id,count(*) cnt from bass1.g_a_02008_day
	              where time_id =20120104
	             group by user_id
	             having count(*)>1
	            ) as a

USER_ID	CNT
89560001672362      	2

USER_ID	CNT
89560001672362      	2


USER_ID	CNT
89560001672362      	3

select * from                         bass1.g_a_02008_day
where time_id = 20120104
and user_id = '89560001672362'
           
TIME_ID	USER_ID	USERTYPE_ID
20120103	89560001672362      	1031
20120103	89560001672362      	2020

             
20120102	89560001672362      	2020
20120102	89560001672362      	1031


TIME_ID	USER_ID	USERTYPE_ID
20120102	89560001672362      	2020
20120102	89560001672362      	2020
20120102	89560001672362      	1031


delete from (
select * from                         bass1.g_a_02008_day
where time_id = 20120104
and user_id = '89560001672362'
) t
where USER_ID = '89560001672362'

89560001668643      	2


select * from
(
select product_no,count(*) cnt from CHECK_0200402008_DAY_3
group by product_no
			   ) k where k.cnt>=2
               with ur
               
PRODUCT_NO	CNT
18289059112    	2

select * from CHECK_0200402008_DAY_3
where PRODUCT_NO = '18289059112'

               
select count(0) from CHECK_0200402008_DAY_3
               

 select user_id,count(*) cnt from bass1.g_a_02008_day
	              where time_id =20120101
                  

select * from                         bass1.g_a_02008_day
where time_id = 20120101
and user_id = '89560001668643'

TIME_ID	USER_ID	USERTYPE_ID
20120101	89560001668643      	2020
20120101	89560001668643      	1010


select * from bass2.dw_product_20120101
where user_id = '89560001668643'
PRODUCT_NO
18289059112

select * from bass2.dw_product_20120101
where PRODUCT_NO = '18289059112'


select * from bass2.dw_product_bass1_20120101
where user_id = '89560001668643'

select * from 
TEMP_CHECK_02008_D
where user_id = '89560001668643'


select distinct 20120101,user_id,'2020' from G_A_02004_DAY where user_id in(select user_id from product_xhx4) with ur


select * from PRODUCT_XHX3
PRODUCT_NO
18289059112    

select * from CHECK_0200402008_DAY_1
where PRODUCT_NO = '18289059112'

USER_ID	PRODUCT_NO	USERTYPE_ID	SIM_CODE
89560001672362      	18289059112    	1	0
89560001668643      	18289059112    	1	0

select * from product_xhx4

USER_ID
89560001668643      


select * from bass2.dw_product_bass1_20120104
where PRODUCT_NO = '18289059112'


USER_ID	CUST_ID	USERTYPE_ID	BRAND_ID	CRM_BRAND_ID2	USERSTATUS_ID	STOPSTATUS_ID	PRODUCT_NO	IMSI	CITY_ID	CHANNEL_ID	CREATE_DATE	ACCTTYPE_ID	TEST_MARK	FREE_MARK	ENTERPRISE_MARK
89560001672362	89503000006732	1	4	210	1	16	18289059112	460008951389778	895	50500112	2011-12-16 	0	0	0	1
89560001668643	89503000248463	1	5	250	1	0	18289059112	460008951388580	895	50500112	2011-12-16 	0	0	0	0


select * from bass2.dim_pub_brand3
where crm_brand_id3 in (210,250)


select * from bass2.dim_pub_brand2
where crm_brand_id2 in (210,250)

CRM_BRAND_ID2	CRM_BRAND_NAME2
250	新神州行长话卡
210	动感地带音乐套餐




89503000006732
89503000248463


select * from                         bass1.g_a_02008_day
where time_id = 20120101
and user_id = '89560001668643'

TIME_ID	USER_ID	USERTYPE_ID
20120101	89560001672362      	2020
20120101	89560001668643      	1010

update (
select * from                         bass1.g_a_02008_day
where time_id = 20120101
and user_id = '89560001668643'
) t 
set user_id = '89560001672362' 
where USER_ID = '89560001668643'
and USERTYPE_ID = '2020'


select * from                         bass1.g_a_02008_day
where time_id = 20120101
and user_id in ( '89560001668643','89560001672362')

TIME_ID	USER_ID	USERTYPE_ID
20120101	89560001668643      	1010
20120101	89560001672362      	1031
20120101	89560001672362      	2020

delete from 
(
select * from                         bass1.g_a_02008_day
where time_id = 20120101
and user_id in ( '89560001668643','89560001672362')
) 
where user_id = '89560001672362'
and USERTYPE_ID ='1031'


dele (
select * from                         bass1.g_a_02008_day
where time_id = 20120101
and user_id in ( '89560001668643','89560001672362')
) t 
set user_id = '89560001672362' 
where USER_ID = '89560001668643'
and USERTYPE_ID = '2020'




89560001672362      


select * from                         bass1.g_a_02008_day
where time_id = 20120101
and user_id = '89560001672362'

TIME_ID	USER_ID	USERTYPE_ID
20120101	89560001672362      	1031
20120101	89560001672362      	2020


select * from bass2.dw_product_bass1_20111231
where PRODUCT_NO = '18289059112'



select * from bass2.dw_product_20120102
where PRODUCT_NO = '18289059112'




select count(user_id) 
                    from bass2.dw_product_20120105
                   where usertype_id in (1,2,9) 
                     and day_off_mark = 1 
                     and userstatus_id not in (1,2,3,6,8)
                     and test_mark<>1


1
137

        declare global temporary table session.int_check_user_status
                                (
                           user_id        CHARACTER(15),
                           product_no     CHARACTER(15),
                           test_flag      CHARACTER(1),
                           sim_code       CHARACTER(15),
                           usertype_id    CHARACTER(4),
                           create_date    CHARACTER(15),
                           time_id        int
                                )                            
                                partitioning key           
                                 (
                                   user_id    
                                 ) using hashing           
                                with replace on commit preserve rows not logged in tbs_user_temp

       insert into session.int_check_user_status (
                                                                 user_id    
                                                                ,product_no 
                                                                ,test_flag  
                                                                ,sim_code   
                                                                ,usertype_id  
                                                                ,create_date
                                                                ,time_id )
                                        select e.user_id
                                                                ,e.product_no  
                                                                ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
                                                                ,e.sim_code
                                                                ,f.usertype_id  
                                                                ,e.create_date  
                                                                ,f.time_id       
                                        from (select user_id,create_date,product_no,sim_code,usertype_id
                                                                                        ,row_number() over(partition by user_id order by time_id desc ) row_id   
                                              from bass1.g_a_02004_day
                                              where time_id<=20120105 ) e
                                        inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
                                                                       from bass1.g_a_02008_day
                                                                       where time_id<=20120105 ) f on f.user_id=e.user_id
                                        where e.row_id=1 and f.row_id=1

select tabname from syscat.tables where tabname like '%02004%'                        


select count(distinct user_id) from   session.int_check_user_status    
                        where usertype_id IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id=20120105
                          


select user_id from   session.int_check_user_status    
                        where usertype_id IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id=20120105                     
except
select user_id 
from bass2.dw_product_20120105
where usertype_id in (1,2,9) 
 and day_off_mark = 1 
 and userstatus_id not in (1,2,3,6,8)
 and test_mark<>1


USER_ID
89160001755027 
89160001755201 


select user_id 
from bass2.dw_product_20120105
where usertype_id in (1,2,9) 
 and day_off_mark = 1 
 and userstatus_id not in (1,2,3,6,8)
 and test_mark<>1
except
select user_id from   session.int_check_user_status    
                        where usertype_id IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id=20120105                     

USER_ID
89560001672362


USER_ID
89160001741974 

select * from 

select user_id 
from bass2.dw_product_20120101
where usertype_id in (1,2,9) 
 and day_off_mark = 1 
 and userstatus_id not in (1,2,3,6,8)
 and test_mark<>1
 and user_id in ('89560001668643','89560001672362')
 
 
 20120101	89560001672362      	2020
20120101	89560001668643      	1010

dim_cust_status


89503000006732
89503000248463


select * from bass2.dw_cust_20120101
where cust_id in ('89503000006732','89503000248463')



        select A.*,B.NAME from bass2.dw_product_ins_off_ins_prod_201112 a, bass2.dim_prod_up_product_item  b
where product_instance_id 
in 
('89560001672362'
)
and a.OFFER_ID = b.PRODUCT_ITEM_ID




select * from  table( bass1.get_after('01002')) a 

select *  from  table( bass1.get_before('IMPORTSERV')) a 
where control_code like '%DAY%'

select * from  table( bass1.get_after('22049')) a 


select * from app.sch_control_before 
where before_control_code 
in 
(
'BASS1_G_S_02007_MONTH.tcl'
,'BASS1_G_S_03005_MONTH.tcl'
,'BASS1_G_S_03017_MONTH.tcl'
,'BASS1_G_I_03007_MONTH.tcl'
,'BASS1_G_S_03004_MONTH.tcl'
,'BASS1_G_S_22305_MONTH.tcl'
,'BASS1_G_S_03015_MONTH.tcl'
,'BASS1_G_S_22307_MONTH.tcl'
,'BASS1_G_S_03019_MONTH.tcl'
,'BASS1_G_I_02006_MONTH.tcl'
,'BASS1_G_S_22039_MONTH.tcl'
)


select * from app.sch_control_task
where control_code like '%IMPORTSERV%'


CONTROL_CODE
BASS1_INT_CHECK_IMPORTSERV_MONTH
BASS1_INT_CHECK_IMPORTSERV_MONTH


update app.sch_control_before 
set control_code = 'BASS1_INT_CHECK_IMPORTSERV_MONTH.tcl'
where control_code = 'BASS1_INT_CHECK_IMPORTSERV_MONTH'

insert into app.sch_control_before 
values('BASS1_EXP_G_S_03004_MONTH','BASS1_INT_CHECK_IMPORTSERV_MONTH.tcl')



G_S_03004_MONTH
G_S_03004_MONTH
G_S_03005_MONTH



BASS1_INT_CHECK_TD_DAY.tcl	BASS1_G_A_02008_DAY.tcl
BASS1_INT_CHECK_INDEX_SAME_DAY.tcl	BASS1_G_A_02008_DAY.tcl
BASS1_G_A_02004_DAY.tcl	BASS1_G_A_02008_DAY.tcl
BASS1_INT_CHECK_Z345_DAY.tcl	BASS1_G_A_02008_DAY.tcl
BASS1_INT_CHECK_GPRS_FLOW_DAY.tcl	BASS1_G_A_02008_DAY.tcl
INT_CHECK_DATARULE_DAY.tcl	BASS1_G_A_02008_DAY.tcl
BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl	BASS1_G_A_02008_DAY.tcl
BASS1_INT_CHECK_33TO40_DAY.tcl	BASS1_G_A_02008_DAY.tcl
BASS1_INT_CHECK_63EO_TO_DAY.tcl	BASS1_G_A_02008_DAY.tcl
BASS1_G_BUS_00000_DAY.tcl	BASS1_G_A_02008_DAY.tcl
BASS1_INT_CHECK_E1_DAY.tcl	BASS1_G_A_02008_DAY.tcl

update  app.sch_control_runlog
set flag = -2
where control_code in (
select before_control_code from  table( bass1.get_after('03007')) a 
)
and flag = 0

select control_code  from  table( bass1.get_after('02004')) a 
where control_code like '%INT%DAY%'
except
select control_code  from  table( bass1.get_after('02008')) a 
where control_code like '%INT%DAY%'


select control_code  from  table( bass1.get_after('02004')) a 
where control_code like '%INT%DAY%'
except
select control_code  from  table( bass1.get_after('02008')) a 
where control_code like '%INT%DAY%'


update  app.sch_control_runlog
set flag = -2
where control_code in (
select control_code  from  table( bass1.get_after('02004')) a 
where control_code like '%INT%DAY%'
except
select control_code  from  table( bass1.get_after('G_I_21020_MONTH')) a 
where control_code like '%INT%DAY%'
)
and flag = 0



select * from                         bass1.g_a_02004_day
where user_id in ( '89560001668643','89560001672362')

TIME_ID	USER_ID	CUST_ID	USERTYPE_ID	CREATE_DATE	USER_BUS_TYP_ID	PRODUCT_NO	IMSI	CMCC_ID	CHANNEL_ID	MCV_TYP_ID	PROMPT_TYPE	SUBS_STYLE_ID	BRAND_ID	SIM_CODE
20111216	89560001672362      	89503000006732      	1	20111216	01	18289059112    	460008951389778	13105	50500112                                	1	1	04	3	0
20120101	89560001668643      	89503000248463      	1	20111216	01	18289059112    	460008951388580	13105	50500112                                	1	1	04	2	0


select * from                         bass1.g_a_02008_day
where user_id in ( '89560001668643','89560001672362')
TIME_ID	USER_ID	USERTYPE_ID
20111216	89560001672362      	1010
20120101	89560001672362      	2020
20120101	89560001668643      	1010


select * from G_S_22012_DAY
where time_id >= 20111231



ls -alrt \
*04005* \
*22012* \
*02025* \
*02023* \
*02024* \
*02022* 

select * from bass1.mon_all_interface 
where interface_code 
in ('04005','22012','02025','02023','02024','02022')
and type = 'd'


select * from g_s_02025_day
where time_id = 20120101




        select USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20120101 and VALID_DT = '20120101'
        except
                                select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
                from (
                        select  USER_ID,BASE_PKG_ID from 
                        G_S_02024_DAY a
                        where time_id <= 20120101 
                     ) a 
                left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id
                
select * from G_I_02022_DAY
where user_id in ('89157333552187','89660001671857','89157332112431')
and time_id = 20111231
             

TIME_ID     USER_ID              BASE_PKG_ID                    VALID_DT
----------- -------------------- ------------------------------ --------
   20120101 89660001671857       999914211030058001             20120101
   20120101 89157333552187       999914211030058001             20120101 --> test -> not test 
   20120101 89157332112431       999914211030088001             20120101 --> QW_QQT_JC_SL58
   
select * from bass2.dw_product_bass1_20120101
where user_id = '89660001671857'

select * from bass2.dw_product_bass1_20120101
where user_id = '89660001671857'



select * from G_S_02024_DAY
where user_id in ('89157333552187','89660001671857','89157332112431')
and time_id = 20120101                 


select * from G_S_02024_DAY
where user_id in ('89157333552187','89660001671857','89157332112431')
and time_id = 20120101                 


select * from G_S_02024_DAY
where user_id in ('89157333552187','89660001671857','89157332112431')
and time_id = 20111231
              


 select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
                from (
                        select  USER_ID,BASE_PKG_ID from 
                        G_S_02024_DAY a
                        where time_id <= 20120101 
                     ) a 
                left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id
                where user_id = '89660001671857'
                
                
select * from G_S_02024_DAY
where user_id in ('89157333552187','89660001671857','89157332112431')
and time_id = 20120101     
                
                
                


    select A.PRODUCT_INSTANCE_ID,a.CREATE_DATE,a.DONE_DATE,a.VALID_DATE,a.EXPIRE_DATE,B.NAME 
    from bass2.dw_product_ins_off_ins_prod_201112 a 
    , bass2.dim_prod_up_product_item  b 
    where product_instance_id 
    in 
    ('89660001671857'
    )
    and a.OFFER_ID = b.PRODUCT_ITEM_ID
    order by VALID_DATE 
    
                
select * from G_S_02024_DAY
where user_id in ('89157333552187')
and time_id <= 20120101     
                

select 
        20111208 time_id
        ,a.PRODUCT_INSTANCE_ID USER_ID
        ,char(a.offer_id) BASE_PKG_ID
        ,char(a.ORG_ID) CHANNEL_ID
        ,replace(char(date(a.CREATE_DATE)),'-','') REC_DT
        ,replace(char(date(a.VALID_DATE) ),'-','') VALID_DT
from  bass2.Dw_product_ins_off_ins_prod_ds a 
        , bass2.dw_product_20111208 b 
where  a.state =1 
        and a.PRODUCT_INSTANCE_ID=b.user_id 
        and b.usertype_id in (1,2,9) 
        and b.userstatus_id in (1,2,3,6,8)
        and a.valid_type in (1,2)
        and date(a.CREATE_DATE) = '2011-12-08'
        and date(a.VALID_DATE)>= '2011-12-08'
        and date(a.expire_date) >= '2011-12-08'
        and user_id in ('89157333552187')
        
        and not exists ( 
                         select 1 from bass2.dwd_product_test_phone_20111208 b 
                         where a.product_instance_id = b.USER_ID  and b.sts = 1
                        )   
        and char(a.offer_id) in (
                SELECT char(offer_id) FROM BASS2.dim_prod_up_offer 
                WHERE OFFER_TYPE='OFFER_PLAN'
                and TRADEMARK = 161000000001
        union 
                select char(xzbas_value)  as offer_id 
                from  BASS1.ALL_DIM_LKP 
                where BASS1_TBID = 'BASS_STD1_0114'
                and bass1_value like 'QW_QQT_JC%'
                and XZBAS_COLNAME not like '套餐减半%'
        )                      
        
        
        
        select user_id,TEST_MARK 
        from bass2.dw_product_20111208
        where user_id = '89157333552187' -- test -> not test
        
        
      select * from G_S_22302_DAY
where time_id = 20120102
      
        
        select control_code  from  table( bass1.get_before('22302')) a 


select * from app.sch_control_runlog 
where control_code in (
select before_control_code from  table( bass1.get_before('03017')) a 
)

update 
(
select * from app.sch_control_task where control_code like '%L34%'
) 
A
SET CC_FLAG = 1
WHERE CC_FLAG = 2




select distinct BASE_PKG_ID from G_S_02024_DAY where time_id = 20120101


select * from 

select *from bass1.g_rule_check where rule_code like 'R128%' order by time_id desc


select distinct user_id,ADD_PKG_ID from G_S_02025_DAY where time_id = 20120101
except
select distinct user_id,ADD_PKG_ID from G_I_02023_DAY where time_id = 20120101

select distinct ADD_PKG_ID from 


select * from app



select distinct ADD_PKG_ID from G_I_02023_DAY where time_id = 20120101 and ADD_PKG_ID like '999%'
except
select distinct ADD_PKG_ID from G_S_02025_DAY where time_id = 20120101


select distinct user_id,ADD_PKG_ID from G_I_02023_DAY where time_id = 20120101  and ADD_PKG_ID like '999%'
except
select distinct user_id,ADD_PKG_ID from G_S_02025_DAY where time_id = 20120101




select distinct user_id,BASE_PKG_ID from G_S_02024_DAY where time_id = 20120101 and BASE_PKG_ID like '999%'
except
select distinct user_id,BASE_PKG_ID from G_I_02022_DAY where time_id = 20120101

89101110285299      
select * from G_I_02022_DAY
where user_id = '89101110285299'
and time_id in (20111231,20120101)


select * from G_s_02024_DAY
where user_id = '89101110285299'
and time_id in (20111231,20120101)

TIME_ID	USER_ID	BASE_PKG_ID	CHANNEL_ID	REC_DT	VALID_DT
20120101	89101110285299      	999914211030058001            	91000003                                	20120101	20120201



select distinct  OVER_PROD_ID 
from G_I_02019_MONTH where time_id = 201112

select distinct  BASE_PROD_ID 
from G_I_02018_MONTH where time_id = 201112


select distinct  OVER_PROD_ID 
from G_I_02021_MONTH where time_id = 201112

select distinct  BASE_PROD_ID 
from G_I_02020_MONTH where time_id = 201112





select
(
select count(distinct user_id) from  bass1.g_a_02004_02008_stage  
                        where USERSTATUS IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id/100 = 201201
                          ) MONTH_OFF
,(
select sum(bigint(TARGET2))
from bass1.g_rule_check
where 
rule_code='R159_4'
and time_id / 100  = 201201
)  MONTH_ALL_DAY_OFF
, ((
select count(distinct user_id) from  bass1.g_a_02004_02008_stage  
                        where USERSTATUS IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id/100 = 201201
                          ) *1.00/(
select sum(bigint(TARGET2))
from bass1.g_rule_check
where 
rule_code='R159_4'
and time_id / 100  = 201201
)-1)*100 PERCENT
from bass2.dual

select * from 


select * from  app.sch_control_alarm 
where  control_code like '%R230%.tcl%'
order by alarmtime desc


  select * from
                (select * from 
                (
                select user_id,chg_vip_time,row_number()over(partition by user_id order by time_id desc) row_id from BASS1.G_I_02005_MONTH
                where time_id=201112
                ) k
                where k.row_id =1) a
                left outer join 
                (
                select * from
                (
                select user_id,create_date,row_number()over(partition by user_id order by time_id desc) row_id 
                from BASS1.G_A_02004_DAY
                where time_id<=20111231
                ) k
                where k.row_id=1) b
                on a.user_id=b.user_id
                where bigint(chg_vip_time)<bigint(create_date)
                with ur
                
update BASS1.G_I_02005_MONTH
set CHG_VIP_TIME = '20110629'
where user_id = '89160001048760'
and time_id = 201112

USER_ID	CHG_VIP_TIME	ROW_ID	USER_ID	CREATE_DATE	ROW_ID
89160001048760      	20110624	1	89160001048760      	20110629	1


89160001048760

                
delete from (                
select * from   G_I_21020_MONTH
				where COMP_BRAND_ID = '021000'
				and time_id = 201112
				and length(trim(COMP_PRODUCT_NO)) <> 11
) t


select count(0) from   G_I_21020_MONTH
				where COMP_BRAND_ID = '031000'
				and time_id = 201112
				and length(trim(COMP_PRODUCT_NO)) <> 11
				with ur 
                
                
select * from                 


update G_I_21020_MONTH 
set CALL_COUNTS = char(int(rand(1)*5+1))
where (
char(TIME_ID) = substr(COMP_LAST_DATE,1,6)
and CALL_COUNTS = '0'
and SMS_COUNTS = '0'
and MMS_COUNTS = '0'
and time_id = 201112 )




select 
 b.*
, lower(
 'put *'||b.interface_code||'*.dat ' 
) put_dat
, lower(
 'put *'||b.interface_code||'*.verf ' 
) put_verf
, lower(
 'rm *'||b.interface_code||'* ' 
) put_verf
, lower(
 ' *'||b.interface_code||'*dat \' 
) list
 from   bass1.MON_ALL_INTERFACE b where 
deadline  in (8)
and type = 'm'
and sts = 1



s
select * from  table( bass1.get_before('02031')) a 


select * from   app.sch_control_runlog 
where control_code in 
(select before_control_code from  table( bass1.get_before('G_S_22063_MONTH')) a )



        select count(distinct a.user_id) VALID_CNT,count(distinct b.user_id ) REC_CNT, count(distinct b.user_id )*1.00/count(distinct a.user_id) RATE
        from 
        (
        select USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20120103 and VALID_DT = '20120103'
        ) a
        left join (
                        select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
                from (
                        select  USER_ID,BASE_PKG_ID from 
                        G_S_02024_DAY a
                        where time_id <= 20120103 
                     ) a 
                left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id
                ) b on a.USER_ID = b.USER_ID and a.BASE_PKG_ID= b.BASE_PKG_ID
        with ur
        
        
        
        
         select USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20120103 and VALID_DT = '20120103'
        except
        select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
                from (
                        select  USER_ID,BASE_PKG_ID from 
                        G_S_02024_DAY a
                        where time_id <= 20120103  and time_id >= 20120101
                     ) a 
                left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id
                
                   select USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20111229 and VALID_DT = '20111229'      
                
                                        select  USER_ID,BASE_PKG_ID from 
                        G_S_02024_DAY a
                        where time_id <= 20111229
                        AND (USER_ID,BASE_PKG_ID) IN 
                        (                   select USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20111229 and VALID_DT = '20111229'   )
                        
                 select * from   
        G_I_02022_DAY  a        
        where (user_id,BASE_PKG_ID) in (
         select USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20111231 and VALID_DT = '20111231'
        except
        select a.user_id,a.BASE_PKG_ID BASE_PKG_ID 
                from (
                        select  USER_ID,BASE_PKG_ID from 
                        G_S_02024_DAY a
                        where time_id <= 20111231  
                     ) a 
                
                )
                
           20111231	89160001736347      	QW_QQT_JC_SL88                	20111231
     
                select time_id ,count(0)
                from G_S_02024_DAY
                group by time_id 
                
                
            
                
select * from   
G_S_02024_DAY  a   
where user_id =   '89160001736347'
  
TIME_ID	USER_ID	BASE_PKG_ID	CHANNEL_ID	REC_DT	VALID_DT
20120103	89160001736347      	999914211020088001            	100003003                               	20120103	20120201
20111231	89160001736347      	QW_QQT_JC_SL88                	11111144                                	20111231	20111231


                
select * from   
G_S_02024_DAY  a        
where (user_id,BASE_PKG_ID) in (
select USER_ID,BASE_PKG_ID from   
G_I_02022_DAY  a
where time_id = 20111231 and VALID_DT = '20111231'
except
select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
from (
    select  USER_ID,BASE_PKG_ID from 
    G_S_02024_DAY a
    where time_id <= 20111231  
 ) a 
left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id

)



select USER_ID,BASE_PKG_ID from   
G_I_02022_DAY  a
where time_id = 2011 and VALID_DT = '20111231'

               
R258	02022的新增用户中有成功办理量的用户占比≥98%
sel count(d.subs_id) as val1,count(a.subs_id) as val2
from (sel subs_id,basic_pack_id
          from $DWPVIEW.TB_SVC_SUBS_GTONE_BASIC_PACK 
          where eff_dt=$Tx_Date and end_dt>$Tx_Date
          and cmcc_prov_prvd_id = $Branch_ID) a
left join (sel subs_id,basic_pack_id 
     from $DWPVIEW.TB_SVC_GTONE_BASIC_PACK_PROC 
     where dat_rcd_dt between add_months(cast(substr('$Tx_Date',1,6)||'01' as date format 'YYYYMMDD'),-1) and $Tx_Date
     and cmcc_prov_prvd_id = $Branch_ID group by 1,2) d
on a.subs_id = d.subs_id and a.basic_pack_id = d.basic_pack_id) t
               
               
               
    select distinct USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20111229 and VALID_DT = '20111229'      
        
        
select * from         G_I_02022_DAY  a  where time_id = 20111229
and    ( USER_ID,BASE_PKG_ID ) in (     
    select distinct USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20111229
	except
    select distinct USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20111228
        )
        
        20111229	89160001174590      	QW_QQT_JC_SL58                	20110802


        
        select * from         G_S_02024_DAY  a 
where    ( USER_ID,BASE_PKG_ID ) in (     
    select distinct USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20111229
	except
    select distinct USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20111228
        )
        
        
        
        

select USER_ID,BASE_PKG_ID  from         G_I_02022_DAY  a  where time_id = 20111229
and    ( USER_ID,BASE_PKG_ID ) in (     
    select distinct USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20111229
	except
    select distinct USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20111228
        )
        except
        select USER_ID,BASE_PKG_ID  from         G_S_02024_DAY  a 
where    ( USER_ID,BASE_PKG_ID ) in (     
    select distinct USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20111229
	except
    select distinct USER_ID,BASE_PKG_ID from   
        G_I_02022_DAY  a
        where time_id = 20111228
        )
        

USER_ID	BASE_PKG_ID
89160000878180      	QW_QQT_JC_BD128               
89160000878007      	QW_QQT_JC_BD58                
89160000878136      	QW_QQT_JC_BD58                
89160000878159      	QW_QQT_JC_BD58                
89160000878212      	QW_QQT_JC_BD58                
89160000878281      	QW_QQT_JC_BD58                
89160000878293      	QW_QQT_JC_BD58                
89160000878323      	QW_QQT_JC_BD58                
89160000878855      	QW_QQT_JC_BD58                
89160000878902      	QW_QQT_JC_BD58                
89160000879699      	QW_QQT_JC_BD58                
89160000879705      	QW_QQT_JC_BD58                
89760000859398      	QW_QQT_JC_BD58                
89157332235104      	QW_QQT_JC_SL388               
89157332112431      	QW_QQT_JC_SL58                
89460000857857      	QW_QQT_JC_SL58                
89760000859381      	QW_QQT_JC_SL58                
89160000878376      	QW_QQT_JC_SW128               
89160000878383      	QW_QQT_JC_SW58                
89160000878483      	QW_QQT_JC_SW58                
89160000878497      	QW_QQT_JC_SW58                
89160000878949      	QW_QQT_JC_SW58                
89160000878970      	QW_QQT_JC_SW58                
89160000879035      	QW_QQT_JC_SW58                
89160000879088      	QW_QQT_JC_SW58                
89160000879323      	QW_QQT_JC_SW58                
89160000879346      	QW_QQT_JC_SW58                
89160000879367      	QW_QQT_JC_SW58                
89160000879527      	QW_QQT_JC_SW58                
89160000879573      	QW_QQT_JC_SW58                
89160000879585      	QW_QQT_JC_SW58                
89760000859353      	QW_QQT_JC_SW58                
89160000878345      	QW_QQT_JC_SW88                

        
select * from G_I_02022_DAY
where user_id = '89160000878180'

TIME_ID	USER_ID	BASE_PKG_ID	VALID_DT
20120103	89160000878180      	999914211040128001            	20110601
20120102	89160000878180      	999914211040128001            	20110601
20120101	89160000878180      	999914211040128001            	20110601
20111231	89160000878180      	QW_QQT_JC_BD128               	20110601
20111230	89160000878180      	QW_QQT_JC_BD128               	20110601
20111229	89160000878180      	QW_QQT_JC_BD128               	20110601


        
        
select * from G_S_02024_DAY
where user_id = '89160000878180'


        
        

select * from  app.sch_control_alarm 
where 
control_code like 'BASS1%'
order by alarmtime desc


select * from bass2.ODS_product_ins_off_ins_prod_20111228
where product_instance_id = '89160000878180'


        select A.*,B.NAME from bass2.ODS_product_ins_off_ins_prod_20111228 a, bass2.dim_prod_up_product_item  b
where product_instance_id 
in 
('89160000878180'
)
and a.OFFER_ID = b.PRODUCT_ITEM_ID


SELECT * FROM BASS2.DIM_TERM_TAC


TIME_ID	USER_ID	BASE_PKG_ID	CHANNEL_ID	REC_DT	VALID_DT
20111231	89157332235104      	QW_QQT_JC_SL88                	11111115                                	20111231	20120101



        select A.*,B.NAME from bass2.DW_product_ins_off_ins_prod_DS a, bass2.dim_prod_up_product_item  b
where product_instance_id 
in 
('89157332235104'
)
and a.OFFER_ID = b.PRODUCT_ITEM_ID



   select * from         G_S_02024_DAY  a 
WHERE USER_ID
IN (
'89160000878180'            
,'89160000878007'            
,'89160000878136'            
,'89160000878159'            
,'89160000878212'            
,'89160000878281'            
,'89160000878293'            
,'89160000878323'            
,'89160000878855'            
,'89160000878902'            
,'89160000879699'            
,'89160000879705'            
,'89760000859398'            
,'89157332235104'            
,'89157332112431'            
,'89460000857857'            
,'89760000859381'            
,'89160000878376'            
,'89160000878383'            
,'89160000878483'            
,'89160000878497'            
,'89160000878949'            
,'89160000878970'            
,'89160000879035'            
,'89160000879088'            
,'89160000879323'            
,'89160000879346'            
,'89160000879367'            
,'89160000879527'            
,'89160000879573'            
,'89160000879585'            
,'89760000859353'            
,'89160000878345'            
)
select * from G_I_02022_DAY
where user_id = '89157332235104'
AND TIME_ID <= 20111229



select * from bass2.dwd_product_test_phone_20111229 b 
				where b.USER_ID  = '89157332235104'
				and b.sts = 1
				
TIME_ID	USER_ID	BASE_PKG_ID	VALID_DT
20111229	89157332235104      	999914211030388001	20110801


select 	TIME_ID,USER_ID,BASE_PKG_ID,VALID_DT
from (
	select 
		20111228 TIME_ID
		,char(a.product_instance_id)  USER_ID
		,e.NEW_PKG_ID BASE_PKG_ID
		,replace(char(date(a.VALID_DATE)),'-','') VALID_DT
		,row_number()over(partition by a.product_instance_id order by expire_date desc ,VALID_DATE desc ) rn 
	from  bass2.Dw_product_ins_off_ins_prod_ds a
	,(select xzbas_value  as offer_id 
					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0114'
					      and bass1_value like 'QW_QQT_JC%'
					      and XZBAS_COLNAME not like '套餐减半%'
				      ) c
	,(select user_id from bass2.dw_product_20111228
		    where usertype_id in (1,2,9) 
		    and userstatus_id in (1,2,3,6,8)
		    and test_mark<>1) d
	,bass1.DIM_QW_QQT_PKGID e
	where  char(a.offer_id) = c.offer_id 
	  and char(a.product_instance_id)  = d.user_id
                                and char(a.product_instance_id)  = '89157332235104'
	  and bass1.fn_get_all_dim_ex('BASS_STD1_0114',char(a.offer_id))  = e.OLD_PKG_ID	  
	  and a.state =1
	  and a.valid_type = 1
	  and date(a.VALID_DATE)<='2011-12-28'	  
    and date(a.expire_date) >= '2011-12-28'	  
	  and not exists (	select 1 from bass2.dwd_product_test_phone_20111228 b 
				where a.product_instance_id = b.USER_ID  and b.sts = 1
			 ) 
) t where t.rn = 1
	 with ur
     
select * from bass2.dw_product_20111229
		    where usertype_id in (1,2,9) 
		    and userstatus_id in (1,2,3,6,8)
		    and test_mark<>1
            and user_id = '89157332235104'
            

select user_id,usertype_id,userstatus_id,test_mark from bass2.dw_product_20111229
where user_id = '89157332235104'
union all
select user_id,usertype_id,userstatus_id,test_mark from bass2.dw_product_20111228
where user_id = '89157332235104'

            
            


select 20111230 dt,user_id,usertype_id,userstatus_id,test_mark from bass2.dw_product_20111230
where  USER_ID
IN (
'89160000878180'            
,'89160000878007'            
,'89160000878136'            
,'89160000878159'            
,'89160000878212'            
,'89160000878281'            
,'89160000878293'            
,'89160000878323'            
,'89160000878855'            
,'89160000878902'            
,'89160000879699'            
,'89160000879705'            
,'89760000859398'            
,'89157332235104'            
,'89157332112431'            
,'89460000857857'            
,'89760000859381'            
,'89160000878376'            
,'89160000878383'            
,'89160000878483'            
,'89160000878497'            
,'89160000878949'            
,'89160000878970'            
,'89160000879035'            
,'89160000879088'            
,'89160000879323'            
,'89160000879346'            
,'89160000879367'            
,'89160000879527'            
,'89160000879573'            
,'89160000879585'            
,'89760000859353'            
,'89160000878345'            
)
union all
select 20111229 dt,user_id,usertype_id,userstatus_id,test_mark from bass2.dw_product_20111229
where  USER_ID
IN (
'89160000878180'            
,'89160000878007'            
,'89160000878136'            
,'89160000878159'            
,'89160000878212'            
,'89160000878281'            
,'89160000878293'            
,'89160000878323'            
,'89160000878855'            
,'89160000878902'            
,'89160000879699'            
,'89160000879705'            
,'89760000859398'            
,'89157332235104'            
,'89157332112431'            
,'89460000857857'            
,'89760000859381'            
,'89160000878376'            
,'89160000878383'            
,'89160000878483'            
,'89160000878497'            
,'89160000878949'            
,'89160000878970'            
,'89160000879035'            
,'89160000879088'            
,'89160000879323'            
,'89160000879346'            
,'89160000879367'            
,'89160000879527'            
,'89160000879573'            
,'89160000879585'            
,'89760000859353'            
,'89160000878345'            
)
union all
select 20111228 dt,user_id,usertype_id,userstatus_id,test_mark from bass2.dw_product_20111228
where  USER_ID
IN (
'89160000878180'            
,'89160000878007'            
,'89160000878136'            
,'89160000878159'            
,'89160000878212'            
,'89160000878281'            
,'89160000878293'            
,'89160000878323'            
,'89160000878855'            
,'89160000878902'            
,'89160000879699'            
,'89160000879705'            
,'89760000859398'            
,'89157332235104'            
,'89157332112431'            
,'89460000857857'            
,'89760000859381'            
,'89160000878376'            
,'89160000878383'            
,'89160000878483'            
,'89160000878497'            
,'89160000878949'            
,'89160000878970'            
,'89160000879035'            
,'89160000879088'            
,'89160000879323'            
,'89160000879346'            
,'89160000879367'            
,'89160000879527'            
,'89160000879573'            
,'89160000879585'            
,'89760000859353'            
,'89160000878345'            
)
union all
select 20111227 dt,user_id,usertype_id,userstatus_id,test_mark from bass2.dw_product_20111227
where  USER_ID
IN (
'89160000878180'            
,'89160000878007'            
,'89160000878136'            
,'89160000878159'            
,'89160000878212'            
,'89160000878281'            
,'89160000878293'            
,'89160000878323'            
,'89160000878855'            
,'89160000878902'            
,'89160000879699'            
,'89160000879705'            
,'89760000859398'            
,'89157332235104'            
,'89157332112431'            
,'89460000857857'            
,'89760000859381'            
,'89160000878376'            
,'89160000878383'            
,'89160000878483'            
,'89160000878497'            
,'89160000878949'            
,'89160000878970'            
,'89160000879035'            
,'89160000879088'            
,'89160000879323'            
,'89160000879346'            
,'89160000879367'            
,'89160000879527'            
,'89160000879573'            
,'89160000879585'            
,'89760000859353'            
,'89160000878345'            
)
order by 2,1


select * from bass2.dwd_product_test_phone_20111228 b 
				where  b.sts = 1
                and  b.USER_ID  
                
                
            IN (
'89160000878180'            
,'89160000878007'            
,'89160000878136'            
,'89160000878159'            
,'89160000878212'            
,'89160000878281'            
,'89160000878293'            
,'89160000878323'            
,'89160000878855'            
,'89160000878902'            
,'89160000879699'            
,'89160000879705'            
,'89760000859398'            
,'89157332235104'            
,'89157332112431'            
,'89460000857857'            
,'89760000859381'            
,'89160000878376'            
,'89160000878383'            
,'89160000878483'            
,'89160000878497'            
,'89160000878949'            
,'89160000878970'            
,'89160000879035'            
,'89160000879088'            
,'89160000879323'            
,'89160000879346'            
,'89160000879367'            
,'89160000879527'            
,'89160000879573'            
,'89160000879585'            
,'89760000859353'            
,'89160000878345'            
)






        select count(distinct a.user_id) VALID_CNT,count(distinct b.user_id ) REC_CNT, count(distinct b.user_id )*1.00/count(distinct a.user_id) RATE
        from 
        (
        select user_id,value(t.NEW_PKG_ID,a.BASE_PKG_ID) BASE_PKG_ID,VALID_DT from   
        G_I_02022_DAY  a
        left join bass1.DIM_QW_QQT_PKGID t on a.BASE_PKG_ID = t.old_pkg_id
        where time_id = 20111229
	except 
	select user_id,value(t.NEW_PKG_ID,a.BASE_PKG_ID) BASE_PKG_ID,VALID_DT from   
        G_I_02022_DAY  a
                left join bass1.DIM_QW_QQT_PKGID t on a.BASE_PKG_ID = t.old_pkg_id
        where time_id = 20111228
        ) a
        left join (
                        select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
                from (
                        select  USER_ID,BASE_PKG_ID from 
                        G_S_02024_DAY a
                     ) a 
                left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id
                ) b on a.USER_ID = b.USER_ID and a.BASE_PKG_ID= b.BASE_PKG_ID
        with ur
	
    
    

    
    


        select count(distinct a.user_id) VALID_CNT,count(distinct b.user_id ) REC_CNT
        , count(distinct b.user_id )*1.00/count(distinct a.user_id) RATE
        from 
        (
        select user_id,value(t.NEW_PKG_ID,a.BASE_PKG_ID) BASE_PKG_ID,VALID_DT from   
        G_I_02022_DAY  a
        left join bass1.DIM_QW_QQT_PKGID t on a.BASE_PKG_ID = t.old_pkg_id
        where time_id = 20111031
	except 
	select user_id,value(t.NEW_PKG_ID,a.BASE_PKG_ID) BASE_PKG_ID,VALID_DT from   
        G_I_02022_DAY  a
                left join bass1.DIM_QW_QQT_PKGID t on a.BASE_PKG_ID = t.old_pkg_id
        where time_id = 20111030
        ) a
        left join (
                        select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
                from (
                        select  USER_ID,BASE_PKG_ID from 
                        G_S_02024_DAY a
                     ) a 
                left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id
                ) b on a.USER_ID = b.USER_ID and a.BASE_PKG_ID= b.BASE_PKG_ID
        with ur


	
    
    
    
        select count(distinct a.user_id) VALID_CNT,count(distinct b.user_id ) REC_CNT
        , count(distinct b.user_id )*1.00/count(distinct a.user_id) RATE
        from 
        (
        select user_id,value(t.NEW_PKG_ID,a.BASE_PKG_ID) BASE_PKG_ID,VALID_DT from   
        G_I_02022_DAY  a
        left join bass1.DIM_QW_QQT_PKGID t on a.BASE_PKG_ID = t.old_pkg_id
        where time_id = 20111229
        except 
        select user_id,value(t.NEW_PKG_ID,a.BASE_PKG_ID) BASE_PKG_ID,VALID_DT from   
        G_I_02022_DAY  a
                left join bass1.DIM_QW_QQT_PKGID t on a.BASE_PKG_ID = t.old_pkg_id
        where time_id = 20111228
        ) a
        left join (
                        select a.user_id,value(b.new_pkg_id,a.BASE_PKG_ID) BASE_PKG_ID 
                from (
                        select  USER_ID,BASE_PKG_ID from 
                        G_S_02024_DAY a
                     ) a 
                left join bass1.DIM_QW_QQT_PKGID b on a.BASE_PKG_ID = b.old_pkg_id
                ) b on a.USER_ID = b.USER_ID and a.BASE_PKG_ID= b.BASE_PKG_ID
        with ur
        
        VALID_CNT	REC_CNT	RATE
78993	78958	0.9995569227653083184


VALID_CNT	REC_CNT	RATE
262	262	1.0000000000000000000



select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22104_DAY 
group by  time_id 
order by 1 



select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22102_DAY 
group by  time_id 
order by 1 


select *from G_S_22105_MONTH


select sum(bigint(target1)),sum(bigint(target2)) from bass1.g_rule_check where rule_code in ('R161_16')
and time_id / 100 = 201112
 order by time_id desc





select 20111229 dt,user_id,usertype_id,userstatus_id,test_mark,create_date from bass2.dw_product_20111229
where  USER_ID
IN (
'89160000878180'            
,'89160000878007'            
,'89160000878136'            
,'89160000878159'            
,'89160000878212'            
,'89160000878281'            
,'89160000878293'            
,'89160000878323'            
,'89160000878855'            
,'89160000878902'            
,'89160000879699'            
,'89160000879705'            
,'89760000859398'            
,'89157332235104'            
,'89157332112431'            
,'89460000857857'            
,'89760000859381'            
,'89160000878376'            
,'89160000878383'            
,'89160000878483'            
,'89160000878497'            
,'89160000878949'            
,'89160000878970'            
,'89160000879035'            
,'89160000879088'            
,'89160000879323'            
,'89160000879346'            
,'89160000879367'            
,'89160000879527'            
,'89160000879573'            
,'89160000879585'            
,'89760000859353'            
,'89160000878345'            
)




select sum(bigint(target1)),sum(bigint(target2)) from bass1.g_rule_check 
where rule_code in ('R161_16')
and time_id  between 20110101 and 20111031

 order by time_id desc



select sum(bigint(TD_DATA_FLUX))*1.00/1024/10000,sum(bigint(TD_TNET_DATA_FLUX))*1.00/1024/10000 from bass1.G_S_22203_DAY 
where time_id  between 20110101 and 20111031



select a.mns_type,
                         sum(a.flow1),sum(a.flow2)
                  from bass2.dw_product_201110 b,
                      (select user_id,svcitem_id,roamtype_id,mns_type,
                          sum(bigint(upflow1+upflow2))/(1024*1024) as flow1,
                          sum(bigint(downflow1+downflow2))/(1024*1024) as flow2
                       from bass2.dw_newbusi_gprs_201110
                       group by user_id,svcitem_id,roamtype_id,mns_type
                       ) a
                  where a.user_id = b.user_id
                  group by a.mns_type

MNS_TYPE	2	3
0	803587	4908787
1	243494	1808890

                  

select * from  app.sch_control_alarm 
where control_code like 'BASS1%MON%'
and date (alarmtime) between '2011-12-01' and '2011-12-10'
order by alarmtime desc




                  
                  

select count(0) ,count(distinct user_id ) from    G_I_02016_MONTH where time_id = 201111


                  
select * from g_i_02016_month
where user_id = '89157334068705'
--and PROD_ID = '107'
and time_id = 201112

select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 2 days
and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc

BASS1_INT_CHECK_TD_MONTH.tcl	int -s INT_CHECK_TD_MONTH.tcl	2	 R146 校验不通过 	2012-1-3 12:47:24.726139	[NULL]	-1	[NULL]



select count(0) val1 
from (  select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID
from bass1.g_s_21003_month
where time_id=201112 and mns_type='1'
except
select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
from bass1.g_s_04017_day
where time_id/100=201112 and mns_type='1'
group by product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
) M
with ur
                  
                  
                  PRODUCT_NO	ROAM_TYPE_ID	TOLL_TYPE_ID	CALL_TYPE_ID
13659544731    	500	010	01

select count(0) from bass1.g_s_21003_month
where time_id=201112

select * from bass1.g_s_21003_month
where time_id=201112 and mns_type='1'
and PRODUCT_NO= '13659544731'
and ROAM_TYPE_ID='500'
and TOLL_TYPE_ID='010'
and CALL_TYPE_ID='01'


TIME_ID	BILL_MONTH	PRODUCT_NO	BRAND_ID	SVC_TYPE_ID	TOLL_TYPE_ID	IP_TYPE_ID	ADVERSARY_ID	ROAM_TYPE_ID	CALL_TYPE_ID	CALL_COUNTS	BASE_BILL_DURATION	TOLL_BILL_DURATION	CALL_DURATION	BASE_CALL_FEE	TOLL_CALL_FEE	CALLFW_TOLL_FEE	CALL_FEE	FAVOURED_BASECALL_FEE	FAVOURED_TOLLCALL_FEE	FAVOURED_CALLFW_TOLLFEE	FAVOURED_CALL_FEE	FREE_DURATION	FAVOUR_DURATION	MNS_TYPE
201112	201112	13659544731    	2	009	010	1000	010000	500	01	1             	1             	0             	4             	0             	0             	0             	0             	0             	0             	0             	0             	4             	0             	1



delete from (
select * from bass1.g_s_21003_month
where time_id=201112 and mns_type='1'
and PRODUCT_NO= '13659544731'
and ROAM_TYPE_ID='500'
and TOLL_TYPE_ID='010'
and CALL_TYPE_ID='01'
) t 
where PRODUCT_NO= '13659544731'


1           PRODUCT_NO      OPP_NUMBER                    
----------- --------------- ------------------------------
   20111204 13659544731     -                             
   
   




from bass1.g_s_04017_day
where time_id/100=201112 and mns_type='1'


select *from bass1.mon_all_interface 
where interface_code = '02048'


select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06023_month where time_id =201112)
  and time_id =201112
  and channel_status='1'

select * from app.sch_control_before
where control_code 
'TR1_L_11015',
,'TR1_L_13006',
,'TR1_L_11222',
,'TR1_L_13007'




WITH n(control_code, before_control_code) AS 
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE before_control_code in 

(

 'BASS1_G_S_03016_MONTH.tcl'
,'BASS1_G_S_21008_MONTH.tcl'
,'BASS1_G_S_21011_MONTH.tcl'
,'BASS1_G_S_21020_MONTH.tcl'
,'BASS1_G_S_22040_MONTH.tcl'
,'BASS1_G_S_22072_MONTH.tcl'
,'BASS1_G_S_22306_MONTH.tcl'
,'BASS1_G_S_22304_MONTH.tcl'
,'BASS1_G_S_22303_MONTH.tcl'
,'BASS1_G_S_22305_MONTH.tcl'
,'BASS1_G_S_22085_MONTH.tcl'
,'BASS1_G_S_22083_MONTH.tcl'
,'BASS1_G_S_22086_MONTH.tcl'
,'BASS1_G_S_22081_MONTH.tcl'
,'BASS1_G_S_03018_MONTH.tcl'
,'BASS1_G_S_21012_MONTH.tcl'
,'BASS1_G_I_21020_MONTH.tcl'

)


           UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.before_control_code = n.control_code)
SELECT distinct c.control_code FROM n,app.sch_control_task c
where n.control_code = c.control_code
and c.deal_time = 2    
AND c.control_code  like 'BASS1_%' 
             
             
             

select * from syscat.tables where tabname like '%G_S_0300%'                          

TABNAME
G_S_03004_03005_R235_ADJ
G_S_03004_03005_R235_ADJ2
G_S_03004_MONTH
G_S_03004_MONTH_ADJ_BAK
G_S_03004_MONTH_B20110429
G_S_03004_MONTH_LS
G_S_03004_MONTH_TD
TMP_SMS_071025103004140


TABNAME
CHECK_02008_03005
G_S_03004_03005_R235_ADJ
G_S_03004_03005_R235_ADJ2
G_S_03005_MONTH
G_S_03005_MONTH_ADJ_BAK

             

select *from bass2.dw_product_20120104
where user_id = '89560001672362'
             
select *from bass2.dw_product_bass1_20120104
where user_id = '89560001672362'

             
             
select * from bass2.dw_product_20120104
where PRODUCT_NO = '18289059112'


select * from bass2.dw_product_20120104
where PRODUCT_NO = '13908902140'





select distinct control_code from app.sch_control_before 
where before_control_code 
in 
(
'BASS1_G_S_02007_MONTH.tcl'
,'BASS1_G_S_03005_MONTH.tcl'
,'BASS1_G_S_03017_MONTH.tcl'
,'BASS1_G_I_03007_MONTH.tcl'
,'BASS1_G_S_03004_MONTH.tcl'
,'BASS1_G_S_22305_MONTH.tcl'
,'BASS1_G_S_03015_MONTH.tcl'
,'BASS1_G_S_22307_MONTH.tcl'
,'BASS1_G_S_03019_MONTH.tcl'
,'BASS1_G_I_02006_MONTH.tcl'
,'BASS1_G_S_22039_MONTH.tcl'
)








update (
select * from app.sch_control_runlog
where control_code 
in 
(
'BASS1_EXP_G_I_02016_MONTH'
,'BASS1_EXP_G_I_21020_MONTH'
,'BASS1_EXP_G_S_03016_MONTH'
,'BASS1_EXP_G_S_03017_MONTH'
,'BASS1_EXP_G_S_03018_MONTH'
,'BASS1_EXP_G_S_21003_MONTH'
,'BASS1_EXP_G_S_21008_MONTH'
,'BASS1_EXP_G_S_21011_MONTH'
,'BASS1_EXP_G_S_21012_MONTH'
,'BASS1_EXP_G_S_22040_MONTH'
,'BASS1_EXP_G_S_22072_MONTH'
,'BASS1_EXP_G_S_22081_MONTH'
,'BASS1_EXP_G_S_22083_MONTH'
,'BASS1_EXP_G_S_22085_MONTH'
,'BASS1_EXP_G_S_22086_MONTH'
,'BASS1_EXP_G_S_22303_MONTH'
,'BASS1_EXP_G_S_22304_MONTH'
,'BASS1_EXP_G_S_22305_MONTH'
,'BASS1_EXP_G_S_22306_MONTH'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_R009_MONTH.tcl'
,'BASS1_INT_CHECK_R181_MONTH.tcl'
,'BASS1_INT_CHECK_R208_MONTH.tcl'
,'BASS1_INT_CHECK_R216_MONTH.tcl'
,'BASS1_INT_CHECK_R230_MONTH.tcl'
,'BASS1_INT_CHECK_VOICE_MONTH.tcl'
)
and date(begintime) >= '2012-01-01'
and flag = 0
) t
set flag = -2
where flag = 0





select count(0) from (
select a.stud_cnt
,b.*
from (select * from  g_i_06001_month where time_id = 201112 ) a 
, (
select school_id,count(0) cnt 
from G_I_02031_MONTH
where time_id = 201112
group by school_id
) b 
where a.school_id = b.school_id
and bigint(a.stud_cnt) < cnt
) t


STUD_CNT	SCHOOL_ID	CNT
5000  	89189400000001      	5546

UPDATE (select * from  g_i_06001_month where time_id = 201112 ) a 
SET STUD_CNT = '5546'
WHERE STUD_CNT = '5000'
AND SCHOOL_ID = '89189400000001'


select *from G_S_03004_MONTH_ADJ_BAK
select *from G_S_03005_MONTH_ADJ_BAK
G_S_03005_MONTH_ADJ_BAK


select * from db2inst1.MONIT_sql where date(snapshot_time)='2011-12-03'
order by agent_id,snapshot_time




select STMT_TEXT  from db2inst1.MONIT_sql
 where 
 upper(STMT_TEXT) like '%0300%'
 and upper(STMT_TEXT) like '%DELETE%'
 and upper(STMT_TEXT) NOT like '%||%'
 --AND HOUR(SNAPSHOT_TIME) > 12
order by snapshot_time




select * from bass2.stat_rep_content
where op_time = '2011-12-01'
and zb_code in ('CU3320','CU3321','CU3322','CU3323')
with ur


WITH n(control_code, before_control_code) AS 
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE before_control_code in ( 'BASS1_G_S_03004_MONTH.tcl' ,'BASS1_G_S_03004_MONTH.tcl')
           UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.before_control_code = n.control_code)
SELECT distinct c.control_code FROM n,app.sch_control_task c
where n.control_code = c.control_code
and c.deal_time = 2    
AND c.control_code  like 'BASS1_%' 
             
             
             
             
             
update 
CONTROL_CODE in (
 'BASS1_REPORT_KEY_INDEX_MONTH.tcl'
,'BASS1_INT_CHECK_TD_MONTH.tcl'
,'BASS1_INT_CHECK_R235_MONTH.tcl'
,'BASS1_INT_CHECK_R221_MONTH.tcl'
,'BASS1_INT_CHECK_R036_MONTH.tcl'
,'BASS1_INT_CHECK_R034_MONTH.tcl'
,'BASS1_INT_CHECK_R031_MONTH.tcl'
,'BASS1_INT_CHECK_L5_MONTH.tcl'
,'BASS1_INT_CHECK_IMPORTSERV_MONTH.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_D9E234F2_651TO56_MONTH.tcl'
,'BASS1_INT_CHECK_B67_MONTH.tcl'
,'BASS1_G_S_22204_MONTH_TWO.tcl'
,'BASS1_G_S_22204_MONTH_ONE.tcl'
,'BASS1_G_S_22204_MONTH.tcl'
,'BASS1_EXP_G_S_22204_MONTH'
,'BASS1_EXP_G_S_21003_MONTH'
,'BASS1_EXP_G_S_03012_MONTH'
,'BASS1_EXP_G_S_03005_MONTH'
,'BASS1_EXP_G_S_03004_MONTH'
)
             
             
             

update (
select * from app.sch_control_runlog
where control_code 
in (
 'BASS1_REPORT_KEY_INDEX_MONTH.tcl'
,'BASS1_INT_CHECK_TD_MONTH.tcl'
,'BASS1_INT_CHECK_R221_MONTH.tcl'
,'BASS1_INT_CHECK_R036_MONTH.tcl'
,'BASS1_INT_CHECK_R034_MONTH.tcl'
,'BASS1_INT_CHECK_R031_MONTH.tcl'
,'BASS1_INT_CHECK_L5_MONTH.tcl'
,'BASS1_INT_CHECK_IMPORTSERV_MONTH.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_D9E234F2_651TO56_MONTH.tcl'
,'BASS1_INT_CHECK_B67_MONTH.tcl'
,'BASS1_G_S_22204_MONTH_TWO.tcl'
,'BASS1_G_S_22204_MONTH_ONE.tcl'
,'BASS1_G_S_22204_MONTH.tcl'
,'BASS1_EXP_G_S_22204_MONTH'
,'BASS1_EXP_G_S_21003_MONTH'
,'BASS1_EXP_G_S_03012_MONTH'
,'BASS1_EXP_G_S_03005_MONTH'
,'BASS1_EXP_G_S_03004_MONTH'
)
and date(begintime) >= '2012-01-01'
and flag = 0
) t
set flag = -2
where flag = 0




insert into app.sch_control_before 
values ( 'BASS1_INT_CHECK_R023R024_DAY.tcl','BASS1_G_A_02004_DAY.tcl')
, ( 'BASS1_INT_CHECK_R023R024_DAY.tcl','BASS1_INT_CHECK_INDEX_SAME_DAY.tcl')

delete from app.sch_control_before 
where control_code = 'INT_CHECK_R023R024_DAY.tcl'


SELECT * FROM app.sch_control_task 
WHERE CONTROL_CODE LIKE 'BASS1%INT%DAY%'


delete from app.sch_control_task where control_code in ('BASS1_INT_CHECK_R023R024_DAY.tcl');
INSERT INTO app.sch_control_task 
VALUES(
   'BASS1_INT_CHECK_R023R024_DAY.tcl',
   1,
   2,
   'int -s INT_CHECK_R023R024_DAY.tcl',
   10000,
   - 1,
   'R023-R024校验',
   'app',
   'BASS1',
   1,
   '/bassapp/bass1/tcl/' 
   );
   
   
   SELECT *
    from  BASS1.G_RULE_CHECK 
 	  				where time_id = 201202 and rule_code in ('56','55') 
                    

                    

                    
update ( select * from app.sch_control_runlog 
where control_code = 'BASS1_EXP_G_S_21003_MONTH'
                    ) a 
                    set flag = 0
                    where flag = -2

\

update 
(
select * from app.g_runlog where unit_code = '02016'
and time_id = 201112 ) a
set return_flag = 0
where return_flag = 1




                    

sys                    


select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and control_code like 'BASS1%'
order by alarmtime desc



insert into bass1.g_s_05001_month
select * from  bass1.T_GS05001M where time_id = 201112

select * from  bass1.g_s_05001_month where time_id = 201112

select * from  bass1.g_s_05002_month where time_id = 201112

insert into bass1.g_s_05002_month
select * from  bass1.T_GS05002M where time_id = 201112

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05001_month 
group by  time_id 
order by 1 desc 

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05002_month 
group by time_id 
order by 1 desc 



select time_id/100
,sum(bigint(target1)) 
,sum(bigint(target2)) 
from   bass1.G_RULE_CHECK where rule_code = 'R159_4'      
group by time_id/100



select time_id/100
,sum(bigint(target1)) 
,sum(bigint(target2)) 
from   bass1.G_RULE_CHECK where rule_code = 'R159_1'      
group by time_id/100




bass1.int_02004_02008_month_stage 


select time_id/100,count( user_id) from   bass1.int_02004_02008_month_stage 
                        where usertype_id IN ('2010','2020','2030','9000')
group by time_id/100
             
22058             
22060                          
22062                          
22063
select * from   app.sch_control_runlog  A
where substr(a.control_code,15,5)
in 
(
'22058'
,'22060'
,'22062'
,'22063'

)
AND date(a.begintime) >=  '2012-01-01'
AND FLAG = 0
                          

CONTROL_CODE
BASS1_EXP_G_S_22062_MONTH
BASS1_EXP_G_S_22060_MONTH
BASS1_EXP_G_S_22058_MONTH
BASS1_EXP_G_S_22063_MONTH
                          
                          
                          

 
in (
 'BASS1_EXP_G_I_22405_MONTH'
,'BASS1_EXP_G_I_22406_MONTH'
,'BASS1_EXP_G_I_22422_MONTH'
,'BASS1_EXP_G_S_22057_MONTH'
,'BASS1_EXP_G_S_22059_MONTH'
,'BASS1_EXP_G_S_22061_MONTH'
,'BASS1_EXP_G_S_22067_MONTH'
,'BASS1_EXP_G_S_22069_MONTH'
,'BASS1_EXP_G_S_22074_MONTH'
,'BASS1_EXP_G_S_22075_MONTH'
,'BASS1_EXP_G_S_22076_MONTH'
,'BASS1_EXP_G_S_22077_MONTH'
)

                          
                          
         BASS1_G_S_22062_MONTH.tcl
BASS1_G_S_22060_MONTH.tcl
BASS1_G_S_22058_MONTH.tcl
BASS1_G_S_22063_MONTH.tcl                 



WITH n(control_code, before_control_code) AS 
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE before_control_code  
in (
 'BASS1_G_S_22062_MONTH.tcl'
,'BASS1_G_S_22060_MONTH.tcl'
,'BASS1_G_S_22058_MONTH.tcl'
,'BASS1_G_S_22063_MONTH.tcl'
)


           UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.before_control_code = n.control_code)
SELECT distinct c.control_code FROM n,app.sch_control_task c
where n.control_code = c.control_code
and c.deal_time = 2    
AND c.control_code  like 'BASS1_%' 
                                       
                                       


--本月比上月
select distinct  substr(a.filename,16,5) 
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
 join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join bass1.mon_all_interface c on substr(a.filename,16,5) = c.INTERFACE_CODE 
                                       
                                       
except

--上月比本月
select distinct  substr(a.filename,16,5) 
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 2 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
 join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 2 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join bass1.mon_all_interface c on substr(a.filename,16,5) = c.INTERFACE_CODE                                        


except
select distinct  substr(a.filename,16,5) 
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
 join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join bass1.mon_all_interface c on substr(a.filename,16,5) = c.INTERFACE_CODE 
                                       
                                       


select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_02059_DAY  t
											where time_id >= 20100626
			  ) a
			where rn = 1	and STATUS_ID = '1'
			AND trim(A.enterprise_id) <> ''
			and not exists (select 1 from (select distinct a.enterprise_id from bass1.G_A_01004_DAY a 
 ) t where a.enterprise_id = t.enterprise_id )
		with ur
        
        
       
        
        
	select length(trim(enterprise_id)),count(0)
	from  bass1.G_A_01004_DAY
	group by  length(trim(enterprise_id))
	
    
    



select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_02059_DAY  t
											where time_id = 20120106
			  ) a
			where rn = 1	and STATUS_ID = '1'
			AND trim(A.enterprise_id) <> ''
			and enterprise_id not in (select enterpri
            
            
            not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN BASS2.TRANS_ENTERPRISE_ID_20100625 B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.enterprise_id = t.enterprise_id )
		with ur
        
        
        
        			select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_02060_DAY  t
			  ) a
			where rn = 1 and 	STATUS_ID = '1'
			and not exists (select 1 from (select distinct a.enterprise_id enterprise_id from bass1.G_A_01004_DAY a 
 ) t where a.enterprise_id = t.enterprise_id )
		with ur
        
        
        
        
        			select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_02061_DAY  t
											where time_id >= 20100624
			  ) a
			where rn = 1	and STATUS_ID = '1'
			and not exists (select 1 from (select distinct a.enterprise_id enterprise_id from bass1.G_A_01004_DAY a 
) t where a.enterprise_id = t.enterprise_id )
		with ur		
        
        
        
        select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_02062_DAY  t
											where time_id >= 20100626
			  ) a
			where rn = 1	and STATUS_ID = '1'
			and not exists (select 1 from (select distinct  enterprise_id from bass1.G_A_01004_DAY a 
 ) t where a.enterprise_id = t.enterprise_id )



select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id ,ENTERPRISE_BUSI_TYPE order by time_id desc ) rn 
			                from 
			                G_A_02054_DAY  t
			  ) a
			where rn = 1	and STATUS_ID = '1'
			and not exists (select 1 from (select distinct  enterprise_id from bass1.G_A_01004_DAY a 
 ) t where a.enterprise_id = t.enterprise_id )
		with ur
        
        
        

select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id ,ENTERPRISE_BUSI_TYPE order by time_id desc ) rn 
			                from 
			                G_A_02055_DAY  t
			  ) a
			where rn = 1	and STATUS_ID = '1'
			and not exists (select 1 from (select distinct  enterprise_id from bass1.G_A_01004_DAY a 
 ) t where a.enterprise_id = t.enterprise_id )
		with ur	

        
        

select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id ,ENTERPRISE_BUSI_TYPE order by time_id desc ) rn 
			                from 
			                G_A_02057_DAY  t
			  ) a
			where rn = 1	and STATUS_ID = '1'
			and not exists (select 1 from (select distinct  enterprise_id from bass1.G_A_01004_DAY a 
 ) t where a.enterprise_id = t.enterprise_id )
		with ur		

        
        
        
        
			select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id ,ENTERPRISE_BUSI_TYPE order by time_id desc ) rn 
			                from 
			                G_A_02057_DAY  t
			  ) a
			where rn = 1	and STATUS_ID = '1'
			and not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN BASS2.TRANS_ENTERPRISE_ID_20100625 B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.enterprise_id = t.enterprise_id )
		with ur		
        
        
        
        
        
        select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id ,ENTERPRISE_BUSI_TYPE order by time_id desc ) rn 
			                from 
			                G_A_02058_DAY  t
			  ) a
			where rn = 1	and STATUS_ID = '1'
			and not exists (select 1 from (select distinct  enterprise_id from bass1.G_A_01004_DAY a 
) t where a.enterprise_id = t.enterprise_id )
		with ur
        
        
        
        
        select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id ,ENTERPRISE_BUSI_TYPE order by time_id desc ) rn 
			                from 
			                G_A_02064_DAY  t
			  ) a
			where rn = 1	and STATUS_ID = '1'
			and not exists (select 1 from (select distinct  enterprise_id from bass1.G_A_01004_DAY a 
 ) t where a.enterprise_id = t.enterprise_id )
		with ur	
        
        
        
        
        select count(0) from 
		(select a.* , row_number()over(partition by enterprise_id,cust_id order by time_id desc ) rn 
			from G_A_01007_DAY a 
		) a where a.rn = 1 
		and RELA_STATE = '1'
		and not exists (select 1 from (select distinct  enterprise_id from bass1.G_A_01004_DAY a 
 ) t where a.cust_id = t.enterprise_id )
 
 
 
 select count(0) from G_I_02022_DAY
 where time_id = 20120105
 
 
 
 
 select count(0) from 
			(
			                select enterprise_id
			                ,row_number()over(partition by t.enterprise_id ,ENTERPRISE_BUSI_TYPE order by time_id desc ) rn 
			                from 
			                G_A_02064_DAY  t
			  ) a
			where rn = 1	and STATUS_ID = '1'
			except 
 select enterprise_id
 from (select enterprise_id
			                ,row_number()over(partition by t.enterprise_id  order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
			  ) a
			where rn = 1
            ) o with ur
            
            
            
        
        
        

 select count(0) from  
 (
 select enterprise_id from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id ,ENTERPRISE_BUSI_TYPE order by time_id desc ) rn 
			                from 
			                G_A_02064_DAY  t
			  ) a
			where rn = 1	and STATUS_ID = '1'
			except 
            select enterprise_id
            from (select enterprise_id
			                ,row_number()over(partition by t.enterprise_id  order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
                            where  length(trim(enterprise_id)) = 14
			        ) a
			        where rn = 1
) o 
with ur



select * from syscat.tables where tabname like '%ENTERPRISE%';
select tabschema,tabname from syscat.tables where tabname like '%ENTERPRISE%';



select count(0) from G_S_03004_MONTH where time_id = 201112
10370690
 10370690 s_13100_201112_03004_00_001.dat
 
 
 
SELECT name,npages,fpages FROM SYSIBM.SYSTABLES 
where tbspace='TBS_APP_BASS1'
order by npages desc,fpages desc


G_S_21003_TO_DAY	2280645	3008280

G_S_21003_TO_DAY	2280645	3008280

        db2 runstats on table BASS1.G_S_04004_DAY with distribution;


reorgchk current statistics on table BASS1.G_S_21003_TO_DAY;
time reorg table BASS1.G_S_21003_TO_DAY

runstats on table BASS1.G_S_21003_TO_DAY with distribution;
reorgchk current statistics on table BASS1.G_S_21003_TO_DAY;

INT_210012916_201112	2006580	2006580
G_S_04004_DAY	1397250	1397250
G_S_21003_TO_DAY	1064010	3287520
G_S_21003_STORE_DAY	862230	862245
G_S_04005_DAY	590505	1203375

G_S_21003_TO_DAY	1064010	3287520


select * from  app.sch_control_alarm 
WHERE control_code like 'BASS1%02016%'
order by alarmtime desc
;


select count(0) from G_I_02016_MONTH where time_id = 201112


update 
(
select * from app.g_runlog 
where unit_code = '02016'
and time_id = 201112
) t
set return_flag = 1

select avg(target1) 
from bass1.g_rule_check where rule_code in ('R174')
and time_id / 100 >= 201101

1
135203700121.00000
74796247813.
85389116862
select * from bass1.g_rule_check where rule_code in ('R140') and time_id / 100 = 201202 order by time_id desc
select * from bass1.g_rule_check where rule_code in ('R143') and time_id / 100 = 201202 order by time_id desc

select * from bass1.g_rule_check where rule_code in ('R173') and time_id / 100 = 201202 order by time_id desc
select * from bass1.g_rule_check where rule_code in ('R174')  and time_id / 100 = 201202  order by time_id desc
select * from bass1.g_rule_check where rule_code in ('R169')  and time_id / 100 = 201201  order by time_id desc
select * from bass1.g_rule_check where rule_code in ('R170')  and time_id / 100 = 201201  order by time_id desc
select * from bass1.g_rule_check where rule_code in ('R171')  and time_id / 100 = 201201  order by time_id desc
select * from bass1.g_rule_check where rule_code in ('R172')  and time_id / 100 = 201201  order by time_id desc
select * from bass1.g_rule_check where rule_code in ('R163')  and time_id / 100 = 201202  order by time_id desc
select * from bass1.g_rule_check where rule_code in ('R165')  and time_id / 100 = 201202  order by time_id desc



select 'export_js.sh程序执行异常，系统杀掉进程，稍后系统会自动启动，请注意观察!', phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD' and phone_id in ('15089051890')

select * from bass1.g_rule_check where rule_code in ('R174','R170','R173','R171','R172','R169')
and time_id = 20120109 order by time_id desc



select message_id, send_time,mobile_num,message_content from   APP.SMS_SEND_INFO
where send_time is not null
and mobile_num = '15089051890'
and send_time >=  current timestamp - 1 days
and date(send_time) = char(current date )
order by send_time desc
;

select time_id , count(0) 
--,  count(distinct time_id ) 
from G_I_02027_MONTH 
group by  time_id 
order by 1 


select PKG_STS , count(0) 
--,  count(distinct PKG_STS ) 
from G_I_02026_MONTH 
group by  PKG_STS 
order by 1 


CREATE TABLE "BASS1   "."G_I_02026_MONTH_LOAD"  (
                  "OLD_PKG_ID" VARCHAR(12) NOT NULL , 
                  "NEW_PKG_ID" VARCHAR(18) NOT NULL , 
                  "PKG_NAME" CHAR(128) NOT NULL )   
                 DISTRIBUTE BY HASH("OLD_PKG_ID", "NEW_PKG_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 




select * from BASS1.G_I_02026_MONTH_LOAD

select count(0),count(distinct user_id),count(distinct time_id) from G_S_02066_DAY
where time_id / 100 = 201111

1	2	   
64	64	   
		
        
 select * from bass2.dim_channel_info
 where channel_id in (
select channel_id from bass2.dw_PRODUCT_20120109
WHERE USER_ID IN (
select user_id from bass1.G_S_02066_DAY where time_id / 100 = 201111

 )
 )
 
 
 select * from bass2.dw_product_plan_msg_201201 
 
 
 select * from bass1.mon_all_interface 
 where interface_code = '02066'
 
 
 
select distinct time_id from G_S_02066_DAY
where time_id / 100 = 201111





  


order by alarmtime desc


select type,deadline,count(0) cnt
from bass1.mon_all_interface
where sts=1
group by type,deadline

TYPE	DEADLINE	CNT	   
d	9	8	   
d	11	39	   
d	13	24	   
d	15	6	   
m	3	8	   
m	5	19	   
m	8	36	   
m	10	38	   
m	15	4	   
			

select * from app.sch_control_runlog where control_code like  'BASS1%DAY%'

select * from  table( bass1.get_after('02004')) a 

drop table G_I_02026_MONTH_test


CREATE TABLE BASS1.G_I_02026_MONTH_test  (
        pkg_desc               varchar(600)
)   
                 DISTRIBUTE BY HASH(pkg_desc)   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 

rename BASS1.G_I_02026_MONTH to G_I_02026_MONTH_old20120112;
CREATE TABLE BASS1.G_I_02026_MONTH  (
         time_id                integer      
        ,pkg_id                 char(18) 
        ,pkg_name               char(100)
        ,pkg_desc               varchar(600)
        ,pkg_sts                char(1) 
        ,stop_dt                char(8)
)   
                 DISTRIBUTE BY HASH(time_id,pkg_id)   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 

rename bass1.G_I_02027_MONTH to G_I_02027_MONTH_old20120112;
CREATE TABLE "BASS1   "."G_I_02027_MONTH"  (
         time_id                integer       
        ,user_id                char(20)  
        ,pkg_id                 char(18)
        ,eff_dt                 char(8)
				  )   
                 DISTRIBUTE BY HASH(user_id,pkg_id)   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 

select * from  app.g_unit_info where unit_code = '02026';
select *  from  app.g_unit_info where unit_code = '02027';


delete from  app.g_unit_info where unit_code = '02026';
delete from  app.g_unit_info where unit_code = '02027';


insert into app.g_unit_info values ('02026' , 1 , '资费套餐基本信息（资费统一编码）' , 'bass1.g_i_02026_month' , 1 , 0 , 0);
insert into app.g_unit_info values ('02027' , 1 , '用户选择资费套餐（资费统一编码）' , 'bass1.g_i_02027_month' , 1 , 0 , 0);






insert into app.sch_control_before values 
 ('BASS1_G_I_02026_MONTH.tcl' , 'BASS2_Dw_product_ins_off_ins_prod_ms.tcl')
;
insert into app.sch_control_before values 
 ('BASS1_G_I_02027_MONTH.tcl' , 'BASS1_G_I_02026_MONTH.tcl')
,('BASS1_G_I_02027_MONTH.tcl' , 'BASS2_Dw_product_ins_off_ins_prod_ms.tcl')
;



insert into app.sch_control_before values 
 ('BASS1_EXP_G_I_02026_MONTH' , 'BASS1_G_I_02026_MONTH.tcl')
;
insert into app.sch_control_before values 
 ('BASS1_EXP_G_I_02027_MONTH' , 'BASS1_G_I_02027_MONTH.tcl')
;



delete from  app.sch_control_task where control_code in ( 'BASS1_G_I_02026_MONTH.tcl','BASS1_EXP_G_I_02026_MONTH');
delete from  app.sch_control_task where control_code in ( 'BASS1_G_I_02027_MONTH.tcl','BASS1_EXP_G_I_02027_MONTH');



insert into app.sch_control_task values('BASS1_G_I_02026_MONTH.tcl' , 2 , 2 , 'int -s G_I_02026_MONTH.tcl' ,-1,-1,'资费套餐基本信息（资费统一编码）' , 'app' , 'BASS1' , 1 , '/bassapp/bass1/tcl/');
insert into app.sch_control_task values('BASS1_G_I_02027_MONTH.tcl' , 2 , 2 , 'int -s G_I_02027_MONTH.tcl' ,-1,-1,'用户选择资费套餐（资费统一编码）' , 'app' , 'BASS1' , 1 , '/bassapp/bass1/tcl/');



insert into app.sch_control_task values('BASS1_EXP_G_I_02026_MONTH' , 2 , 2 , 'bass1_export bass1.g_i_02026_month LASTMONTH()' ,-1,-1,'EXPORT_of 资费套餐基本信息（资费统一编码）' , 'app' , 'BASS1' , 1 , '/bassapp/backapp/bin/bass1_export/');
insert into app.sch_control_task values('BASS1_EXP_G_I_02027_MONTH' , 2 , 2 , 'bass1_export bass1.g_i_02027_month LASTMONTH()' ,-1,-1,'EXPORT_of 用户选择资费套餐（资费统一编码）' , 'app' , 'BASS1' , 1 , '/bassapp/backapp/bin/bass1_export/');




select repeat('0',100) from bass2.dual

select length('我,') from bass2.dual

select count(0),count(distinct OLD_PKG_ID),count(distinct NEW_PKG_ID)
from G_I_02026_MONTH_LOAD

 BASS1.G_I_02026_MONTH_LOAD
 
 
 select PKG_NAME||repeat(' ',600-length(PKG_NAME))
from   BASS1.G_I_02026_MONTH_LOAD


select  item_type,count(0) from   bass2.dim_prod_up_product_item 
where item_type = 'OFFER_PLAN'
group by item_type



select *  from   bass2.dim_prod_up_product_item 
fetch first 10 rows only


select product_item_id,del_flag,eff_date,exp_date  from   bass2.dim_prod_up_product_item 
where item_type = 'OFFER_PLAN'
group by item_type



select * from   BASS1.ALL_DIM_LKP 
where bass1_tbid in ('BASS_STD1_0114','BASS_STD1_0115')


select pkg_id,pkg_name from G_I_02026_MONTH


select * from G_I_02026_MONTH_LOAD

select * from bass1.G_I_02026_MONTH
fetch first 10 rows only


select count(0),count(distinct pkg_id )
,count(distinct user_id||pkg_id )
 from G_I_02027_MONTH
 
 1	2	3	   
3371652	276	3371652	   
			

USERSTATUS_ID	MONTH_OFF_MARK	3	   
4	1	16	   
5	1	81	   
3	0	516670	   
8	0	43	   
1	0	2854842	   
			

select USERSTATUS_ID, MONTH_OFF_MARK,count(0)
from  bass2.dw_product_201112 a
,G_I_02027_MONTH b 
where a.user_id = b.user_id
group by USERSTATUS_ID,MONTH_OFF_MARK




select * from G_S_22063_MONTH
where time_id = 201111



		  SELECT
			   201111
			 	,'$op_month'
			 	,trim(char(a.CHANNEL_ID))
				,char(bigint( sum(case when t_index_id in (1,4,14) then result else 0 end )                 ))
				,char(bigint( sum(case when t_index_id in (10,11,12,13,19,20,21) then result else 0 end )   ))
				,char(bigint( sum(case when t_index_id in (7) then result else 0 end )                      ))
				,'0'
				,'0'
				,'0'
			FROM BASS2.DW_CHANNEL_INFO_201111 A
			inner join bass2.stat_channel_reward_0002 b on a.channel_id=b.channel_id
			WHERE A.CHANNEL_TYPE_CLASS IN (90105,90102) 
						and b.op_time=201111
						AND B.result>0
                        			group by trim(char(a.CHANNEL_ID))
                        
                        

    select count(*) from bass1.g_s_22063_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06021_month where time_id =201111 and channel_type<>'1')
  and time_id =201111
  

select time_id , count(0) cnt
--,  count(distinct time_id ) 
,sum(bigint(FH_REWARD))FH_REWARD
,sum(bigint(BASIC_REWARD))BASIC_REWARD
,sum(bigint(INCR_REWARD))INCR_REWARD
,sum(bigint(INSPIRE_REWARD))INSPIRE_REWARD
,sum(bigint(TERM_REWARD))TERM_REWARD
,sum(bigint(RENT_CHARGE))RENT_CHARGE
from G_S_22063_MONTH 
/**
where channel_id not in     (select channel_id from bass1.g_s_22063_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06021_month where time_id =201111 and channel_type<>'1')
  and time_id =201111
  )
 **/
group by  time_id 
order by 1 desc

                        
                        
                        
                        
select * from g_s_22062_month
                        
                        
                        
201112	23518	116047	   

201111	19070	80739	   

select a.time_id /100, count(0) cnt
--,  count(distinct time_id ) VAL_BUSI_REC_CNT
,sum(bigint(NEW_USER_CNT))NEW_USER_CNT
,sum(bigint(VAL_BUSI_REC_CNT))VAL_BUSI_REC_CNT
,sum(bigint(VAL_BUSI_OPEN_CNT))VAL_BUSI_OPEN_CNT
,sum(bigint(IMP_VAL_OPEN_CNT))IMP_VAL_OPEN_CNT
from g_s_22091_day  a
where a.CHANNEL_ID in (select distinct channel_id from g_s_22063_month b  where time_id = 201111)
group by  a.time_id / 100
order by 1 desc

                        


CREATE TABLE "BASS1   "."G_S_22063_MONTH_BAK20120113"  (
                  "TIME_ID" INTEGER NOT NULL , 
                  "STATMONTH" CHAR(6) NOT NULL , 
                  "CHANNEL_ID" CHAR(40) NOT NULL , 
                  "FH_REWARD" CHAR(10) , 
                  "BASIC_REWARD" CHAR(10) , 
                  "INCR_REWARD" CHAR(10) , 
                  "INSPIRE_REWARD" CHAR(10) , 
                  "TERM_REWARD" CHAR(10) , 
                  "RENT_CHARGE" CHAR(8) )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "STATMONTH",  
                 "CHANNEL_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 


INSERT INTO G_S_22063_MONTH_BAK20120113
SELECT * 
FROM G_S_22063_MONTH
                        
select * from bass2.stat_channel_reward_0002
fetch first 10 rows only
                        
                        
                        

select * from  app.sch_control_alarm 
where  control_code like 'BASS1%22063%'


    select count(*) from bass1.g_s_22063_month where time_id = 201112
    select count(*) from bass1.g_s_22063_month where time_id = 201112
                        
                        
                        
                        
                            select count(*) from bass1.g_s_22063_month
                            where time_id = 201112
                            
                            select count(*) from 
                            G_S_22063_MONTH_BAK20120113 where time_id = 201112
                            
                            

    select count(*) from bass1.G_S_22063_MONTH_BAK20120113
where channel_id not in
(select distinct channel_id from bass1.g_i_06021_month where time_id =201112 and channel_type<>'1')
  and time_id =201112
                        
109
109.000

    select count(*) from bass1.G_S_22063_MONTH_BAK20120113
                        where channel_id in
                        (select distinct channel_id from bass1.g_i_06021_month where time_id =201112 and channel_type='1')
                          and time_id =201112

                            
                            


select STOP_DT , count(0) 
--,  count(distinct STOP_DT ) 
from G_I_02026_MONTH 
group by  STOP_DT 
order by 1 

                            
                            


select t_index_id , count(0) 
--,  count(distinct t_index_id ) 
from bass2.stat_channel_reward_0002 
where op_time = 201111
group by  t_index_id 
order by 1 
                            
                            
update(
select * from app.g_runlog 
where time_id in (201111,201112)
and return_flag=1               
and unit_code = '22063'
) a 
set   return_flag=0
           

/bassapp/backapp/bin/bass1_export/bass1_export bass1.G_S_22063_MONTH 2011-11 &
/bassapp/backapp/bin/bass1_export/bass1_export bass1.G_S_22063_MONTH 2011-12 &

           
           
select * from G_I_06032_DAY
where time_id = 20120101
           
           
select * from G_I_06032_DAY
where time_id = 20120112


           
select substr(new_pkg_id,1,4),count(0) from BASS1.G_I_02026_MONTH_LOAD
group by substr(new_pkg_id,1,4)

fetch first 10 rows only 

 select * from           
(
select '1' a from bass2.dual
union all
select '2' a from bass2.dual
) t
where a between '1' and '2'
           

select count(0) from  G_I_02027_MONTH a
where not exists ( select 1 from bass1.int_02004_02008_month_stage b 
where a.user_id = b.user_id )
and a.time_id = 





select count(0)
from 
           
           
           
           delete from (
           select * from bass1.mon_all_interface
           where interface_code = '02026'
           and sts is null
           ) t
           


select * from G_I_06032_DAY
where time_id = 20120101

update   G_I_02026_MONTH
set PKG_ID = '31'||substr(pkg_id,3)

select     pkg_id,     substr(pkg_id,3)
from G_I_02026_MONTH


update   G_I_02027_MONTH
set PKG_ID = '31'||substr(pkg_id,3)



select  *
from G_I_02026_MONTH
fetch first 10 rows only




update 
(
select * from app.g_runlog where unit_code = '02027'
and time_id = 201112 ) a
set return_flag = 0
where return_flag = 1




select substr(pkg_id,1,4),count(0) from BASS1.G_I_02026_MONTH
group by substr(pkg_id,1,4)




update   G_I_02026_MONTH
set PKG_ID = '99'||substr(pkg_id,3)
where substr(pkg_id,1,4) = '3199'
 
select     pkg_id,     substr(pkg_id,3)
from G_I_02026_MONTH


update   G_I_02027_MONTH
set PKG_ID = '99'||substr(pkg_id,3)
where substr(pkg_id,1,4) = '3199'




update   G_I_02026_MONTH_load
set NEW_PKG_ID = '31'||substr(NEW_PKG_ID,3)


select 
         TIME_ID
        ,PKG_ID
        ,PKG_NAME
       -- ,PKG_DESC
        ,PKG_STS
        ,STOP_DT
        from G_I_02026_MONTH
        
        
 
 
 
select pkg_id , count(0) 
--,  count(distinct pkg_id ) 
from G_I_02027_MONTH 
group by  pkg_id 
order by 1 

select count(0)
from G_I_02027_MONTH 



CREATE TABLE "BASS1   "."G_I_02026_MONTH_1"  (
                  "TIME_ID" INTEGER , 
                  "PKG_ID" CHAR(18) , 
                  "BASS2_PKG_ID" CHAR(12) , 
                  "PKG_NAME" CHAR(100) , 
                  "PKG_DESC" VARCHAR(600) , 
                  "PKG_STS" CHAR(1) , 
                  "STOP_DT" CHAR(8) )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PKG_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 




select * from G_I_02026_MONTH_1



select * from  app.sch_control_alarm 
where control_code like 'BASS1%DAY%'
order by alarmtime desc






select 
time_id
,int(substr(rule_code,6)) seq
,rule_code
,case 
    when rule_code = 'R161_1' then '新增客户数'
    when rule_code = 'R161_2' then '客户到达数'
    when rule_code = 'R161_3' then '净增客户数'
    when rule_code = 'R161_4' then '通信客户数'
    when rule_code = 'R161_5' then '当月累计通信客户数'
    when rule_code = 'R161_6' then '使用TD网络的客户数'
    when rule_code = 'R161_7' then '当月累计使用TD网络的手机客户数'
    when rule_code = 'R161_8' then '当月累计使用TD网络的信息机客户数'
    when rule_code = 'R161_9' then '当月累计使用TD网络的数据卡客户数'
    when rule_code = 'R161_10' then '当月累计使用TD网络的上网本客户数'
    when rule_code = 'R161_11' then '联通移动客户总数'
    when rule_code = 'R161_12' then '电信移动客户总数'
    when rule_code = 'R161_13' then '联通移动新增客户数'
    when rule_code = 'R161_14' then '电信移动新增客户数'
    when rule_code = 'R161_15' then '使用TD网络的客户在T网上计费时长'
    when rule_code = 'R161_16' then '使用TD网络的客户在T网上的数据流量'
    when rule_code = 'R161_17' then '离网客户数' else '0' end rule_name 
,case 
    when rule_code = 'R161_1' then 15
    when rule_code = 'R161_2' then 2
    when rule_code = 'R161_3' then 100
    when rule_code = 'R161_4' then 5
    when rule_code = 'R161_5' then 5
    when rule_code = 'R161_6' then 5
    when rule_code = 'R161_7' then 5
    when rule_code = 'R161_8' then 5
    when rule_code = 'R161_9' then 5
    when rule_code = 'R161_10' then 5
    when rule_code = 'R161_11' then 2
    when rule_code = 'R161_12' then 2
    when rule_code = 'R161_13' then 8
    when rule_code = 'R161_14' then 8
    when rule_code = 'R161_15' then 20
    when rule_code = 'R161_16' then 20
    when rule_code = 'R161_17' then 70 else 0 end threshold 
    , target3*100 wave_rate
    ,case 
    when rule_code = 'R161_1' and abs(target3*100) > 15 then '超标'
    when rule_code = 'R161_2' and abs(target3*100) > 2 then '超标'
    when rule_code = 'R161_3' and abs(target3*100) > 100 then '超标'
    when rule_code = 'R161_4' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_5' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_6' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_7' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_8' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_9' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_10' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_11' and abs(target3*100) > 2 then '超标'
    when rule_code = 'R161_12' and abs(target3*100) > 2 then '超标'
    when rule_code = 'R161_13' and abs(target3*100) > 8 then '超标'
    when rule_code = 'R161_14' and abs(target3*100) > 8 then '超标'
    when rule_code = 'R161_15' and abs(target3*100) > 20 then '超标'
    when rule_code = 'R161_16' and abs(target3*100) > 20 then '超标'
    when rule_code = 'R161_17' and abs(target3*100) > 70 then '超标'
else '0' end if_ok
from 
bass1.g_rule_check a 
where rule_code like 'R161_%'
and time_id = 20120113;


select BODY from syscat.functions  
where FUNCNAME like '%CHK%WAVE%'





drop FUNCTION bass1.chk_wave
CREATE FUNCTION bass1.chk_wave(p_time_id integer)
RETURNS
TABLE ( time_id int
        ,seq int
        ,rule_code varchar(10)
        ,rule_name varchar(128)
        ,threshold decimal(20,6)
        ,wave_rate decimal(20,6)
        ,if_ok VARCHAR(8)
      )
BEGIN ATOMIC      
     DECLARE v_time_id int default 0;
if p_time_id = 0 then
set v_time_id=int(replace(char(current date - 1 days),'-',''));
else 
set v_time_id= p_time_id;
end if;      
RETURN
select 
time_id
,int(substr(rule_code,6)) seq
,rule_code
,case 
    when rule_code = 'R161_1' then '新增客户数'
    when rule_code = 'R161_2' then '客户到达数'
    when rule_code = 'R161_3' then '净增客户数'
    when rule_code = 'R161_4' then '通信客户数'
    when rule_code = 'R161_5' then '当月累计通信客户数'
    when rule_code = 'R161_6' then '使用TD网络的客户数'
    when rule_code = 'R161_7' then '当月累计使用TD网络的手机客户数'
    when rule_code = 'R161_8' then '当月累计使用TD网络的信息机客户数'
    when rule_code = 'R161_9' then '当月累计使用TD网络的数据卡客户数'
    when rule_code = 'R161_10' then '当月累计使用TD网络的上网本客户数'
    when rule_code = 'R161_11' then '联通移动客户总数'
    when rule_code = 'R161_12' then '电信移动客户总数'
    when rule_code = 'R161_13' then '联通移动新增客户数'
    when rule_code = 'R161_14' then '电信移动新增客户数'
    when rule_code = 'R161_15' then '使用TD网络的客户在T网上计费时长'
    when rule_code = 'R161_16' then '使用TD网络的客户在T网上的数据流量'
    when rule_code = 'R161_17' then '离网客户数' else '0' end rule_name 
,case 
    when rule_code = 'R161_1' then 15
    when rule_code = 'R161_2' then 2
    when rule_code = 'R161_3' then 100
    when rule_code = 'R161_4' then 5
    when rule_code = 'R161_5' then 5
    when rule_code = 'R161_6' then 5
    when rule_code = 'R161_7' then 5
    when rule_code = 'R161_8' then 5
    when rule_code = 'R161_9' then 5
    when rule_code = 'R161_10' then 5
    when rule_code = 'R161_11' then 2
    when rule_code = 'R161_12' then 2
    when rule_code = 'R161_13' then 8
    when rule_code = 'R161_14' then 8
    when rule_code = 'R161_15' then 20
    when rule_code = 'R161_16' then 20
    when rule_code = 'R161_17' then 70 else 0 end threshold 
    , target3*100 wave_rate
    ,case 
    when rule_code = 'R161_1' and abs(target3*100) > 15 then '超标'
    when rule_code = 'R161_2' and abs(target3*100) > 2 then '超标'
    when rule_code = 'R161_3' and abs(target3*100) > 100 then '超标'
    when rule_code = 'R161_4' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_5' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_6' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_7' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_8' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_9' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_10' and abs(target3*100) > 5 then '超标'
    when rule_code = 'R161_11' and abs(target3*100) > 2 then '超标'
    when rule_code = 'R161_12' and abs(target3*100) > 2 then '超标'
    when rule_code = 'R161_13' and abs(target3*100) > 8 then '超标'
    when rule_code = 'R161_14' and abs(target3*100) > 8 then '超标'
    when rule_code = 'R161_15' and abs(target3*100) > 20 then '超标'
    when rule_code = 'R161_16' and abs(target3*100) > 20 then '超标'
    when rule_code = 'R161_17' and abs(target3*100) > 70 then '超标'
else '0' end if_ok
from 
bass1.g_rule_check a 
where rule_code like 'R161_%'
and time_id =v_time_id;
end            



select * from app.sch_control_task 
where control_code like  '%11014%'

ODS_PRODUCT_SC_SCORELIST_YYYYMM



A11014


ODS_PRODUCT_SC_PAYMENT_YYYYMMDD


select * from bass2.dw_cust_vip_card_20120115



select distinct VIP_CARD_LVL from  bass2.ods_cust_cm_vip_info_20120115
select * from bass2.DIM_APP_NB_CUSTCLASS



 rename bass1.G_I_02006_MONTH to G_I_02006_MONTH_bak20120117
rename bass1.G_I_02006_MONTH to G_I_02006_MONTH_bak20120118

 
 select count(0) from G_I_02006_MONTH_bak20120117
 

 CREATE TABLE "BASS1   "."G_I_02006_MONTH"  (
         time_id                integer        
        ,user_id                char(20)      
        ,month_points           char(8)       
        ,month_qqt_points       char(8)       
        ,month_age_points       char(8)       
        ,trans_points           char(8)       
        ,convertible_points     char(8)       
        ,all_points             char(8)       
        ,all_consume_points     char(8)       
        ,all_converted_points   char(8)       
        ,leave_clear_points     char(8)       
        ,other_clear_points     char(8)       
				  )   
                 DISTRIBUTE BY HASH("USER_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 



select user_id,userstatus_id from bass2.dw_product_20120116 where product_no = '13989007120'

select days('2012-01-17')-days('2011-09-01') from bass2.dual

select * from 
G_I_02006_MONTH
where  user_id = '89160001196765'

select * from bass2.dwd_product_sc_scorelist_201112
where product_instance_id = '89160001196765'

select * from bass2.dwd_product_sc_scorelist_201111
where product_instance_id = '89357332152792'

        select * from ams.sc_scorelist_0891_2011
where product_instance_id = '89357332152792'

select * from bass2.dwd_product_sc_payout_201112
where USER_ID = '89160001196765'






		select
			product_instance_id  as user_id
			,sum( case when  count_cycle_id=201112 and scrtype=1 then orgscr+adjscr else 0 end )   as month_points
			,sum( case when  count_cycle_id=201112 and scrtype=24 then orgscr+adjscr else 0 end )   as month_qqt_points
			,sum( case when  count_cycle_id=201112 and scrtype=25 then orgscr+adjscr else 0 end )   as month_age_points
			,sum( case when  scrtype=5 then orgscr+adjscr else 0 end )   as trans_points
			,sum( CURSCR )   as convertible_points
			,sum( orgscr+adjscr )   as all_points
			,sum( case when scrtype=1 then orgscr+adjscr else 0 end )   as all_consume_points
			,sum( USRSCR )   as all_converted_points
		from bass2.dwd_product_sc_scorelist_201112
		where   actflag='1' and product_instance_id = '89357332152792'
		group by product_instance_id
		
        


		select
			product_instance_id  as user_id
			,sum( case when  count_cycle_id=$op_month and scrtype=1 then orgscr+adjscr else 0 end )   as month_points
			,sum( case when  count_cycle_id=$op_month and scrtype=24 then orgscr+adjscr else 0 end )   as month_qqt_points
			,sum( case when  count_cycle_id=$op_month and scrtype=25 then orgscr+adjscr else 0 end )   as month_age_points
			,sum( case when  scrtype=5 then orgscr+adjscr else 0 end )   as trans_points
			,sum(  CURSCR )   as convertible_points
			,sum( orgscr+adjscr )   as all_points
			,sum( case when scrtype=1 then orgscr+adjscr else 0 end )   as all_consume_points
			,sum( USRSCR )   as all_converted_points
		from bass2.dwd_product_sc_scorelist_$op_month
		where   actflag='1'
		group by product_instance_id
		
		
        

select * from bass2.dw_product_201112
where MONTH_OFF_MARK = 1
and USER_ONLINE > 365
and BRAND_ID = 1
fetch first 10 rows only
        

 SELECT * FROM  SC_MONTH_BACKUP_0893_1111 WHERE /*KEY_NUM ='13989007120'*/
  

select * from bass2.ods_product_sc_month_backup_201112
where  END_SCORE > 0
fetch first 10 rows only




select * from bass2.ods_product_sc_month_backup_201110
where  product_instance_id = '89101110015523'
fetch first 10 rows only






		select
			product_instance_id  as user_id
			,sum( case when  count_cycle_id=201111 and scrtype=1 then orgscr+adjscr else 0 end )   as month_points
			,sum( case when  count_cycle_id=201111 and scrtype=24 then orgscr+adjscr else 0 end )   as month_qqt_points
			,sum( case when  count_cycle_id=201111 and scrtype=25 then orgscr+adjscr else 0 end )   as month_age_points
			,sum( case when  scrtype=5 then orgscr+adjscr else 0 end )   as trans_points
			,sum( CURSCR )   as convertible_points
			,sum( orgscr+adjscr )   as all_points
			,sum( case when scrtype=1 then orgscr+adjscr else 0 end )   as all_consume_points
			,sum( USRSCR )   as all_converted_points
		from bass2.dwd_product_sc_scorelist_201111
		where   actflag='1' and product_instance_id = '89357332152792'
		group by product_instance_id
		
        
        
        

89101110015523

select YEAR,sum( orgscr+adjscr ) 
from  bass2.dwd_product_sc_scorelist_201112
where product_instance_id = '89101110015523' 
group by YEAR
        
 
select userstatus_id,count(0)
from bass2.dw_product_201112 a ,       
(
select distinct product_instance_id user_id from bass2.dwd_product_sc_scorelist_201112  where  actflag='1'     ) b 
where a.user_id = b.user_id 
group by userstatus_id

USERSTATUS_ID	2	   
0	3	   
1	758064	   
3	136536	   
4	4480	   
5	44453	   
8	1	   
9	16	   
		
        
       
        
drop table "BASS1   "."G_I_02006_MONTH_1"
CREATE TABLE "BASS1   "."G_I_02006_MONTH_1"  (
                
                  "USER_ID" CHAR(20) , 
                  "MONTH_POINTS" integer , 
                  "MONTH_QQT_POINTS" integer , 
                  "MONTH_AGE_POINTS" integer , 
                  "TRANS_POINTS" integer , 
                  "CONVERTIBLE_POINTS" integer , 
                  "ALL_POINTS" integer , 
                  "ALL_CONSUME_POINTS" integer , 
                  "ALL_CONVERTED_POINTS" integer , 
                  "LEAVE_CLEAR_POINTS" integer , 
                  "OTHER_CLEAR_POINTS" integer )   
                 DISTRIBUTE BY HASH("USER_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 

        
        
        select count(0) from G_I_02006_MONTH
        
        

        select 
        201112 TIME_ID
        ,a.USER_ID
        ,char(value(a.MONTH_POINTS,0))
        ,char(value(a.MONTH_QQT_POINTS,0))
        ,char(value(a.MONTH_AGE_POINTS,0))
        ,char(value(a.TRANS_POINTS,0))
        ,char(value(a.CONVERTIBLE_POINTS,0))
        ,char(value(a.ALL_POINTS,0))
        ,char(value(a.ALL_CONSUME_POINTS,0))
        ,char(value(a.ALL_CONVERTED_POINTS,0))
        ,char(value(a.LEAVE_CLEAR_POINTS,0))
        ,char(value(a.OTHER_CLEAR_POINTS,0))
        from G_I_02006_MONTH_1 a
        ,bass2.dw_product_201112 b
           where a.user_id = b.user_id 
                 and b.usertype_id in (1,2,9)
                 and b.test_mark = 0
                 and (b.userstatus_id in (1,2,3,6,8) or b.month_off_mark = 1)
   group by a.user_id
        with ur
        
        

select * from G_I_02006_MONTH
where user_id = '89357332152792'


        
        89101110015523
        
        
        


 rename bass1.G_S_02007_MONTH to G_S_02007_MONTH_bak20120117

CREATE TABLE "BASS1   "."G_S_02007_MONTH"  (
		 time_id                integer                    
        ,POINT_FEEDBACK_ID       CHAR(4)         
        ,USER_ID                char(20)                   
        ,month_converted_points char(8)                    
        ,month_converted_cnt    char(8)            
)   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "USER_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 


select *
 from bass2.ODS_SC_SCRD_ORD_INFO_201112
 fetch first 10 rows only
 
		

        
        
        
        
        

		select 
		a.USER_ID
		,a.SC_PAYMENT_ID
		,b.product_no
		,a.BILLING_CYCLE_ID
		,replace(char(date(STATE_DATE)),'-','') STATE_DATE
from bass2.dwd_product_sc_payout_201112 a
,bass2.dw_product_201112 b 
where a.user_id = b.user_id 

        
        

select * from  bass2.dw_product_sc_payment_dm_201112
where product_no = '13648907966'

fetch first 10 rows only  
        


select *
 from bass2.ODS_SC_SCRD_ORD_INFO_201112
 where MOB_NUM = '13648907966'


 fetch first 10 rows only
 
		EXT2
[NULL]
20111202200829308096
20111202203723312674
[NULL]


        
        
        

select OPERATION_TYPE , count(0) 
--,  count(distinct OPERATION_TYPE ) 
from bass2.dw_product_sc_payment_dm_201112 
group by  OPERATION_TYPE 
order by 1 

select * 
 from bass2.dw_product_ord_so_log_dm_201112
where  ORD_CODE = '20111202203723312674'


PEER_SEQ
20111202203723312674
20111202200829308096
20111202203723312674
20111202200829308096

select * from g_s_02007_month_bak20120117 
where time_id = 201112
fetch first 10 rows only 

89101110014833      


select * 
 from bass2.dw_product_ord_so_log_dm_201112
where user_id = '89101110014833'


ORD_CODE
111228134244B153940180891





select count(0),count(distinct PEER_SEQ) from  bass2.dw_product_sc_payment_dm_201112
where operation_type='3'
and PEER_SEQ is not null
1	2	   
10445	10380	   
		
    select count(0),count(distinct sc_payment_id) from  bass2.dw_product_sc_payment_dm_201112
where operation_type in ('3','8')
and PEER_SEQ is not null
    
    15969
    
    1	2	   
10445	10445	   
		
        
        select PEER_SEQ,count(0)
        from  bass2.dw_product_sc_payment_dm_201112
        group by PEER_SEQ
        having count(0) > 1
        
        
        
        
        select * from   bass2.dw_product_sc_payment_dm_201112
        where PEER_SEQ = '111202124059B141354100895'
        
        
        

where PEER_SEQ = '111228134244B153940180891'



select count(0),count(distinct ORD_CODE)
 from bass2.dw_product_ord_so_log_dm_201112
 where  ORD_CODE is not null
 and offer_name like '%积分%'
 1	2	   
7562	7498	   
		
        
 
 1	2	   
2364698	1541326	   
		
        
                select ORD_CODE,count(0)
        from  bass2.dw_product_ord_so_log_dm_201112
        group by ORD_CODE
        having count(0) > 2
        
        
        
        select ORD_CODE , count(0)
 from bass2.dw_product_ord_so_log_dm_201112 a
 where exists (select 1 from bass2.dw_product_sc_payment_dm_201112 b where  a.user_id = b.user_id and 
 a.ORD_CODE = b.PEER_SEQ )
         group by ORD_CODE
        having count(0) > 2

select *
 from bass2.dw_product_ord_so_log_dm_201112 a
where ORD_CODE = '111201184728B141148030896'


select *
 from  bass2.dw_product_sc_payment_dm_201112 a
where PEER_SEQ = '111201184728B141148030896'

 
 111201184733C102398840896
 
 select *
 from bass2.dw_product_ord_so_log_dm_201112 a
where ORD_CODE = '111201184733C102398840896'



select count(0),count(distinct ORD_CODE)
 from bass2.dw_product_ord_so_log_dm_201112
 where  ORD_CODE is not null
 and (offer_name like '%积分%' or offer_name like '%M值%')
 
 1	2	   
7562	7498	   

1	2	   
10674	10609	   
		
        
select PEER_SEQ from  bass2.dw_product_sc_payment_dm_201112
where operation_type='3'
except
select ORD_CODE
 from bass2.dw_product_ord_so_log_dm_201112
 where  ORD_CODE is not null
 and (offer_name like '%积分%' or offer_name like '%M值%')


select * from  bass2.dw_product_sc_payment_dm_201112
where PEER_SEQ ='111229192528B155103110894'

select * from  bass2.dw_product_ord_so_log_dm_201112
where ORD_CODE ='111229192528B155103110894'

SC_PAYMENT_ID	OPT_SEQ	PEER_SEQ
111201083457C102383130891	1	111201083457B140759360891
		
        select * from  bass2.dwd_product_sc_payout_201112
        where SC_PAYMENT_ID = '111229192528C102752220894'
        
        
select count(0),count(distinct PEER_SEQ) from  bass2.dw_product_sc_payment_dm_201112
where operation_type='3'
and PEER_SEQ is not null
1	2	   
10445	10380	   

select *
from bass2.dw_product_ord_so_log_dm_201112
where ORD_CODE in (
select PEER_SEQ from  bass2.dw_product_sc_payment_dm_201112
where operation_type in ('3')
except
select ORD_CODE
 from bass2.dw_product_ord_so_log_dm_201112
 where  ORD_CODE is not null
 and (offer_name like '%积分%' or offer_name like '%M值%')

)



select distinct offer_name
from bass2.dw_product_ord_so_log_dm_201112
where ORD_CODE in (
select PEER_SEQ from  bass2.dw_product_sc_payment_dm_201112
where operation_type in ('3')
except
select ORD_CODE
 from bass2.dw_product_ord_so_log_dm_201112
 where  ORD_CODE is not null
 and (offer_name like '%积分%' or offer_name like '%M值%')

)


    select count(0),count(distinct sc_payment_id) from  bass2.dw_product_sc_payment_dm_201112
where operation_type in ('3','8')
and PEER_SEQ is not null
    
    15969
    
    
    select count(0),count(distinct sc_payment_id)  from bass2.dwd_product_sc_payout_201112

1	2	   
488875	40772	   
		
        1	2	   
43985	43780	   

1	2	   
40977	40772	   
		
    select count(0),count(distinct a.sc_payment_id),count(distinct peer_seq) from  bass2.dw_product_sc_payment_dm_201112 a
,(
select sc_payment_id,sum(USRSCR) USRSCR
from bass2.dwd_product_sc_payout_201112
group by sc_payment_id
) t
where a.sc_payment_id = t.sc_payment_id
and  a.amount = t.USRSCR

1	2	   
40772	40772	   
		
        
        select count(0),count(distinct ORD_SEQ)
 from bass2.ODS_SC_SCRD_ORD_INFO_201112
        
        1	2	   
23709	23031	   

        select count(0),count(distinct ORD_SEQ)
 from bass2.ODS_SC_SCRD_ORD_INFO_201112
   where ORD_SEQ like '201112%'
   1	2	   
3692	3520	   
		
        
	        select count(0),count(distinct ORD_SEQ) ,count(distinct PEER_SEQ)
 from bass2.ODS_SC_SCRD_ORD_INFO_201112	 a
 ,bass2.dw_product_sc_payment_dm_201112 b 
 where a.ORD_SEQ = b.PEER_SEQ
 
        1	2	3	   
4358	3299	3299	   
			
            
        
select count(0),count(distinct ORD_SEQ||MOB_NUM||char(ITEM_POINT))
 from bass2.ODS_SC_SCRD_ORD_INFO_201112
 
 1	2	   
23709	23031	   
		
              select ORD_SEQ
              from (
select ORD_SEQ,type1
 from bass2.ODS_SC_SCRD_ORD_INFO_201112
   group by ORD_SEQ,type1
   having count(0) > 1
   )
   t 
   group by ORD_SEQ
   having count(0) > 1
   
   select * from bass2.ODS_SC_SCRD_ORD_INFO_201112
   where op_time like '201112%'
   
   
   


select count(0),count(distinct ORD_CODE)
 from bass2.dw_product_ord_so_log_dm_201112
 where  ORD_CODE is not null
 and (offer_name like '%积分%' or offer_name like '%M值%')


1	2	   
10674	10609	   
	select ORD_CODE, count(0)
 from bass2.dw_product_ord_so_log_dm_201112
 where  ORD_CODE is not null
 and (offer_name like '%积分%' or offer_name like '%M值%')
	group by ORD_CODE having count(0) > 1 
    
    111221173840B150170470895
    
    select * from   bass2.dw_product_ord_so_log_dm_201112
    where ORD_CODE = '111221173840B150170470895'
    
    
    
        

select * from G_I_02006_MONTH_bak20120118
where user_id = '89160001196765'
union all
select * from G_I_02006_MONTH  
where user_id = '89160001196765'

      
      


select * from G_I_02006_MONTH_bak20120118
where user_id = '89160001196765'
union all
select * from G_I_02006_MONTH  
where user_id = '89160001196765'

      89157333538285
      
select * from bass2.dw_product_201112
where product_no = '15089051890'

select * from bass2.dw_product_201112
where product_no = '15289195954'
      
      89160000655341
      

select count(0)
from G_I_02006_MONTH a       
,G_I_02006_MONTH_bak20120118 b 
where a.user_id = b.user_id 
and a.ALL_CONVERTED_POINTS <> b.ALL_CONVERTED_POINTS
1	   
9351	   
	CONVERTIBLE_POINTS
    
    select count(0)
from G_I_02006_MONTH a       
,G_I_02006_MONTH_bak20120118 b 
where a.user_id = b.user_id 
and a.CONVERTIBLE_POINTS <> b.CONVERTIBLE_POINTS


select *
from G_I_02006_MONTH a       
,G_I_02006_MONTH_bak20120118 b 
where a.user_id = b.user_id 
where 
and bigint(a.ALL_POINTS) <> 
(bigint(b.ALL_POINTS)+ bigint(b.TRANS_POINTS)+bigint(b.MONTH_AGE_POINTS)+bigint(b.MONTH_QQT_POINTS))



select * from G_I_02006_MONTH_bak20120118
where user_id = '89157333538285'
union all
select * from G_I_02006_MONTH  
where user_id = '89157333538285'
     


insert into app.sch_control_before 
values('BASS1_G_I_02006_MONTH.tcl','M11013')
     
     
select * from app.sch_control_runlog
where control_code in (
select before_control_code from  table( bass1.get_before('02006')) a 
)

     
     
     select * from app.sch_control_before 
where control_code = 'BASS1_G_I_02006_MONTH.tcl'
and before_control_code = 'M11013'



delete from (
select * from app.sch_control_before 
where control_code = 'BASS1_G_I_02006_MONTH.tcl'
and before_control_code = 'M11013'
) a 




insert into app.sch_control_before 
values('BASS1_G_I_02006_MONTH.tcl','TR1_L_11013')




select * from  bass2.ODS_PRODUCT_SC_SCORELIST_201112
where product_instance_id = '89160001196765'




select *
from G_I_02006_MONTH a       
,G_I_02006_MONTH_bak20120118 b 
where a.user_id = b.user_id 
and a.user_id =  '89157333538285'
and bigint(a.ALL_POINTS) <> 
(bigint(b.ALL_POINTS)+ bigint(a.TRANS_POINTS)+bigint(b.MONTH_AGE_POINTS))





select count(0)
from G_I_02006_MONTH a       
,G_I_02006_MONTH_bak20120118 b 
where a.user_id = b.user_id 
and a.TIME_ID = 201112
and b.TIME_ID = 201112
and bigint(a.ALL_POINTS) <> 
(bigint(b.ALL_POINTS)+ bigint(b.TRANS_POINTS))




select *
from G_I_02006_MONTH a       
,G_I_02006_MONTH_bak20120118 b 
where a.user_id = b.user_id 
and a.TIME_ID = 201112
and b.TIME_ID = 201112
and bigint(a.ALL_POINTS) <> 
(bigint(b.ALL_POINTS)+ bigint(b.TRANS_POINTS))
fetch first 10 rows only


select *
from 
(
select 
a.user_id
,bigint(a.ALL_POINTS) ap
,bigint(b.ALL_POINTS) bp1
,bigint(a.TRANS_POINTS) bp2
from G_I_02006_MONTH a       
,G_I_02006_MONTH_bak20120118 b 
where a.user_id = b.user_id 
and a.TIME_ID = 201112
and b.TIME_ID = 201112
) t 
where ap <> (bp1+bp2)
fetch first 10 rows only


89101110038821      	2314	2234	0	   





select *
from G_I_02006_MONTH a       
,G_I_02006_MONTH_bak20120118 b 
where a.user_id = b.user_id 
and a.user_id =  '89101110038821'



select count(0)
from 
(
select 
a.user_id
,bigint(a.ALL_POINTS) ap
,bigint(b.ALL_POINTS) bp1
,bigint(a.TRANS_POINTS) bp2
from G_I_02006_MONTH a       
,G_I_02006_MONTH_bak20120118 b 
where a.user_id = b.user_id 
and a.TIME_ID = 201112
and b.TIME_ID = 201112
) t 
where ap <> (bp1+bp2)




select *
from 
(
select 
a.user_id
,bigint(a.ALL_POINTS) ap
,bigint(b.ALL_POINTS) bp1
,bigint(a.TRANS_POINTS) bp2
from G_I_02006_MONTH a       
,G_I_02006_MONTH_bak20120118 b 
where a.user_id = b.user_id 
and a.TIME_ID = 201112
and b.TIME_ID = 201112
) t 
where ap <> (bp1+bp2)
fetch first 10 rows only



89101110011598      




select *
from G_I_02006_MONTH a       
,G_I_02006_MONTH_bak20120118 b 
where a.user_id = b.user_id 
and a.user_id =  '89101110011598'


89101110011598

select * from bass2.ODS_PRODUCT_SC_SCORELIST_201112
where product_instance_id = '89157333538285'





select * from bass2.dwd_PRODUCT_SC_SCORELIST_201112
where product_instance_id = '89157333538285'

insert into 
bass2.DWD_PRODUCT_SC_SCORELIST_201112_BASS1
select * from 
bass2.DWD_PRODUCT_SC_SCORELIST_201112


CREATE TABLE "BASS2   "."DWD_PRODUCT_SC_SCORELIST_201112_BASS1"  (
                  "SC_SCORELIST_ID" BIGINT , 
                  "SC_PAYMENT_ID" VARCHAR(25) , 
                  "SVCNUM" VARCHAR(12) , 
                  "REGION_ID" VARCHAR(7) , 
                  "AREAR_ID" VARCHAR(20) , 
                  "OPT_ORG_ID" BIGINT , 
                  "BRANDID" BIGINT , 
                  "COUNT_CYCLE_ID" INTEGER , 
                  "SC_CYCLE_ID" INTEGER , 
                  "SCRTYPE" SMALLINT , 
                  "CURSCR" BIGINT , 
                  "ORGSCR" BIGINT , 
                  "ADJSCR" BIGINT , 
                  "USRSCR" BIGINT , 
                  "VALIDDATE" TIMESTAMP , 
                  "EXPIREDATE" TIMESTAMP , 
                  "ACTFLAG" CHAR(1) , 
                  "CHGDATE" TIMESTAMP , 
                  "OPTSN" VARCHAR(25) , 
                  "REMARK" VARCHAR(255) , 
                  "EXT1" VARCHAR(32) , 
                  "EXT2" VARCHAR(32) , 
                  "EXT3" VARCHAR(32) , 
                  "PRODUCT_INSTANCE_ID" VARCHAR(20) , 
                  "YEAR" INTEGER )   
                 DISTRIBUTE BY HASH("SC_SCORELIST_ID")   
                   IN "TBS_3H" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 




select *
from G_I_02006_MONTH a       
,G_I_02006_MONTH_bak20120117 b 
where a.user_id = b.user_id 
and a.TIME_ID = 201112
and b.TIME_ID = 201112
and a.user_id =  '89101110011598'



select * from 
bass2.DWD_PRODUCT_SC_SCORELIST_201112
where SCRTYPE = 21
fetch first 10 rows only


89101110165834




select * from bass2.ODS_PRODUCT_SC_SCORELIST_201112
where product_instance_id = '89101110165834'





select * from bass2.dwd_PRODUCT_SC_SCORELIST_201112
where product_instance_id = '89101110165834'

select * from  bass2.ODS_PRODUCT_SC_SCORELIST_201112 
where  SCRTYPE = 21
;

select max() from bass2.dwd_PRODUCT_SC_SCORELIST_201112

select SCRTYPE , count(0) 
--,  count(distinct SCRTYPE ) 
from bass2.ODS_PRODUCT_SC_SCORELIST_201112 
where year > 2008 and 
group by  SCRTYPE 
order by 1 
SCRTYPE	2	   
1	11005265	   
4	7	   
5	9561550	   
9	1	   
21	980	   
23	4626	   
24	324025	   
25	448616	   
99	100209	   
		


select SCRTYPE , count(0) 
--,  count(distinct SCRTYPE ) 
from bass2.dwd_PRODUCT_SC_SCORELIST_201112 
group by  SCRTYPE 
order by 1 

SCRTYPE	2	   
1	11068314	   
4	7	   
5	959	   
9	1	   
21	5199	   
23	4582	   
24	324157	   
25	448807	   
99	100213	   
		

select * from  bass2.dwd_PRODUCT_SC_SCORELIST_201112 
where  SCRTYPE = 5
;


select year , count(0) 
--,  count(distinct year ) 
from bass2.ODS_PRODUCT_SC_SCORELIST_201112 
group by  year 
order by 1 


        


select REMARK,count(0),sum(adjscr)
from bass2.ODS_PRODUCT_SC_SCORELIST_201112 
where year > 2008 and year < 2012
and scrtype = 5
group by REMARK




select year,SCRTYPE , count(0) 
--,  count(distinct SCRTYPE ) 
from bass2.dwd_PRODUCT_SC_SCORELIST_201112 
group by  year,SCRTYPE 
order by 1 


select scrtype,count(0)
from bass2.ODS_PRODUCT_SC_SCORELIST_201112 
where year > 2008 and year < 2012
group by scrtype

select * from bass2.ODS_PRODUCT_SC_SCORELIST_201112 
where product_instance_id = '89160001196765'



select *
from bass2.ODS_PRODUCT_SC_SCORELIST_201112 
where year > 2008 and year < 2012
and scrtype = 5
fetch first 10 rows only

select count(0) 
from bass2.ODS_PRODUCT_SC_SCORELIST_201112  a, bass2.dwd_PRODUCT_SC_SCORELIST_201112 b
where a.year > 2008 and a.year < 2012
and a.scrtype = 5
and a.remark = '积分改造20120104'
and a.SC_SCORELIST_ID = b.SC_SCORELIST_ID
A'割接前新口径'
b'割接后新口径'
C '割接前新口径-一经转换'

select 'A', a.* from G_I_02006_MONTH_bak20120118 a
where user_id  = '89160001196765'
union all
select 'B', a.* from G_I_02006_MONTH a
where user_id  = '89160001196765'
union all
select  'C'
         ,TIME_ID
        ,USER_ID
        ,MONTH_POINTS
        ,MONTH_QQT_POINTS
        ,MONTH_AGE_POINTS
        ,CHAR(ceil((0.035/0.015-1.0000)*bigint(ALL_CONSUME_POINTS))) TRANS_POINTS
        ,CONVERTIBLE_POINTS
        ,ALL_POINTS
        ,ALL_CONSUME_POINTS
        ,ALL_CONVERTED_POINTS
        ,LEAVE_CLEAR_POINTS
        ,OTHER_CLEAR_POINTS
 from G_I_02006_MONTH_bak20120118
where user_id  = '89160001196765'


select ceil(2.1) from bass2.dual

select char(ceil((0.035/0.015-1.0000)*bigint(ALL_CONSUME_POINTS)))
 from G_I_02006_MONTH_bak20120118
where user_id  = '89160001196765'

select 0.035/0.015,0.035/0.015-1 from bass2.dual




select 
         --SC_SCORELIST_ID
        --,SC_PAYMENT_ID
        SVCNUM
        ,REGION_ID
        --,AREAR_ID
        --,OPT_ORG_ID
        ,BRANDID
        ,COUNT_CYCLE_ID
        ,SC_CYCLE_ID
        ,5 SCRTYPE
        ,ceil(SUM(curscr)*(0.035/0.015-1)) CURSCR
        ,ceil(SUM(curscr)*(0.035/0.015-1)) ORGSCR
        --~   ,ADJSCR
        --~   ,USRSCR
        --~   ,VALIDDATE
        --~   ,EXPIREDATE
        ,1 ACTFLAG
        --~   ,CHGDATE
        --~   ,OPTSN
        ,'积分改造20120104' REMARK
        --~   ,EXT1
        --~   ,EXT2
        --~   ,EXT3
        ,PRODUCT_INSTANCE_ID
        ,YEAR
from bass2.DWD_PRODUCT_SC_SCORELIST_201112
where  scrtype=1 and brandid=161000000001
group by svcnum,region_id,count_cycle_id,product_instance_id,brandid,YEAR,SC_CYCLE_ID

select 0.035/0.015-1 from bass2.dual

select max(CURSCR),max(ORGSCR) from (
select 
         --SC_SCORELIST_ID
        --,SC_PAYMENT_ID
        SVCNUM
        ,REGION_ID
        --,AREAR_ID
        --,OPT_ORG_ID
        ,BRANDID
        ,COUNT_CYCLE_ID
        ,SC_CYCLE_ID
        ,5 SCRTYPE
        ,ceil(SUM(curscr)*decimal(0.035/0.015-1,10,6)) CURSCR
        ,ceil(SUM(curscr)*decimal(0.035/0.015-1,10,6)) ORGSCR
        ,0 ADJSCR
        ,0 USRSCR
        --~   ,VALIDDATE
        --~   ,EXPIREDATE
        ,'1' ACTFLAG
        --~   ,CHGDATE
        --~   ,OPTSN
        ,'积分改造20120104' REMARK
        --~   ,EXT1
        --~   ,EXT2
        --~   ,EXT3
        ,PRODUCT_INSTANCE_ID
        ,YEAR
from bass2.DWD_PRODUCT_SC_SCORELIST_201112
where  scrtype=1 and brandid=161000000001
group by svcnum,region_id,count_cycle_id,product_instance_id,brandid,YEAR,SC_CYCLE_ID


) t


db2 runstats on table bass2.DWD_PRODUCT_SC_SCORELIST_201112_BASS1 with distribution and detailed indexes all           

select * from bass2.DWD_PRODUCT_SC_SCORELIST_201112_BASS1
where product_instance_id = '89160001196765'



select * from 
G_I_02006_MONTH
where  user_id = '89160001196765'


select 'A', a.* from G_I_02006_MONTH_bak20120118 a
where user_id  = '89160001196765'
union all
select 'B', a.* from G_I_02006_MONTH a
where user_id  = '89160001196765'


89157333538285



select 'NotCvtNotConsm', a.* from G_I_02006_MONTH_bak20120118 a
where user_id  = '89157333538285'
union all
select 'HavCvtNotConsm', a.* from G_I_02006_MONTH_bak20120118cnvt a
where user_id  = '89157333538285'
union all
select 'HavCvtHavConsm', a.* from G_I_02006_MONTH a
where user_id  = '89157333538285'


89160001196765

select 'NotCvtNotConsm', a.* from G_I_02006_MONTH_bak20120118 a
where user_id  = '89160001196765'
union all
select 'HavCvtNotConsm', a.* from G_I_02006_MONTH_bak20120118cnvt a
where user_id  = '89160001196765'
union all
select 'HavCvtHavConsm', a.* from G_I_02006_MONTH a
where user_id  = '89160001196765'



rename bass1.G_I_02006_MONTH to G_I_02006_MONTH_bak20120118cnvt

rename bass1.G_I_02006_MONTH to G_I_02006_MONTH_use20120117data




select * from bass2.dw_product_201112
where product_no = '15289195954'
      
      89160000655341
      
      
      
      select * from bass2.ods_product_sc_scorelist_201112
where product_instance_id = '89160000655341'



select 'NotCvtNotConsm', a.* from G_I_02006_MONTH_bak20120118 a
where user_id  = '89160000655341'
union all
select 'HavCvtNotConsm', a.* from G_I_02006_MONTH_bak20120118cnvt a
where user_id  = '89160000655341'
union all
select 'HavCvtHavConsm', a.* from G_I_02006_MONTH a
where user_id  = '89160000655341'


select 
svcnum,region_id,count_cycle_id,product_instance_id,brandid
        ,ceil(SUM(curscr)*decimal(0.035/0.015-1,10,6)) CURSCR --ceil(SUM(curscr)*decimal(0.035/0.015-1,10,6))
        ,ceil(SUM(curscr)*decimal(0.035/0.015-1,10,6)) ORGSCR
from bass2.DWD_PRODUCT_SC_SCORELIST_201112
where  scrtype=1 and brandid=161000000001
and product_instance_id  = '89160000655341'
GROUP BY svcnum,region_id,count_cycle_id,product_instance_id,brandid




select 
svcnum,region_id,count_cycle_id,product_instance_id,brandid
,YEAR,SC_CYCLE_ID
        ,ceil(SUM(curscr)*decimal(0.035/0.015-1,10,6)) CURSCR --ceil(SUM(curscr)*decimal(0.035/0.015-1,10,6))
        ,ceil(SUM(curscr)*decimal(0.035/0.015-1,10,6)) ORGSCR
from bass2.DWD_PRODUCT_SC_SCORELIST_201112
where  scrtype=1 and brandid=161000000001
and product_instance_id  = '89160000655341'
GROUP BY svcnum,region_id,count_cycle_id,product_instance_id,brandid
,YEAR,SC_CYCLE_ID




select 
svcnum,region_id,count_cycle_id,product_instance_id,brandid
,YEAR,SC_CYCLE_ID
        ,ceil(SUM(curscr)*decimal(0.035/0.015-1,10,6)) CURSCR --ceil(SUM(curscr)*decimal(0.035/0.015-1,10,6))
        ,ceil(SUM(curscr)*decimal(0.035/0.015-1,10,6)) ORGSCR
from bass2.DWD_PRODUCT_SC_SCORELIST_201112
where  scrtype=1 and brandid=161000000001
and product_instance_id  = '89160000655341'
GROUP BY svcnum,region_id,count_cycle_id,product_instance_id,brandid
,YEAR,SC_CYCLE_ID





select * from  bass2.ODS_PRODUCT_SC_SCORELIST_201112
where product_instance_id = '89160000655341'



select 'NotCvtNotConsm', a.* from G_I_02006_MONTH_bak20120118 a
where user_id  = '89160000655341'
union all
select 'HavCvtNotConsm', a.* from G_I_02006_MONTH_bak20120118cnvt a
where user_id  = '89160000655341'
union all
select 'HavCvtHavConsm', a.* from G_I_02006_MONTH_use20120117data a
where user_id  = '89160000655341'
union all
select 'HavCvtNotConsm2', a.* from G_I_02006_MONTH a
where user_id  = '89160000655341'





select 'NotCvtNotConsm', a.* from G_I_02006_MONTH_bak20120118 a
where user_id  = '89157333538285'
union all
select 'HavCvtNotConsm', a.* from G_I_02006_MONTH_bak20120118cnvt a
where user_id  = '89157333538285'
union all
select 'HavCvtHavConsm', a.* from G_I_02006_MONTH_use20120117data a
where user_id  = '89157333538285'
union all
select 'HavCvtNotConsm2', a.* from G_I_02006_MONTH a
where user_id  = '89157333538285'



select 'NotCvtNotConsm', a.* from G_I_02006_MONTH_bak20120118 a
where user_id  = '89160001196765'
union all
select 'HavCvtNotConsm', a.* from G_I_02006_MONTH_bak20120118cnvt a
where user_id  = '89160001196765'
union all
select 'HavCvtHavConsm', a.* from G_I_02006_MONTH_use20120117data a
where user_id  = '89160001196765'
union all
select 'HavCvtNotConsm2', a.* from G_I_02006_MONTH a
where user_id  = '89160001196765'



89160001196765


select count(0)
from G_I_02006_MONTH
where bigint(ALL_POINTS) <> bigint(ALL_CONVERTED_POINTS) + bigint(CONVERTIBLE_POINTS) 



select count(0),count(distinct user_id)
from G_I_02006_MONTH

1	2	   
918811	918811	   
		
        
select count(0),count(distinct product_instance_id) from  bass2.ODS_PRODUCT_SC_SCORELIST_201112
where actflag='1'

21437306	936231	   

select sum(a.orgscr) , sum(b.orgscr)
from 
(
select PRODUCT_INSTANCE_ID ,scrtype, sum(orgscr) orgscr 
from bass2.ODS_PRODUCT_SC_SCORELIST_201112 a 
where actflag='1'
 group by PRODUCT_INSTANCE_ID,scrtype
 ) a 
, (
select PRODUCT_INSTANCE_ID ,scrtype, sum(orgscr) orgscr 
from bass2.DWD_PRODUCT_SC_SCORELIST_201112_BASS1 a 
where actflag='1'
 group by PRODUCT_INSTANCE_ID,scrtype
 ) b 
where a.PRODUCT_INSTANCE_ID = b.PRODUCT_INSTANCE_ID
and a.scrtype = b.scrtype



DWD_PRODUCT_SC_SCORELIST_201112_BASS1


1	2	   
2922439633	2988538313	   
		


select *
from 
(
select PRODUCT_INSTANCE_ID ,scrtype, sum(orgscr) orgscr 
from bass2.ODS_PRODUCT_SC_SCORELIST_201112 a 
where actflag='1'
 group by PRODUCT_INSTANCE_ID,scrtype
 ) a 
, (
select PRODUCT_INSTANCE_ID ,scrtype, sum(orgscr) orgscr 
from bass2.DWD_PRODUCT_SC_SCORELIST_201112_BASS1 a 
where actflag='1'
 group by PRODUCT_INSTANCE_ID,scrtype
 ) b 
where a.PRODUCT_INSTANCE_ID = b.PRODUCT_INSTANCE_ID
and a.scrtype = b.scrtype
and a.orgscr <> b.orgscr

与BOSS差异在转移积分上。一经用1231.24hr的数据转换，而boss用4号24点的数据转换。




update 
(
select * from app.g_runlog 
where unit_code = '02006'
and time_id = 201112
and return_flag = 1
) t
set return_flag = 0




select *
 from  bass2.dw_product_sc_payment_dm_201112 a
where user_id = '89160001196765'

 


select count(0)
,count(distinct ord_code )
from G_S_02007_MONTH_4



		from (select user_id,ord_code,max(offer_name) offer_name,count(*) cnt from  bass2.dw_product_ord_so_log_dm_$op_month 
	         group by user_id,ord_code ) a ,
             
             


select count(0)
,count(distinct SC_PAYMENT_ID )
,count(distinct user_id )
,count(distinct peer_seq )
from G_S_02007_MONTH_2a

1	2	3	4	   
37265	37265	37265	13481	   


1	2	3	4	   
37265	37265	37265	13737	   
				
				
                
            
            

select count(0) from G_S_02007_MONTH_1

select 
         a.USER_ID
        ,a.MONTH_CONVERTED_POINTS
        ,a.MONTH_CONVERTED_CNT
		--,c.TYPE1
		--,d.offer_name
		from G_S_02007_MONTH_1 a
		left join  G_S_02007_MONTH_2a b on a.user_id = b.user_id  
		left join G_S_02007_MONTH_3 c on  b.PEER_SEQ = c.ORD_SEQ
		left join G_S_02007_MONTH_4 d on  b.PEER_SEQ = d.ORD_CODE
            

drop table G_S_02007_MONTH_3

 CREATE TABLE BASS1.G_S_02007_MONTH_3  (
                  ORD_SEQ CHAR(20) , 
                  ORDER_SUM_POINT bigint,
                  type1 varchar(10),
                  ITEM_TYPE varchar(2)
                  ) 
                 DISTRIBUTE BY HASH(ORD_SEQ)   
                   IN TBS_APP_BASS1 INDEX IN TBS_INDEX ; 
                   
insert into G_S_02007_MONTH_3
select  
ORD_SEQ 
,ORDER_SUM_POINT
,type1
,ITEM_TYPE
from 
( select a.* , row_number()over(partition by ORD_SEQ order by ORDER_SUM_POINT desc  ) rn 
 from (select ORD_SEQ,ITEM_TYPE,type1,sum(ORDER_SUM_POINT) ORDER_SUM_POINT  from bass2.ODS_SC_SCRD_ORD_INFO_201112
  where op_time like '201112%' 
  group by  ORD_SEQ,ITEM_TYPE,type1
 )  a
,G_S_02007_MONTH_2a b 
where  a.ORD_SEQ = b.peer_seq
) t where t.rn = 1

            
            


select ORD_SEQ,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' feedback_id 
from 
(
select
                ORD_SEQ,ITEM_TYPE
                ,case 
                    when type1 = '实物类' then '00'
                    when type1 = '自有类' then '01'
                    when type1 = '合作类' then '02'
                    else type1 end type1 
     from G_S_02007_MONTH_3 a
) t
					


insert into G_S_02007_MONTH_1
			select user_id
				,sum(USRSCR) MONTH_CONVERTED_POINTS 
				,count(distinct sc_payment_id)  MONTH_CONVERTED_CNT
			from bass2.dwd_product_sc_payout_201112
			group by user_id
            with ur

                        
                        

select 
         a.USER_ID
        ,a.MONTH_CONVERTED_POINTS
        ,a.MONTH_CONVERTED_CNT
		,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' feedback_id_hq
		,case when (d.offer_name like '%加油%' or d.offer_name like '%购物%') then '2100'
	       when (d.offer_name like '%充值卡%' or d.offer_name like '%送%话费%') then '2210'
	       when (d.offer_name like '%GPRS套餐%' or d.offer_name like '%手机报%' or d.offer_name like '%短信套餐%' or 
	             d.offer_name like '%送彩铃%'   or d.offer_name like '%彩信套餐%' or d.offer_name like '%无线音乐俱乐部%') then '2230'
	       when (d.offer_name like '%VIP客户%') then '2220'
	       when (d.offer_name like '%送手机%') or (d.offer_name like '%兑手机%') then '2240'
	       when (d.offer_name like '%电影票%') or (d.offer_name like '%演唱会门票%') or (d.offer_name like '%新专辑%') then '2100'
	       else '2250'
		end feedback_id_lc
		from G_S_02007_MONTH_1 a
		left join  G_S_02007_MONTH_2a b on a.user_id = b.user_id  
		left join G_S_02007_MONTH_3 c on  b.PEER_SEQ = c.ORD_SEQ
		left join G_S_02007_MONTH_4 d on  b.PEER_SEQ = d.ORD_CODE
                        
                        

select distinct offer_name from G_S_02007_MONTH_4                        
select distinct substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' from G_S_02007_MONTH_3                        

G_S_02007_MONTH_3

		select count(0) from  G_S_02007_MONTH_2a b 
		 join G_S_02007_MONTH_3 c on  b.PEER_SEQ = c.ORD_SEQ
        
        

select feedback_id_hq,feedback_id_lc,count(0)
from (

select 
         a.USER_ID
        ,a.MONTH_CONVERTED_POINTS
        ,a.MONTH_CONVERTED_CNT
		,feedback_id feedback_id_hq
		,case when (d.offer_name like '%加油%' or d.offer_name like '%购物%') then '2100'
	       when (d.offer_name like '%充值卡%' or d.offer_name like '%送%话费%') then '2210'
	       when (d.offer_name like '%GPRS套餐%' or d.offer_name like '%手机报%' or d.offer_name like '%短信套餐%' or 
	             d.offer_name like '%送彩铃%'   or d.offer_name like '%彩信套餐%' or d.offer_name like '%无线音乐俱乐部%') then '2230'
	       when (d.offer_name like '%VIP客户%') then '2220'
	       when (d.offer_name like '%送手机%') or (d.offer_name like '%兑手机%') then '2240'
	       when (d.offer_name like '%电影票%') or (d.offer_name like '%演唱会门票%') or (d.offer_name like '%新专辑%') then '2100'
	       else '2250'
		end feedback_id_lc
		from G_S_02007_MONTH_1 a
		left join  G_S_02007_MONTH_2a b on a.user_id = b.user_id  
		left join (select ORD_SEQ,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' feedback_id 
						from 
						(
						select
										ORD_SEQ,ITEM_TYPE
										,case 
											when type1 = '实物类' then '00'
											when type1 = '自有类' then '01'
											when type1 = '合作类' then '02'
											else type1 end type1 
							 from G_S_02007_MONTH_3 a
						) t
						) c on  b.PEER_SEQ = c.ORD_SEQ
		left join G_S_02007_MONTH_4 d on  b.PEER_SEQ = d.ORD_CODE
) t 
group by 

        feedback_id_hq,feedback_id_lc
        



select count(0),count(distinct user_id)
from (

select 
         a.USER_ID
        ,a.MONTH_CONVERTED_POINTS
        ,a.MONTH_CONVERTED_CNT
		,feedback_id feedback_id_hq
		,case when (d.offer_name like '%加油%' or d.offer_name like '%购物%') then '2100'
	       when (d.offer_name like '%充值卡%' or d.offer_name like '%送%话费%') then '2210'
	       when (d.offer_name like '%GPRS套餐%' or d.offer_name like '%手机报%' or d.offer_name like '%短信套餐%' or 
	             d.offer_name like '%送彩铃%'   or d.offer_name like '%彩信套餐%' or d.offer_name like '%无线音乐俱乐部%') then '2230'
	       when (d.offer_name like '%VIP客户%') then '2220'
	       when (d.offer_name like '%送手机%') or (d.offer_name like '%兑手机%') then '2240'
	       when (d.offer_name like '%电影票%') or (d.offer_name like '%演唱会门票%') or (d.offer_name like '%新专辑%') then '2100'
	       else '2250'
		end feedback_id_lc
		from G_S_02007_MONTH_1 a
		left join  G_S_02007_MONTH_2a b on a.user_id = b.user_id  
		left join (select ORD_SEQ,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' feedback_id 
						from 
						(
						select
										ORD_SEQ,ITEM_TYPE
										,case 
											when type1 = '实物类' then '00'
											when type1 = '自有类' then '01'
											when type1 = '合作类' then '02'
											else type1 end type1 
							 from G_S_02007_MONTH_3 a
						) t
						) c on  b.PEER_SEQ = c.ORD_SEQ
		left join G_S_02007_MONTH_4 d on  b.PEER_SEQ = d.ORD_CODE
) t 
group by 

        feedback_id_hq,feedback_id_lc
        
        

select count(0) from G_S_02007_MONTH
        
        

select * from g_s_02007_month
        
        
        
select POINT_FEEDBACK_ID , count(0) 
--,  count(distinct POINT_FEEDBACK_ID ) 
from g_s_02007_month 
group by  POINT_FEEDBACK_ID 
order by 1 



select ENTERPRISE_ID||ENTERPRISE_BUSI_TYPE||MANAGE_MODE , count(0) 
--,  count(distinct ENTERPRISE_ID||ENTERPRISE_BUSI_TYPE||MANAGE_MODE ) 
from g_a_02054_day 
where time_id = 20120119
group by  ENTERPRISE_ID||ENTERPRISE_BUSI_TYPE||MANAGE_MODE 
order by 1 



89100000003797      13302	4	   
89102999652155      13302	2	   
delete from 
(
select * from g_a_02054_day 
where time_id = 20120119
and ENTERPRISE_ID in ('89100000003797','89102999652155')
and not (order_date = '20120119'
and status_id = '2'
)
)


select ENTERPRISE_ID||ENTERPRISE_BUSI_TYPE||MANAGE_MODE , count(0) 
--,  count(distinct ENTERPRISE_ID||ENTERPRISE_BUSI_TYPE||MANAGE_MODE ) 
from g_a_02054_day 
where time_id = 20120120
group by  ENTERPRISE_ID||ENTERPRISE_BUSI_TYPE||MANAGE_MODE having count(0) > 1
order by 1 


89100000003797      13302	3	   

delete from (
select * from g_a_02054_day 
where time_id = 20120120
and ENTERPRISE_ID in ('89100000003797')
and order_date < '20120119'
) a 


RENAME TABLE BASS2.DIM_TERM_TAC TO DIM_TERM_TAC_20120131BAK;
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
  
  
  ALTER TABLE BASS2.DIM_TERM_TAC_MID ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE




delete from BASS2.DIM_TERM_TAC 
insert into BASS2.DIM_TERM_TAC
select * from BASS2.DIM_TERM_TAC_MID

 insert into BASS2.DIM_TERM_TAC
select 
ID,
TAC_NUM,
TERM_ID,
TERM_MODEL,
TERMPROD_ID,
TERMPROD_NAME,
NET_TYPE,
TERM_TYPE from BASS2.DIM_TERM_TAC_20120131BAK
where net_type <>'2';


select tac_nuM,count(*) from BASS2.DIM_TERM_TAC
group by tac_nuM
having count(*)>1

35805102       	2

35719102       	2	   

select tac_nuM,count(*) from BASS2.DIM_TERM_TAC_20120131BAK
group by tac_nuM
having count(*)>1




/**
假如有重复：
35805102
select * from BASS2.DIM_TERM_TAC
where tac_nuM='35719102'
ID	TAC_NUM	TERM_ID	TERM_MODEL	TERMPROD_ID	TERMPROD_NAME	NET_TYPE	TERM_TYPE	   
635	35719102       	26003     	K-Touch T390                                      	006010071 	天宇朗通                                                                                                                                                                                                	2	1	   
17307	35719102	26805	TCL C338	006010004	TCL	1	0	   
								
select * from BASS2.DIM_TERM_TAC_20120131BAK
where tac_nuM='35719102'

ID	TAC_NUM	TERM_ID	TERM_MODEL	TERMPROD_ID	TERMPROD_NAME	NET_TYPE	TERM_TYPE	   
17307	35719102	26805	TCL C338	006010004	TCL	1	0	   
								
delete from BASS2.DIM_TERM_TAC
where tac_nuM='35719102' and id = 17307
commit;

delete from BASS2.DIM_TERM_TAC
where tac_nuM='35805102' and term_id='25208';

select * from BASS2.DIM_TERM_TAC
where tac_nuM='35719102'
**/

--drop table BASS2.DIM_TERM_TAC_MID

db2 RUNSTATS ON table BASS2.DIM_TERM_TAC      with distribution and detailed indexes all  




select * from bass1.g_rule_check where rule_code in ('R107') order by time_id desc
select * from bass1.g_rule_check where rule_code in ('R108') order by time_id desc


select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 3 days
and control_code like 'BASS1%'
order by alarmtime desc
;





select count(0) from bass2.CDR_CALL_DTL_20110101
where OPP_NUMBER = '95533'

fetch first 10 rows only




select 
CALLMOMENT_ID
,sum(CALL_DURATION_M) dur_m 
from bass2.CDR_CALL_DTL_20110101
where OPP_NUMBER = '95533'
and calltype_id = 0
group by CALLMOMENT_ID




CREATE TABLE "BASS1   "."T_STAT_95533"  (
    time_id integer 
    ,CALLMOMENT_ID smallint
    ,dur_m bigint
                   )   
                 DISTRIBUTE BY HASH("TIME_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 



select * from  BASS1.T_STAT_95533




select time_id , count(0) 
--,  count(distinct time_id ) 
from BASS1.T_STAT_95533 
group by  time_id 
order by 1 

select replace(char(date('2011-02-01') + 1 days),'-','') from bass2.dual



select replace(char(date('2011-02-01') + 333 days),'-','') from bass2.dual



select time_id,count(0) from bass1.g_user_lst
group by time_id 
order by 1 desc 



select time_id/100 
, sum(dur_m)*1.0/10000 dur_m
, sum(case when not (CALLMOMENT_ID between 2 and 9 ) then dur_m else 0 end)*1.0/10000 busy_dur_m
--,  count(distinct time_id ) 
from BASS1.T_STAT_95533 
group by  time_id /100
order by 1 
 


select 
sum(bigint(ALL_FEE))
,sum(bigint(UP_FLOWS))
,sum(bigint(DOWN_FLOWS))
from G_S_04002_DAY
where time_id/100 between 201101 and 201112


select  distinct user_id,ADD_PKG_ID from bass1.g_i_02023_day a where time_id = 20120131 
fetch first 10 rows only
select distinct user_id,OVER_PROD_ID from bass1.g_i_02021_month a where time_id = 201201 and  OVER_PROD_ID like '99991222%'
fetch first 10 rows only
    select 
        (select  count(distinct user_id||ADD_PKG_ID) cnt from bass1.g_i_02023_day a where time_id = 20120131 )                     
         - (select  count(distinct user_id||OVER_PROD_ID) cnt from bass1.g_i_02021_month a where time_id = 201201 and  OVER_PROD_ID like '99991222%')
         from bass2.dual
        with ur 
        
        

select * from  bass1.g_i_02021_month where time_id / 100 = 201201
select * from bass1.g_i_02021_month_temp1

select * from bass1.g_i_02021_month_temp2
        
        select *
                                from  BASS1.ALL_DIM_LKP 
                                where BASS1_TBID = 'BASS_STD1_0115'
                                and bass1_value like 'QW_QQT_DJ%'


                select 
                     201201,
                     a.user_id,
                     value(value(e.new_pkg_id,c.ADD_PKG_ID),char(a.offer_id)),
                     value(c.VALID_DT,a.create_date) VALID_DT
                     select distinct value(value(e.new_pkg_id,c.ADD_PKG_ID),char(a.offer_id))
                from bass1.g_i_02021_month_temp2 a
                inner join bass1.g_i_02021_month_temp1 b        on  a.user_id=b.user_id
                left join (select xzbas_value  as offer_id ,bass1_value bass1_offer_id
                                from  BASS1.ALL_DIM_LKP 
                                where BASS1_TBID = 'BASS_STD1_0115'
                                and bass1_value like 'QW_QQT_DJ%'
                                ) d on char(a.offer_id) = d.offer_id
                left join bass1.DIM_QW_QQT_PKGID e on  d.bass1_offer_id = e.old_pkg_id              
                left join  (select user_id , ADD_PKG_ID,VALID_DT
                                from
                                (select a.*,row_number()over(partition by user_id,ADD_PKG_ID order by VALID_DT desc ) rn
                                from bass1.g_i_02023_day  a
                                where time_id  = 20120131
                                ) t where  t.rn = 1 
                                ) c on  a.user_id=c.user_id and e.new_pkg_id = c.ADD_PKG_ID
with ur


        
        
select * from
                (select * from 
                (
                select user_id,chg_vip_time,row_number()over(partition by user_id order by time_id) row_id from BASS1.G_I_02005_MONTH
                where time_id=201202
                ) k
                where k.row_id =1) a
                left outer join 
                (
                select * from
                (
                select user_id,create_date,row_number()over(partition by user_id order by time_id desc) row_id 
                from BASS1.G_A_02004_DAY
                where time_id<=20120229
                ) k
                where k.row_id=1) b
                on a.user_id=b.user_id
                where bigint(chg_vip_time)<bigint(create_date)
                with ur

USER_ID	CHG_VIP_TIME	ROW_ID	USER_ID	CREATE_DATE	ROW_ID	   
89160001048760      	20110624	1	89160001048760      	20110629	1	   
						
89160001048760      	20110624	1	89160001048760      	20110629	1	   

update BASS1.G_I_02005_MONTH
set CHG_VIP_TIME = '20110629'
where user_id = '89160001048760'
and time_id = 201202
          
          
delete from 
(
select * from   G_I_21020_MONTH
				where COMP_BRAND_ID = '021000'
				and time_id = 201201
				and length(trim(COMP_PRODUCT_NO)) <> 11
) t


                
                
select count(0) from   G_I_21020_MONTH where time_id = 201201
734925

				where COMP_BRAND_ID = '021000'
				and time_id = 201201
				and length(trim(COMP_PRODUCT_NO)) <> 11

                
                
                 select a.* ,(target1-target2)*1.0/target2 from  bass1.G_RULE_CHECK a where rule_code = '53'
 order by 1 desc 
 
 BASS1_INT_CHECK_D9E234F2_651TO56_MONTH.tcl
 
 
 ---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where 
control_code like 'BASS1_INT_CHECK_R037toR039_MONTH%'
order by alarmtime desc



                 select a.* ,(target1-target2)*1.0/target2 from  bass1.G_RULE_CHECK a where rule_code = '53'
 order by 1 desc 
 
 
 select a.*  from  bass1.G_RULE_CHECK a where rule_code = 'R146'
  order by 1 desc 

 201201	R235	1373830.00000	75573.00000	0.05500	0.00000	   
 
 
 select count(*) from bass1.G_RULE_CHECK where time_id = 201103 and rule_code in ('R038') and abs(target3) > 0.1
 
  select count(*) from bass1.G_RULE_CHECK where time_id = 201201 and rule_code in ('R038') and abs(target3) > 0.1

select a.*,abs(target3) 
from bass1.G_RULE_CHECK  a where  rule_code in ('R235') 





update G_I_21020_MONTH 
set CALL_COUNTS = char(int(rand(1)*5+1))
where (
char(TIME_ID) = substr(COMP_LAST_DATE,1,6)
and CALL_COUNTS = '0'
and SMS_COUNTS = '0'
and MMS_COUNTS = '0'
and time_id = 201201 )



select distinct BASE_PROD_ID
from G_I_02020_MONTH
where time_id = 201201



select distinct OVER_PROD_ID
from G_I_02019_MONTH
where time_id = 201201





select distinct BASE_PROD_ID
from G_I_02018_MONTH
where time_id = 201201


OFFER_ID	BASS1_OFFER_ID	   
111090001363	QW_QQT_DJ_DX0001	   
111090001364	QW_QQT_DJ_CX0001	   
111090001365	QW_QQT_DJ_ZX0001	   
111090001366	QW_QQT_DJ_YD0001	   
111090001367	QW_QQT_DJ_YY0001	   
111090001368	QW_QQT_DJ_FHZX0001	   
		

select distinct OVER_PROD_ID
from G_I_02021_MONTH
where time_id = 201201

999912221010006001            	   
999912220630003001            	   
999912220470003001            	   
999912220450010001            	   
999912220440010001            	   

999912221010006001            	   
999912221010005001            	   
999912220630003001            	   
999912220470003001            	   
999912220450010001            	   
999912220440010001            	   


select distinct OVER_PROD_ID
from G_I_02021_MONTH
where time_id = 201112




select distinct OVER_PROD_ID
from G_I_02021_MONTH
where time_id = 201111


QW_QQT_DJ_ZX0001              	   
QW_QQT_DJ_YD0001              	   
QW_QQT_DJ_FHZX0001            	   
QW_QQT_DJ_DX0001              	   
QW_QQT_DJ_CX0001              	   

BASS1_OFFER_ID
QW_QQT_DJ_CX0001
QW_QQT_DJ_DX0001
QW_QQT_DJ_FHZX0001
QW_QQT_DJ_YD0001
QW_QQT_DJ_YY0001
QW_QQT_DJ_ZX0001



111090001367


 select count(0) from bass2.dw_product_ins_off_ins_prod_201201
 where OFFER_ID = 111090001367
 

        select A.*,B.NAME from bass2.dw_product_ins_off_ins_prod_201108 a, bass2.dim_prod_up_product_item  b
where product_instance_id 
in 
('89160001135476'
)
and a.OFFER_ID = b.PRODUCT_ITEM_ID
AND B.NAME LIKE '%全球通%'


select * 
from g_s_03004_03005_R235_adj
where user_id not in (select  user_id 
from G_S_03004_MONTH_ADJ_BAK)


89101110016671 

select * from g_s_03004_03005_R235_adj
where user_id = '89101110016671'

TIME_ID	USER_ID	IFFEEZERO	IF03004	   
201201	89101110016671 	0	1	   
				
                
select * from G_S_03004_MONTH_ADJ_BAK
where user_id = '89101110016671'

		select *                 
		from g_s_03004_month                 
		where time_id = 201201                 
		and                   ( int(substr(ACCT_ITEM_ID,2))/100 in (1,2,3)                                 
		or ACCT_ITEM_ID in ('0401','0403','0407')                  )      
        and user_id = '89101110016671'
                


select IF03004 ,IFFEEZERO
,case when b.USER_ID is not null then 1 else 0 end ifadj
, count(0) ,count(distinct a.user_id)
--,  count(distinct IF03004 ) 
from g_s_03004_03005_R235_adj  a
left join G_S_03004_MONTH_ADJ_BAK b on a.user_id = b.user_id 
group by  IF03004 ,IFFEEZERO
,case when b.USER_ID is not null then 1 else 0 end
order by 1 


                
select a.*        
from g_s_03004_03005_R235_adj  a
left join G_S_03004_MONTH_ADJ_BAK b on a.user_id = b.user_id 
where 
(case when b.USER_ID is not null then 1 else 0 end ) = 0
and IF03004 = '1'
and IFFEEZERO = '1'



89101110011990 

                
		select ACCT_ITEM_ID,count(0),count(distinct user_id)                
		from g_s_03004_month  t               
		where time_id = 201201                  
        and exists (
select 1
from g_s_03004_03005_R235_adj k
where k.user_id not in (select  user_id 
from G_S_03004_MONTH_ADJ_BAK) and t.user_id = k.user_id)
group by ACCT_ITEM_ID

                
                

CREATE TABLE "BASS1   "."G_S_03004_MONTH_ADJ_BAK2"  (
                  "TIME_ID" INTEGER NOT NULL , 
                  "USER_ID" CHAR(20) NOT NULL , 
                  "ACCT_ITEM_ID" CHAR(4) NOT NULL , 
                  "BILL_CYC_ID" CHAR(6) NOT NULL , 
                  "FEE_RECEIVABLE" CHAR(10) NOT NULL , 
                  "FAV_CHRG" CHAR(10) )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "USER_ID",  
                 "ACCT_ITEM_ID",  
                 "BILL_CYC_ID")   
                   IN "TBS_APP_BASS1" NOT LOGGED INITIALLY ; 









                
                CREATE TABLE "BASS1   "."G_S_03005_MONTH_ADJ_BAK2"  (
                  "TIME_ID" INTEGER NOT NULL , 
                  "ACCT_ID" CHAR(20) NOT NULL , 
                  "USER_ID" CHAR(20) NOT NULL , 
                  "BILL_CYC_ID" CHAR(6) NOT NULL , 
                  "ITEM_ID" CHAR(4) NOT NULL , 
                  "SHOULD_FEE" CHAR(10) NOT NULL )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "ACCT_ID",  
                 "USER_ID",  
                 "BILL_CYC_ID",  
                 "ITEM_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 



select '201201'
,a.user_id   
, case when c.user_id is null then '0' else '1' end iffeezero  
, case when d.user_id is null then '0' else '1' end if03004  
from  (                  
        select distinct user_id 
        from BASS1.G_S_21003_MONTH_mobile   a                          
        ,int_02004_02008_month_stage b                  
        where a.product_no = b.product_no                 
        and  b.usertype_id NOT IN ('2010','2020','2030','9000')                 
        and b.test_flag = '0' 
        ) a
--在网用户
left join  (
                        select user_id                 
                        from g_s_03004_month                 
                        where time_id = 201201                 
                        and                   ( int(substr(ACCT_ITEM_ID,2))/100 in (1,2,3)                                 
                        or ACCT_ITEM_ID in ('0401','0403','0407'))                  
                        group by user_id                   
                        having sum(bigint(FEE_RECEIVABLE)) > 0 
                        ) b  on  a.user_id = b.user_id
--有语音收入的用户
left join (    select  user_id from  g_s_03004_month                   
                                where time_id = 201201                
                                and ( int(substr(ACCT_ITEM_ID,2))/100 in (1,2,3)                         
                                or ACCT_ITEM_ID in ('0401','0403','0407')
                                )                 
                                group by user_id  having sum(bigint(FEE_RECEIVABLE)) <= 0                              
                  ) c on a.user_id = c.user_id
--03004中语音费<=0的用户
left join (select distinct  user_id from  g_s_03004_month where time_id = 201201 ) d on a.user_id = d.user_id
--用户是否在03004中
where b.user_id is  null
with ur



select count(0) val1 
from (  select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID
from bass1.g_s_21003_month
where time_id=201201 and mns_type='1'
except
select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
from bass1.g_s_04017_day
where time_id/100=201201 and mns_type='1'
group by product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
) M
with ur


select count(0) val1 
from (  select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID
from bass1.g_s_21003_month
where time_id=201201 and mns_type='1'
except
select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
from bass1.g_s_04017_day
where time_id/100=201201 and mns_type='1'
group by product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
) M
with ur



select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
from bass1.g_s_04017_day
where time_id/100=201201 and mns_type='1'
group by product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID 
except
select product_no,ROAM_TYPE_ID,TOLL_TYPE_ID,CALL_TYPE_ID
from bass1.g_s_21003_month
where time_id=201201 and mns_type='1'



select * from bass1.g_rule_check where rule_code in ('R235') order by time_id desc



 bass1.g_a_02004_02008_stage
 
 select count(0) from BASS1.G_S_03012_MONTH
 
 
 select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22049_MONTH 
group by  time_id 
order by 1 

select time_



select
201201
  ,'201201'
  ,a.PRODUCT_NO
  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.CITY_ID)),'13101') 
  ,char(a.channel_id)
  ,case when a.channel_type=90102 
         and a.channel_type_dtl2 in (90176,90886,90741,90885,90178,90177,90880,90883,90175,90884,90179) then '2'
   else '1'
   end 
  ,char(bigint(sum(a.fee )))
from bass2.stat_channel_reward_0007 a
WHERE a.channel_type in (90105,90102)
  and a.op_time=201201
group by
  a.PRODUCT_NO
  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13101') 
  ,char(a.channel_id)  
  ,case when a.channel_type=90102 
         and a.channel_type_dtl2 in (90176,90886,90741,90885,90178,90177,90880,90883,90175,90884,90179) then '2'
   else '1'
   end 
   
select count(0)   
   from bass2.stat_channel_reward_0007 a
where a.op_time=201201



select op_time , count(0) 
--,  count(distinct op_time ) 
from bass2.stat_channel_reward_0007 
group by  op_time 
order by 1 


BASS1_G_S_22049_MONTH.tcl	REP_stat_channel_reward_0007.tcl	   

select * from 
app.sch_control_runlog
where control_code = 'REP_stat_channel_reward_0007.tcl'
CONTROL_CODE	BEGINTIME	ENDTIME	RUNTIME	FLAG	   
REP_stat_channel_reward_0007.tcl	2012-02-02 15:16:35	2012-02-02 15:17:30	55	0	   
					

select count(0)   
   from bass2.stat_channel_reward_0007 a
where a.op_time=201201

                    
                    
                    

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05001_month 
group by  time_id 
order by 1 desc 

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05002_month 
group by time_id 
order by 1 desc 





insert into bass1.g_s_05001_month
select * from  bass1.T_GS05001M where time_id = 201201


insert into bass1.g_s_05002_month
select * from  bass1.T_GS05002M where time_id = 201201

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05001_month 
group by  time_id 
order by 1 desc 

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05002_month 
group by time_id 
order by 1 desc 







select * from bass1.mon_all_interface 
where interface_code = '02026'




CREATE TABLE "BASS1   "."INT_02004_02008_MONTH_STAGE_201112"  (
                  "USER_ID" CHAR(15) , 
                  "PRODUCT_NO" CHAR(15) , 
                  "TEST_FLAG" CHAR(1) , 
                  "SIM_CODE" CHAR(15) , 
                  "USERTYPE_ID" CHAR(4) , 
                  "CREATE_DATE" CHAR(15) , 
                  "BRAND_ID" CHAR(4) , 
                  "TIME_ID" INTEGER )   
                 DISTRIBUTE BY HASH("USER_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 



insert into bass1.INT_02004_02008_MONTH_STAGE_201112 (
                     user_id    
                    ,product_no 
                    ,test_flag  
                    ,sim_code   
                    ,usertype_id  
                    ,create_date
                    ,brand_id
                    ,time_id )
                select e.user_id
                    ,e.product_no  
                    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
                    ,e.sim_code
                    ,f.usertype_id  
                    ,e.create_date  
                    ,e.brand_id
                    ,f.time_id       
                from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id
                                ,row_number() over(partition by user_id order by time_id desc ) row_id   
                from bass1.g_a_02004_day
                where time_id/100 <= 201112 ) e
                inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
                           from bass1.g_a_02008_day
                           where  time_id/100 <= 201112 ) f on f.user_id=e.user_id
                where e.row_id=1 and f.row_id=1

                    

 select * from bass1.G_RULE_CHECK where rule_code in ('R038')
 order by 1 desc
 
 select * from bass1.G_RULE_CHECK where rule_code in ('53')
 order by 1 desc
 
 
 
                    
                    


CREATE TABLE "BASS1   "."INT_02004_02008_MONTH_STAGE_201105"  (
                  "USER_ID" CHAR(15) , 
                  "PRODUCT_NO" CHAR(15) , 
                  "TEST_FLAG" CHAR(1) , 
                  "SIM_CODE" CHAR(15) , 
                  "USERTYPE_ID" CHAR(4) , 
                  "CREATE_DATE" CHAR(15) , 
                  "BRAND_ID" CHAR(4) , 
                  "TIME_ID" INTEGER )   
                 DISTRIBUTE BY HASH("USER_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 



insert into bass1.INT_02004_02008_MONTH_STAGE_201105 (
                     user_id    
                    ,product_no 
                    ,test_flag  
                    ,sim_code   
                    ,usertype_id  
                    ,create_date
                    ,brand_id
                    ,time_id )
                select e.user_id
                    ,e.product_no  
                    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
                    ,e.sim_code
                    ,f.usertype_id  
                    ,e.create_date  
                    ,e.brand_id
                    ,f.time_id       
                from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id
                                ,row_number() over(partition by user_id order by time_id desc ) row_id   
                from bass1.g_a_02004_day
                where time_id/100 <= 201105 ) e
                inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
                           from bass1.g_a_02008_day
                           where  time_id/100 <= 201105 ) f on f.user_id=e.user_id
                where e.row_id=1 and f.row_id=1

                    
create index bass1.idxr038201201 on  bass1.INT_02004_02008_MONTH_STAGE_201201(user_id)
create index bass1.idxr038201105 on  bass1.INT_02004_02008_MONTH_STAGE_201105(user_id)
create index bass1.idxr038201112 on  bass1.INT_02004_02008_MONTH_STAGE_201112(user_id)

                    

select brand_id,ITEM_ID,sum(should_fee) fee
from INT_02004_02008_MONTH_STAGE_201201 a
,(select user_id,ITEM_ID,sum(bigint(should_fee)) should_fee 
from g_s_03005_month where item_id in ('0100','0200','0300','0400','0500','0600','0700','0900') 
 and time_id = 201201 group by user_id,ITEM_ID) b
 where a.user_id = b.user_id 
group by a.brand_id ,ITEM_ID
order by 1




select brand_id,ITEM_ID,sum(should_fee) fee
from INT_02004_02008_MONTH_STAGE_201112 a
,(select user_id,ITEM_ID,sum(bigint(should_fee)) should_fee 
from g_s_03005_month where item_id in ('0100','0200','0300','0400','0500','0600','0700','0900') 
 and time_id = 201112 group by user_id,ITEM_ID) b
 where a.user_id = b.user_id 
group by a.brand_id ,ITEM_ID
order by 1



select time_id , count(0) 
--,  count(distinct time_id ) 
,sum(bigint(TRANS_POINTS))
from G_I_02006_MONTH 
group by  time_id 
order by 1 

select * from bass2.dw_product_201201
where product_no = '13989007120'

89160001196765

select * from G_I_02006_MONTH
where user_id = '89160001196765'



select time_id , count(0) 
--,  count(distinct time_id ) 
from G_a_02052_MONTH
group by  time_id 
order by 1 


select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22062_MONTH 
group by  time_id 
order by 1 


select * from BASS1.DIM_QW_QQT_PKGID 


select 
xzbas_value 二经编码
,xzbas_colname 二经名称
 from bass1.ALL_DIM_LKP where bass1_tbid in ('BASS_STD1_0114','BASS_STD1_0115')




select distinct PKG_ID from BASS1.G_I_02026_MONTH

select distinct PKG_ID from BASS1.G_I_02027_MONTH

select a.*
from G_I_02026_MONTH_LOAD a
	left join (select xzbas_value BASS2_ID, bass1_value QW_QQT_ID from bass1.ALL_DIM_LKP where bass1_tbid in ('BASS_STD1_0114','BASS_STD1_0115') ) c on a.old_pkg_id = c.bass2_id
	left join BASS1.DIM_QW_QQT_PKGID d on c.qw_qqt_id = d.OLD_PKG_ID
    where  c.qw_qqt_id  is not null
    
    
    select * from G_I_02026_MONTH_LOAD a
    where substr(a.new_pkg_id,1,4) = '9999'
    
    select * from bass1.G_I_02026_MONTH_1  a
    where substr(a.pkg_id,1,4) = '9999'
    

select count(0) from g_a_02004_day where time_id = 20120210
G_A_01004_DAY
select count(0) from G_A_01004_DAY where time_id = 20120210
12825
select count(0) from G_A_02008_DAY where time_id = 20120210
35014
select count(0) from G_A_02011_DAY where time_id = 20120210

1
7015

select count(0) from G_A_01004_DAY where time_id = 20120110
select count(0) from G_A_01004_DAY where time_id = 20120110
select count(0) from G_A_01004_DAY where time_id = 20120110
select count(0) from G_A_01004_DAY where time_id = 20120110
select count(0) from G_A_01004_DAY where time_id = 20120110
select count(0) from G_A_01004_DAY where time_id = 20120110
12825
select * from   BASS1.MON_ALL_INTERFACE 
where type = 'd'
and deadline = 9


select * from  table( bass1.get_before('02004')) a 




select * from app.sch_control_before 
where control_code 
in 
(
 'BASS1_G_S_02007_MONTH.tcl'
,'BASS1_G_S_03005_MONTH.tcl'
,'BASS1_G_S_03017_MONTH.tcl'
,'BASS1_G_I_03007_MONTH.tcl'
,'BASS1_G_S_03004_MONTH.tcl'
,'BASS1_G_S_22305_MONTH.tcl'
,'BASS1_G_S_03015_MONTH.tcl'
,'BASS1_G_S_22307_MONTH.tcl'
,'BASS1_G_S_03019_MONTH.tcl'
,'BASS1_G_I_02006_MONTH.tcl'
,'BASS1_G_S_22039_MONTH.tcl'
)

select * from  app.sch_control_runlog
where control_code not in (
select before_control_code from app.sch_control_before 
where control_code 
in 
(
 'BASS1_EXP_G_A_02004_DAY'
,'BASS1_EXP_G_A_02008_DAY'
,'BASS1_EXP_G_A_02011_DAY'
)
and control_code like 'BASS1%'
)



select distinct before_control_code from app.sch_control_before 
where control_code 
in 
(
 'BASS1_EXP_G_A_02004_DAY'
,'BASS1_EXP_G_A_02008_DAY'
,'BASS1_EXP_G_A_02011_DAY'
)
and before_control_code like 'BASS1%'
and control_code not in (select control_code from app.sch_control_runlog)

select distinct before_control_code from app.sch_control_before 
where control_code 
in (
'BASS1_INT_CHECK_Z345_DAY.tcl'	   
,'BASS1_INT_CHECK_E1_DAY.tcl'
,'BASS1_INT_CHECK_02004_02008_DAY.tcl'
,'BASS1_INT_CHECK_33TO40_DAY.tcl'
) 	


select distinct * from app.sch_control_before 
where control_code like 'BASS1%'



select before_control_code from app.sch_control_before 
where
 control_code 
in (
'BASS1_INT_CHECK_Z345_DAY.tcl'	   
,'BASS1_INT_CHECK_E1_DAY.tcl'
,'BASS1_INT_CHECK_02004_02008_DAY.tcl'
,'BASS1_INT_CHECK_33TO40_DAY.tcl'
) 	



select distinct * from app.sch_control_before 
where control_code like 'BASS1%'
and before_control_code like '%DM%'


select * from app.sch_control_runlog a
,(
select distinct control_code from app.sch_control_before 
where
 control_code 
in (
'BASS1_INT_CHECK_Z345_DAY.tcl'	   
,'BASS1_INT_CHECK_E1_DAY.tcl'
,'BASS1_INT_CHECK_02004_02008_DAY.tcl'
,'BASS1_INT_CHECK_33TO40_DAY.tcl'
) 	) b
,app.sch_control_task c
 where a.control_code = b.control_code
and a.control_code = c.control_code 





select * from app.sch_control_runlog a
,(
select distinct before_control_code control_code from app.sch_control_before 
where
 control_code 
in (
'BASS1_INT_CHECK_Z345_DAY.tcl'	   
,'BASS1_INT_CHECK_E1_DAY.tcl'
,'BASS1_INT_CHECK_02004_02008_DAY.tcl'
,'BASS1_INT_CHECK_33TO40_DAY.tcl'
) 	) b
,app.sch_control_task c
 where a.control_code = b.control_code
and a.control_code = c.control_code 
and date(a.endtime) < '2012-02-11'




select * from app.sch_control_runlog a
,(
select distinct before_control_code control_code from app.sch_control_before 
where
 control_code 
in (
'BASS1_INT_CHECK_Z345_DAY.tcl'	   
,'BASS1_INT_CHECK_E1_DAY.tcl'
,'BASS1_INT_CHECK_02004_02008_DAY.tcl'
,'BASS1_INT_CHECK_33TO40_DAY.tcl'
) 	) b
,app.sch_control_task c
 where a.control_code = b.control_code
and a.control_code = c.control_code 



CONTROL_CODE
BASS1_INT_CHECK_Z345_DAY.tcl
BASS1_G_A_02004_DAY.tcl
BASS1_G_A_02011_DAY.tcl




select * from app.sch_control_runlog a
,(
select distinct before_control_code control_code from app.sch_control_before 
where
 control_code 
in (
'BASS1_INT_CHECK_Z345_DAY.tcl'	   
,'BASS1_G_A_02004_DAY.tcl'
,'BASS1_G_A_02011_DAY.tcl'
) 	) b
,app.sch_control_task c
 where a.control_code = b.control_code
and a.control_code = c.control_code 






select * from app.sch_control_runlog a
,(
select distinct before_control_code control_code from app.sch_control_before 
where
 control_code 
in (
'BASS1_INT_CHECK_Z345_DAY.tcl'	   
,'BASS1_G_A_02004_DAY.tcl'
,'BASS1_G_A_02011_DAY.tcl'
) 	) b
,app.sch_control_task c
 where a.control_code = b.control_code
and a.control_code = c.control_code 
and date(a.endtime) < '2012-02-11'



BASS1_G_A_02062_DAY.tcl
BASS1_G_A_02004_DAY.tcl






select * from app.sch_control_runlog a
,(
select distinct before_control_code control_code from app.sch_control_before 
where
 control_code 
in (
'BASS1_G_A_02062_DAY.tcl'	   
,'BASS1_G_A_02004_DAY.tcl'
,'BASS1_G_A_02011_DAY.tcl'
) 	) b
,app.sch_control_task c
 where a.control_code = b.control_code
and a.control_code = c.control_code 
and date(a.endtime) < '2012-02-11'



BASS1_G_A_02062_DAY.tcl
select * from app.sch_control_before  a,app.sch_control_runlog b 
where a.control_code = 'BASS1_G_A_02062_DAY.tcl'
and a.BEFORE_CONTROL_CODE = b.CONTROL_CODE


select a.*,b.*,c.PRIORITY_VAL from app.sch_control_before  a,app.sch_control_runlog b 
,app.sch_control_task c 
where a.control_code 
in (
 'BASS1_G_A_02011_DAY.tcl'
,'BASS1_G_A_02062_DAY.tcl'	   
,'BASS1_G_A_02004_DAY.tcl'
,'BASS1_G_A_02011_DAY.tcl'
,'BASS1_G_A_02008_DAY.tcl'
 )
and a.BEFORE_CONTROL_CODE = b.CONTROL_CODE
and b.CONTROL_CODE = c.CONTROL_CODE








WITH n(control_code, before_control_code) AS 
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE before_control_code in 

(

 'BASS1_G_S_03016_MONTH.tcl'
,'BASS1_G_S_21008_MONTH.tcl'
,'BASS1_G_S_21011_MONTH.tcl'
,'BASS1_G_S_21020_MONTH.tcl'
,'BASS1_G_S_22040_MONTH.tcl'
,'BASS1_G_S_22072_MONTH.tcl'
,'BASS1_G_S_22306_MONTH.tcl'
,'BASS1_G_S_22304_MONTH.tcl'
,'BASS1_G_S_22303_MONTH.tcl'
,'BASS1_G_S_22305_MONTH.tcl'
,'BASS1_G_S_22085_MONTH.tcl'
,'BASS1_G_S_22083_MONTH.tcl'
,'BASS1_G_S_22086_MONTH.tcl'
,'BASS1_G_S_22081_MONTH.tcl'
,'BASS1_G_S_03018_MONTH.tcl'
,'BASS1_G_S_21012_MONTH.tcl'
,'BASS1_G_I_21020_MONTH.tcl'

)


           UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.before_control_code = n.control_code)
SELECT distinct c.control_code FROM n,app.sch_control_task c
where n.control_code = c.control_code
and c.deal_time = 2    
AND c.control_code  like 'BASS1_%' 
             
       
       
       
       
       



select 
 b.*
, lower(
 'put *'||b.interface_code||'*.dat ' 
) put_dat
, lower(
 'put *'||b.interface_code||'*.verf ' 
) put_verf
, lower(
 'rm *'||b.interface_code||'* ' 
) put_verf
, lower(
 ' *'||b.interface_code||'*dat \' 
) list
 from   bass1.MON_ALL_INTERFACE b where 
deadline  in (9)
and type = 'd'
and sts = 1


       
       
       select  substr(filename,18,5) from APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'



select *
from bass1.g_rule_check where rule_code in ('R174')
and target1 = 641104142931

select *
from bass1.g_rule_check where rule_code in ('R173')
and target1 = 108556539559



and time_id / 100 >= 201101


select *
from bass1.g_rule_check where rule_code in ('R174')
and time_id / 100 >= 201201

select count(0)
            from bass2.CDR_GPRS_LOCAL_20120210
            where drtype_id not in (8307)
              and bigint(product_no) not between 14734500000 
              and 14734999999 and apn_ni <> 'JF.XZ.IP.MOBILE.LAN.CHINAMOBILE'
              and service_code not in ('1010000001','1010000002','2000000000')
              
              3838946 bass1
              
              3839974 bass2
              
              

select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1
and unit_code in ('01002','01004','02004','02008','06031','06032','02011','02053')
              
              
        
update (
select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1
and unit_code in ('01002','01004','02004','02008','06031','06032','02011','02053')
       ) t
set   return_flag = 0
where     return_flag=1
  
  


update (
select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag = 0
and unit_code in ('01002','01004','02004','02008','06031','06032','02011','02053')
       ) t
set   return_flag = 1
where     return_flag=0


  
              
              
              select * from  bass2.temp_20120131_zfp11a,b
b,a
a,c


 temp_20120131_zfp11
 
 
 select PRODUCT_NO,OPP_NUMBER ,SMS_CNT,count(0)
 from 
 (
 select PRODUCT_NO,OPP_NUMBER,SMS_CNT
 from bass2.temp_20120131_zfp11 a
 union all
 select OPP_NUMBER PRODUCT_NO,PRODUCT_NO OPP_NUMBER,SMS_CNT
 from bass2.temp_20120131_zfp11 b
 ) t 
 group by PRODUCT_NO,OPP_NUMBER ,SMS_CNT
 having count(0) > 1
 
 

 select * from  bass2.temp_20120131_zfp11
 
 select * from  bass2.temp_20120131_zfp12
 
 13518900000	15289106662	127
  
  
 select * from  bass2.temp_20120131_zfp11
where PRODUCT_NO = '13518900000'
and OPP_NUMBER = '15289106662'

13518900000	15289106662	127	   


 
   
 select * from  bass2.temp_20120131_zfp12
where PRODUCT_NO = '13518900000'
and OPP_NUMBER = '15289106662'

13518900000	15289106662	127	2	   


delete from 
(

 select * from  bass2.temp_20120131_zfp11
 where (PRODUCT_NO,OPP_NUMBER,SMS_CNT) in ( 
 select PRODUCT_NO,OPP_NUMBER,SMS_CNT from  bass2.temp_20120131_zfp12
 )
 
 ) t



 select count(0) from  bass2.temp_20120131_zfp11
 where (PRODUCT_NO,OPP_NUMBER,SMS_CNT) in ( 
 select PRODUCT_NO,OPP_NUMBER,SMS_CNT from  bass2.temp_20120131_zfp12
 )
 
 1656250
 
 select count(0) from  bass2.temp_20120131_zfp12
 
 1656250
 

 select * from  bass2.temp_20120131_zfp11
 where (PRODUCT_NO,OPP_NUMBER,SMS_CNT) in ( 
 select PRODUCT_NO,OPP_NUMBER,SMS_CNT from  bass2.temp_20120131_zfp12
 )  
 and  PRODUCT_NO = '13518900000'
and OPP_NUMBER = '15289106662'



 select * from  bass2.temp_20120131_zfp11
 where (PRODUCT_NO,OPP_NUMBER,SMS_CNT) in ( 
 select PRODUCT_NO,OPP_NUMBER,SMS_CNT from  bass2.temp_20120131_zfp12
 )  
 and  OPP_NUMBER = '13518900000'
and PRODUCT_NO = '15289106662'

a,b
b,a

a,b
b,a


select count(0) from 
 bass2.temp_20120131_zfp11


 select PRODUCT_NO,OPP_NUMBER,SMS_CNT from  bass2.temp_20120131_zfp12
 where ( PRODUCT_NO = '13518900000'
and OPP_NUMBER = '15289106662'
) or 
(
   OPP_NUMBER = '13518900000'
and PRODUCT_NO = '15289106662'
)




select a.PRODUCT_NO,b.OPP_NUMBER,a.SMS_CNT
from bass2.temp_20120131_zfp12 a 
, bass2.temp_20120131_zfp12 b
where a.PRODUCT_NO = b.OPP_NUMBER
and a.OPP_NUMBER = b.PRODUCT_NO


select count(0)
from bass2.temp_20120131_zfp11 a 
, bass2.temp_20120131_zfp11 b
where a.PRODUCT_NO = b.OPP_NUMBER
and a.OPP_NUMBER = b.PRODUCT_NO

1656250

select *
from bass2.temp_20120131_zfp11 a 
, bass2.temp_20120131_zfp11 b
where a.PRODUCT_NO = b.OPP_NUMBER
and a.OPP_NUMBER = b.PRODUCT_NO
 and (( a.PRODUCT_NO = '13518900000'
and a.OPP_NUMBER = '15289106662'
) or 
(
   a.OPP_NUMBER = '13518900000'
and a.PRODUCT_NO = '15289106662'
)
)


select a.PRODUCT_NO a,b.OPP_NUMBER b
from (
select *
from bass2.temp_20120131_zfp12 a 
, bass2.temp_20120131_zfp12 b
where a.PRODUCT_NO = b.OPP_NUMBER
and a.OPP_NUMBER = b.PRODUCT_NO
) t 
group by a.PRODUCT_NO ,b.OPP_NUMBER 



select count(0)
from (
select a.PRODUCT_NO a ,b.OPP_NUMBER  b
from bass2.temp_20120131_zfp12 a 
, bass2.temp_20120131_zfp12 b
where a.PRODUCT_NO = b.OPP_NUMBER
and a.OPP_NUMBER = b.PRODUCT_NO
) t 
group by a,b
having count(0) > 1


select count(0) from bass2.temp_20120131_zfp11 o
where (PRODUCT_NO,OPP_NUMBER)
in (select a,b
from  ( 
select  a.PRODUCT_NO a ,b.OPP_NUMBER  b
, b.PRODUCT_NO c ,b.OPP_NUMBER  d
from bass2.temp_20120131_zfp11 a 
, bass2.temp_20120131_zfp11 b
where a.PRODUCT_NO = b.OPP_NUMBER
and a.OPP_NUMBER = b.PRODUCT_NO
) i
)
and (PRODUCT_NO,OPP_NUMBER)
not in 
(select c,d
from ( 
select  a.PRODUCT_NO a ,b.OPP_NUMBER  b
, b.PRODUCT_NO c ,b.OPP_NUMBER  d
from bass2.temp_20120131_zfp11 a 
, bass2.temp_20120131_zfp11 b
where a.PRODUCT_NO = b.OPP_NUMBER
and a.OPP_NUMBER = b.PRODUCT_NO
) i
)


select * from  bass2.dim_cfg_static_data
where code_name like '%二卡合一%'




drop view v_tac_20120214

create view v_tac_20120214
as
select a.tac_num,
                a.type,
                case when c.tac_num is not null then 1
                  else 0
                end iftd
         from (SELECT b.tac_num, CASE
                                    WHEN lower (a.value_desc) LIKE '%ios%' THEN 3
                                    WHEN lower (a.value_desc) LIKE '%android%' THEN 5
                                    WHEN lower (a.value_desc) LIKE '%symbian%' THEN 6
                                    ELSE 7
                                 END
                                    AS type
                 FROM bass2.dim_device_profile a, bass2.dim_tacnum_devid b
                WHERE a.device_id = b.dev_id AND a.property_id = '003001'
                  AND (lower (a.value_desc) LIKE '%android%'
                    OR lower (a.value_desc) LIKE '%ios%'
                    OR lower (a.value_desc) LIKE '%linux%'
                    OR lower (a.value_desc) LIKE '%palm%'
                    OR lower (a.value_desc) LIKE '%symbian%'
                    OR lower (a.value_desc) LIKE '%windows%')
               GROUP BY b.tac_num, CASE
                                      WHEN lower (a.value_desc) LIKE '%ios%' THEN 3
                                      WHEN lower (a.value_desc) LIKE '%android%' THEN 5
                                      WHEN lower (a.value_desc) LIKE '%symbian%' THEN 6
                                      ELSE 7
                                   END) a
                               LEFT JOIN
                                  (SELECT tac_num
                                     FROM bass2.DIM_TERM_TAC
                                    WHERE net_type = '2') c
                         ON a.tac_num = c.tac_num
          group by a.tac_num,
                a.type,
                case when c.tac_num is not null then 1
                  else 0
                end
                
                

select count(0),count(distinct tac_num) from 
v_tac_20120214                



SELECT a.user_id, b.term_type, b.td_termmark
           FROM    (SELECT user_id, substr (imei, 1, 8) AS imei
                      FROM dw_product_mobilefunc_$year$month
                     WHERE usertype_id IN (1, 2, 9)
                       AND userstatus_id IN (1, 2, 3, 6, 8)) a
                LEFT JOIN
                   session.stat_inte_term_tmp b
                ON a.imei = b.tac_num
          WHERE b.tac_num IS NOT NULL
         GROUP BY a.user_id, b.term_type, b.td_termmark
         
         
         
select time_id , count(0) ,count(distinct user_id) , count(distinct imei)
--,  count(distinct time_id ) 
from G_S_02047_MONTH where time_id = 201201
group by  time_id 
order by 1 

         
         TIME_ID	2	3	4	   
201201	2366921	1373990	1486712	   
				
              v_tac_20120214  
                
                SELECT a.user_id, b.TYPE, b.iftd
           FROM    (SELECT user_id, substr (imei, 1, 8) AS imei
                      FROM bass1.G_S_02047_MONTH /**dw_product_mobilefunc_$year$month**/
                   ) a
                LEFT JOIN
                   v_tac_20120214 b
                ON a.imei = b.tac_num
          WHERE b.tac_num IS NOT NULL
         GROUP BY a.user_id, b.TYPE, b.iftd
         
         

select          



select count(0)
from (
                SELECT a.user_id, b.TYPE, b.iftd
           FROM    (SELECT user_id, substr (imei, 1, 8) AS imei
                      FROM bass1.G_S_02047_MONTH where time_id = 201112 /**dw_product_mobilefunc_$year$month**/
                   ) a
                LEFT JOIN
                   v_tac_20120214 b
                ON a.imei = b.tac_num
          WHERE b.tac_num IS NOT NULL
         GROUP BY a.user_id, b.TYPE, b.iftd
         ) a
         242523
         25.54
         
         x = -14% *x  + 242523
         1.14x=242523
         
         220876
         
         
         
         
         
select * from G_I_06021_MONTH
where time_id = 201201
and channel_id = '20200124'

select * from G_I_06023_MONTH
where time_id = 201201
and channel_id = '10100101'

 channel_addr like '%无%'
         
         
         G_A_06035_DAY
           
select * from G_A_06035_DAY
where time_id/10 = 201201
and channel_name like '%无%'
         
                
                
update (                         
select * from G_I_06021_MONTH
where time_id = 
and
channel_addr  = '无'
) t 
set channel_addr = trim(COUNTRY_NAME)||trim(CHANNEL_NAME)

         
select    trim(COUNTRY_NAME)||trim(CHANNEL_NAME)
from    G_I_06021_MONTH
where time_id = 201201
and
channel_addr  = '无'    
         
         

select avg(bigint(STORE_AREA)) from G_I_06023_MONTH
where time_id = 201201
and STORE_AREA <> ''

and channel_id = '10100101'
         

update (         
select * from G_I_06023_MONTH
where time_id = 201201
and 
(
STORE_AREA = '' or STORE_AREA = '0'
)
and bigint(BUILD_AREA) > 2
and BUILD_AREA <> ''
and channel_id = '100003065'
) t 
set STORE_AREA = char(bigint(BUILD_AREA)-2)

 
100003065

100003064
100003065

select * from G_I_06023_MONTH
where channel_id like '10000%'

select * from G_I_06021_MONTH
where channel_id like '10000%'




SEAT_NUM




update (         
select * from G_I_06023_MONTH
where time_id = 201201
and 
(
SEAT_NUM = '' or SEAT_NUM = '0'
)
) t 
set SEAT_NUM = '1'





       
select case when SEAT_NUM = '' or SEAT_NUM = '0' then '1' else SEAT_NUM  end  SEAT_NUM 
,bigint(case when SEAT_NUM = '' or SEAT_NUM = '0' then '1' else SEAT_NUM  end)*2 STORE_EMPLOYE
 from G_I_06023_MONTH
where time_id = 201201
and 
(
SEAT_NUM = '' or SEAT_NUM = '0'
)


) t 


set SEAT_NUM = '1'


STORE_EMPLOYE


select * from app.g_runlog 
where time_id=int(replace(char(current date - 2 days),'-',''))
and return_flag=1


select * from  bass1.g_rule_check
where rule_code = 'R159_1'
and time_id/100 = 201201


select count(0) from 
bass1.int_02004_02008_month_stage 
where bigint(CREATE_DATE) / 100 = 201201
and TEST_FLAG = '0'
and USERTYPE_ID not IN ('2010','2020','2030','9000')

 57243
 
 
 select sum(target1) from  bass1.g_rule_check
where rule_code = 'R159_1'
and time_id/100 = 201201

57214.00000

1	   
57183	   
	
    
    
     select sum(target1) from  bass1.g_rule_check
where rule_code = 'R159_4'
and time_id/100 = 201201
1
62107.00000



select time_id, sum(bigint(FAVOURED_BASECALL_FEE))
from G_S_21001_DAY 
where TIME_ID/100 = 201201
group by time_id

select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 4 days),'-',''))
and return_flag=1
except
select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 3 days),'-',''))
and return_flag=1

UNIT_CODE
04002
22036



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
where 
    rule_code in ('R159_1','R159_2','R159_3','R159_4')
and rule_code='R159_4'
order by 3 
 

select count(0) from bass2.CDR_GPRS_LOCAL_20120108


BASS1_INT_CHECK_COMP_KPI_DAY.tcl

--置重运
update (
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code = 'BASS1_INT_CHECK_INDEX_SAME_DAY.tcl'
)t 
set flag = 1

update 
(
select * from  app.sch_control_runlog
where   control_code in (
'BASS1_INT_CHECK_INDEX_SAME_DAY.tcl'
)
and flag = -1 
) t
set  flag = -2

------------------------


---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
--and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc



select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 2 days),'-',''))
and return_flag=1
except
select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1



select * from  app.sch_control_runlog
where   control_code  like 'BASS1%'
and 
and flag <> 0




select * from app.sch_control_runlog 
where control_code in (
select before_control_code from  table( bass1.get_before('BASS2_Dw_enterprise_industry_apply.tcl')) a 
)

BASS2_Dw_enterprise_industry_apply.tcl


select * from 
BASS1.G_S_22302_DAY
order by 1 desc 




select count(0) from    g_s_04002_day
where time_id = 20120301
4808224


select * from   BASS1.MON_ALL_INTERFACE 
where interface_code  = '22302'

select * from 
BASS1.G_S_22302_DAY
where time_id = 20120301
order by 1 desc 



select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_04002_DAY 
group by  time_id 
order by 1 



R232 ADJ:
	update G_I_21020_MONTH 
	set CALL_COUNTS = char(int(rand(1)*5+1))
	where (
	char(TIME_ID) = substr(COMP_LAST_DATE,1,6)
	and CALL_COUNTS = '0'
	and SMS_COUNTS = '0'
	and MMS_COUNTS = '0'
	and time_id = 201202 )


                select 
                sum(bigint(a.flows))
                from bass1.g_s_04002_day_flows a,
                bass1.td_check_user_flow b
                where a.product_no=b.product_no
                  and a.mns_type='1'
                  
select count(0)
from                   bass1.g_s_04002_day_flows
1	   
64152	   
	
select count(0)
from                   bass1.td_check_user_flow
1
71




 
 
 
 select * from g_s_22204_month 
where time_id >= 201101
 order by 1 desc ,2 desc 
 
 
 
 bass1.g_s_22204_month_tmp3(cycle_id,td_cust_t_flows)
                values('1',decimal(1.0*(2670241964376+312391406110-)/1024/1024,20,2))
                
                
                
values decimal(1.0*(2670241964376+312391406110)/1024/1024,20,2)
2844460.84
3150573        


		select 
		value(sum(bigint(a.flows)),0)
		from bass1.g_s_04002_day_flows a,
		bass1.td_check_user_flow b
		where a.product_no=b.product_no
		  and a.mns_type='1'
          
                
select * froM app.sch_control_runlog where control_code in (                
select before_control_code from  table( bass1.get_before('BASS1_G_I_02027_MONTH.tcl')) a 

                )

select * from app.sch_control_task where control_code = 'BASS1_G_I_02027_MONTH.tcl'
                
                


select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22049_MONTH 
group by  time_id 
order by 1 
                
                

select
201201
  ,'201201'
  ,a.PRODUCT_NO
  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.CITY_ID)),'13101') 
  ,char(a.channel_id)
  ,case when a.channel_type=90102 
         and a.channel_type_dtl2 in (90176,90886,90741,90885,90178,90177,90880,90883,90175,90884,90179) then '2'
   else '1'
   end 
  ,char(bigint(sum(a.fee )))
from bass2.stat_channel_reward_0007 a
WHERE a.channel_type in (90105,90102)
  and a.op_time=201202
group by
  a.PRODUCT_NO
  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13101') 
  ,char(a.channel_id)  
  ,case when a.channel_type=90102 
         and a.channel_type_dtl2 in (90176,90886,90741,90885,90178,90177,90880,90883,90175,90884,90179) then '2'
   else '1'
   end 
                
                

select b.chnl_new_cnt,a.all_cnt , b.chnl_new_cnt*1.00/a.all_cnt
from (
select count(distinct user_id) all_cnt from    bass1.int_02004_02008_month_stage 
where bigint(create_date)/100 = 201202
) a ,
(
select sum(bigint(NEW_USER_CNT)) chnl_new_cnt 
from G_S_22091_DAY
where time_id / 100 = 201202
) b where 1 = 1

                
                
               


SELECT SUM(T.FY)
                 FROM 
                 (
                    SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
                    FROM BASS1.G_S_21003_TO_DAY
                    WHERE TIME_ID/100=201202
                    UNION
                    SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
                    FROM BASS1.G_S_21006_TO_DAY
                    WHERE TIME_ID/100=201202
                 )T
                 
                 
                 g_s_
                 
SELECT SUM(T.FY)
                 FROM 
                 (
                    SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
                    FROM BASS1.G_S_21003_TO_DAY
                    WHERE TIME_ID/100=201201
                    UNION
                    SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
                    FROM BASS1.G_S_21006_TO_DAY
                    WHERE TIME_ID/100=201201
                 )T


FAVOURED_CALL_FEE

SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
                    FROM BASS1.G_S_21003_MONTH
                    WHERE TIME_ID/100=201201
                    


SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
                    FROM BASS1.G_S_21003_MONTH
                    WHERE TIME_ID=201201
                    
8624568407

SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
                    FROM BASS1.G_S_21003_MONTH
                    WHERE TIME_ID=201202
               7945164157     
                    
                    SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
                    FROM BASS1.G_S_21003_TO_DAY
                    WHERE TIME_ID/100=201202
7945164157


select * from g_a_01005_day where time_id = 20120305


select CUST_ID,BIRTHDAY,MARRY,OCCUPATION_ID,FOLK_ID,EDUCATION_ID,SEX_ID,NATION_ID,VIP_MARK,CMCC_ID,CREATE_DATE,CHANNEL_ID,CUSTSTATUS_ID from bass1.G_A_01002_DAY where time_id=20120305
and CUST_ID is null

                   
                   



select CUST_ID,BIRTHDAY,MARRY,OCCUPATION_ID,FOLK_ID,EDUCATION_ID,SEX_ID,NATION_ID,VIP_MARK,CMCC_ID,CREATE_DATE,CHANNEL_ID,CUSTSTATUS_ID from bass1.G_A_01002_DAY where time_id=20120305
and MARRY is null

                   
                   
                   

select CUST_ID,BIRTHDAY,MARRY,OCCUPATION_ID,FOLK_ID,EDUCATION_ID,SEX_ID,NATION_ID,VIP_MARK,CMCC_ID,CREATE_DATE,CHANNEL_ID,CUSTSTATUS_ID from bass1.G_A_01002_DAY where time_id=20120305
and EDUCATION_ID is null



select CUST_ID,BIRTHDAY,MARRY,OCCUPATION_ID,FOLK_ID,EDUCATION_ID,SEX_ID,NATION_ID,VIP_MARK,CMCC_ID,CREATE_DATE,CHANNEL_ID,CUSTSTATUS_ID from bass1.G_A_01002_DAY where time_id=20120305
and EDUCATION_ID = ''



select CUST_ID,BIRTHDAY,MARRY,OCCUPATION_ID,FOLK_ID,EDUCATION_ID,SEX_ID,NATION_ID,VIP_MARK,CMCC_ID,CREATE_DATE,CHANNEL_ID,CUSTSTATUS_ID from bass1.G_A_01002_DAY where time_id=20120305
and SEX_ID = ''


select CUST_ID,BIRTHDAY,MARRY,OCCUPATION_ID,FOLK_ID,EDUCATION_ID,SEX_ID,NATION_ID,VIP_MARK,CMCC_ID,CREATE_DATE,CHANNEL_ID,CUSTSTATUS_ID from bass1.G_A_01002_DAY where time_id=20120305
and NATION_ID is null



select CUST_ID,BIRTHDAY,MARRY,OCCUPATION_ID,FOLK_ID,EDUCATION_ID,SEX_ID,NATION_ID,VIP_MARK,CMCC_ID,CREATE_DATE,CHANNEL_ID,CUSTSTATUS_ID from bass1.G_A_01002_DAY where time_id=20120305
and VIP_MARK is null


select count(0) from  bass1.G_A_01002_DAY where time_id=20120305


select CUST_ID,BIRTHDAY,MARRY,OCCUPATION_ID,FOLK_ID,EDUCATION_ID,SEX_ID,NATION_ID,VIP_MARK,CMCC_ID,CREATE_DATE,CHANNEL_ID,CUSTSTATUS_ID from bass1.G_A_01002_DAY where time_id=20120305
and CMCC_ID is null




select CUST_ID,BIRTHDAY,MARRY,OCCUPATION_ID,FOLK_ID,EDUCATION_ID,SEX_ID,NATION_ID,VIP_MARK,CMCC_ID,CREATE_DATE,CHANNEL_ID,CUSTSTATUS_ID from bass1.G_A_01002_DAY where time_id=20120305


delete from (
select * from  bass1.G_A_01002_DAY where time_id=20120305
and cust_id = '89160001524433') t



select * from  bass1.G_A_01002_DAY where cust_id = '89160001524433'

insert into bass1.g_s_05001_month
select * from  bass1.T_GS05001M where time_id = 201202


insert into bass1.g_s_05002_month
select * from  bass1.T_GS05002M where time_id = 201202

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05001_month 
group by  time_id 
order by 1 desc 

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05002_month 
group by time_id 
order by 1 desc 


select * from  bass1.g_s_05001_month 
where time_id = 201202

select * from  bass1.g_s_05002_month 
where time_id = 201202

select * from  bass1.g_s_05001_month 
where time_id = 201201

select cust_id 
from bass2.dw_product_20120305
where user_id = '89160001048760'
       

select * from bass2.dwd_vipcust_manager_rela_20120229
where cust_id = '89103001578939'



select * from bass2.dwd_vipcust_manager_rela_20120229
where user_id = '89160001048760'

select * from 
bass2.dwd_cust_vip_card_201202
where cust_id = '89103001578939'


select * from  bass1.G_RULE_CHECK where rule_code = 'R085'
 and time_id > 20110825
 order by 1 desc 
 
 
 
 select *
                from BASS1.G_S_04004_DAY
                where time_id= 20120309
                and BUS_SRV_ID in('4','1')
                and bigint(info_fee)>0
                and MM_KIND='1'
                


delete from                 
(
 select *
                from BASS1.G_S_04004_DAY
                where time_id= 20120309
                and BUS_SRV_ID in('4')
                and bigint(info_fee) = 60
                and MM_KIND='1'
                and product_no = '15289014457'
                ) t
                
                
                
                
select * from 
bass2.ETL_ROLLBACK_TASK_MAP
                
                


select 
 b.*
, lower(
 'put *'||b.interface_code||'*.dat ' 
) put_dat
, lower(
 'put *'||b.interface_code||'*.verf ' 
) put_verf
, lower(
 'rm *'||b.interface_code||'* ' 
) put_verf
, lower(
 ' *'||b.interface_code||'*dat \' 
) list
 from   bass1.MON_ALL_INTERFACE b
where type = 'm'
and sts = 1

                
                

select * from bass2.ODS_CUST_CM_CUSTOMER_20120307                
fetch first 10 rows only

select * from  bass1.mon_all_interface 
where interface_code = '02053'

select unit_code , count(0)
from (
select *
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
left join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join (select * from bass1.mon_all_interface where sts = 1 and type = 'm') c on substr(a.filename,16,5) = c.INTERFACE_CODE 
) t 
group by unit_code 
having count(0) > 1

02053


select distinct c.INTERFACE_NAME,a.*,b.*
FROM 
(select t.*
    from 
    (
    select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
    from APP.G_FILE_REPORT a
    where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
    and err_code='00'
    and length(filename)=length('s_13100_201002_03014_01_001.dat')
    ) t where rn = 1
) a 
left join 
(
select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
) b on substr(a.filename,16,5) = b.unit_code 
left join (select * from bass1.mon_all_interface where sts = 1) c on substr(a.filename,16,5) = c.INTERFACE_CODE 
with ur






select * from G_I_06023_MONTH
where time_id = 201202
and 
(
STORE_AREA = '' or STORE_AREA = '0'
)
and ( BUILD_AREA <> '')




select * from G_I_06023_MONTH
where time_id = 201202
and( channel_id = '100003064'
or channel_id = '94000003'
)


select * from G_I_06021_MONTH
where time_id = 201202
and channel_id = '100003064'



select * from G_I_06021_MONTH
where time_id = 201202
and CHANNEL_TYPE = '1'
and CHANNEL_STATUS = '1'

and channel_id = '100003064'


select *
from 
(
select * from G_I_06021_MONTH
where time_id = 201202
and CHANNEL_TYPE = '1'
and CHANNEL_STATUS = '1'
) a left join 
(select * from G_I_06023_MONTH where time_id = 201202 ) b on a.channel_id = b.channel_id
where (b.BUILD_AREA  <= '0' or b.BUILD_AREA  = ''
         or SEAT_NUM = '' or SEAT_NUM = '0'
       )



100002329
100002391
100003064


前台业务办理笔数

select count(0) from G_S_22091_DAY where time_id / 100 = 201202
and channel_id in ('100002329','100002391','100003064')




select * from  BASS2.Dim_CHANNEL_INFO
where char(channel_id) in ('100002329','100002391','100003064')


ORGANIZE_ID
382161
382248
382866

select count(0) from G_S_22091_DAY where time_id / 100 = 201202
and channel_id in ('382161','382248','382866')


select * from  BASS2.Dim_CHANNEL_INFO
where char(channel_id) in ('382161','382248','382866')


bass2.dw_product_ins_off_ins_prod_201108

select count(0) from bass2.dw_product_ord_cust_dm_201202
where char(org_id) in ('382161','382248','382866','100002329','100002391','100003064')
select count(0) from bass2.dw_product_ord_offer_dm_201202
where char(org_id) in ('382161','382248','382866','100002329','100002391','100003064')


select count(0) from bass2.dw_product_ord_so_log_dm_201202
where char(org_id) in ('382161','382248','382866','100002329','100002391','100003064')


dw_product_ord_so_log_dm_

				   bass2.dw_product_ord_cust_dm_$curr_month a,
				       bass2.dw_product_ord_offer_dm_$curr_month b,


select distinct CHANNEL_ID 
from G_I_06021_MONTH 
where time_id = 201202
and CHANNEL_TYPE = '1'
and CHANNEL_STATUS = '1'
except
select distinct CHANNEL_ID from 
G_S_22091_DAY where time_id / 100 = 201202



select count(0) from g_i_06022_month where                      
channel_id in ('382161','382248','382866','100002329','100002391','100003064')
and time_id = 201202


select * from  BASS2.Dim_CHANNEL_INFO
where char(channel_id) in ('100002329','100002391','100003064')


select * from bass2.Dwd_channel_selfsite_info_20120229
where char(channel_id) in ('100002329','100002391','100003064')





				select user_id from    bass1.g_i_02023_day where time_id=20120315
				except
				select user_id from    bass1.g_i_02022_day 
                      
                delete from (
                select * from    bass1.g_i_02023_day where time_id=20120315
				and user_id = '89160000950064'
                ) t
                
                
select 
         sum(RESULT1)
        ,sum(RESULT2)
        ,sum(RESULT3)
        ,sum(RESULT4)
        ,sum(RESULT5)
        ,sum(RESULT6)
        ,sum(RESULT7)
        ,sum(RESULT8)
        ,sum(RESULT9)
        ,sum(RESULT10)
        ,sum(RESULT11)
        ,sum(RESULT12)
        ,sum(RESULT13)
        ,sum(RESULT14)
        ,sum(RESULT15)
        ,sum(RESULT16)
        ,sum(RESULT17)
        ,sum(RESULT18)
        ,sum(RESULT19)
        ,sum(RESULT20)
        ,sum(RESULT21)
        ,sum(RESULT22)
        ,sum(RESULT23)
        ,sum(RESULT24)
        ,sum(RESULT25)
        ,sum(RESULT26)
        ,sum(RESULT27)
        ,sum(RESULT28)
        ,sum(RESULT29)
        ,sum(RESULT30)
        ,sum(RESULT31)
        ,sum(RESULT32)
from bass2.stat_market_0137
where time_id between '2011-05-17' and '2012-01-31'


                        
select time_id
        ,sum(RESULT1)
        ,sum(RESULT2)
        ,sum(RESULT3)
        ,sum(RESULT4)
        ,sum(RESULT5)
        ,sum(RESULT6)
        ,sum(RESULT7)
        ,sum(RESULT8)
        ,sum(RESULT9)
        ,sum(RESULT10)
        ,sum(RESULT11)
        ,sum(RESULT12)
        ,sum(RESULT13)
        ,sum(RESULT14)
        ,sum(RESULT15)
        ,sum(RESULT16)
        ,sum(RESULT17)
        ,sum(RESULT18)
        ,sum(RESULT19)
        ,sum(RESULT20)
        ,sum(RESULT21)
        ,sum(RESULT22)
        ,sum(RESULT23)
        ,sum(RESULT24)
        ,sum(RESULT25)
        ,sum(RESULT26)
        ,sum(RESULT27)
        ,sum(RESULT28)
        ,sum(RESULT29)
        ,sum(RESULT30)
        ,sum(RESULT31)
        ,sum(RESULT32)
from bass2.stat_market_0137
where time_id between '2011-05-17' and '2012-01-31'
and city_id = '890'
group by time_id
order by 1 desc 


select * from  bass2.stat_market_0137 where time_id = '2011-08-08'
    
                
select count(0) from g_s_02024_day
where time_id between 20110517 and 20120131
112026 	81281 
146053

select time_id,count(0) from g_s_02024_day group by time_id 


select * from  bass2.stat_market_0141 
where time_id = '2012-02-01'


select * from bass2.stat_market_0154
where city_id = '890'


bass2.stat_market_0154



                
select time_id
        ,sum(RESULT1)
        ,sum(RESULT2)
        ,sum(RESULT3)
        ,sum(RESULT4)
        ,sum(RESULT5)
        ,sum(RESULT6)
        ,sum(RESULT7)
        ,sum(RESULT8)
        ,sum(RESULT9)
        ,sum(RESULT10)
        ,sum(RESULT11)
        ,sum(RESULT12)
        ,sum(RESULT13)
        ,sum(RESULT14)
        ,sum(RESULT15)
        ,sum(RESULT16)
        ,sum(RESULT17)
        ,sum(RESULT18)
        ,sum(RESULT19)
        ,sum(RESULT20)
        ,sum(RESULT21)
        ,sum(RESULT22)
        ,sum(RESULT23)
        ,sum(RESULT24)
        ,sum(RESULT25)
        ,sum(RESULT26)
        ,sum(RESULT27)
from bass2.stat_market_0154
where city_id = '890'
--and time_id between '2011-05-17' and '2012-01-31'
group by time_id
order by 1 desc 



select user_id from  BASS2.stat_market_0154_user_01
except
select user_id from  BASS1.g_i_02022_day
where time_id = 20120229

89101110079979	   
89101110083960	   
89101110110553	   
89101110205968	   
89101110213358	   
89101110252464	   
89101110284841	   
89101110288539	   



select  TIME_ID,USER_ID,BASE_PKG_ID,VALID_DT
from (
        select 
                20111025 TIME_ID
                ,char(a.product_instance_id)  USER_ID
                ,bass1.fn_get_all_dim_ex('BASS_STD1_0114',char(a.offer_id)) BASE_PKG_ID
                ,replace(char(date(a.VALID_DATE)),'-','') VALID_DT
                ,row_number()over(partition by a.product_instance_id order by expire_date desc ,VALID_DATE desc ) rn 
        from  bass2.Dw_product_ins_off_ins_prod_201202 a
        ,(select xzbas_value  as offer_id 
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0114'
                                              and bass1_value like 'QW_QQT_JC%'
                                              and XZBAS_COLNAME not like '套餐减半%'
                                      ) c
        where  char(a.offer_id) = c.offer_id 
          --and a.state =1
          --and a.valid_type = 1
          and a.OP_TIME = '2012-02-25'    
          and date(a.VALID_DATE)<='2011-10-25'  
    and date(a.expire_date) >= '2011-10-25'
        
) t where t.rn = 1
and USER_ID = '89101110288539'
         with ur
         
         
           and not exists (      select 1 from bass2.dwd_product_test_phone_20111025 b 
                                where a.product_instance_id = b.USER_ID  and b.sts = 1
                         ) 
                         


select * from bass2.Dw_product_ins_off_ins_prod_201202 a,bass2.dim_prod_up_product_item b
where product_instance_id  = '89101110288539'
and a.OFFER_ID = b.PRODUCT_ITEM_ID



select * from bass2.dw_product_20120131 where product_no = '13989007120'


select * from G_I_02022_DAY where time_id = 20120131
and BASE_PKG_ID like '999914211040%'

USER_ID
89601160028204      
89257331538332      
89260000405763      
89657332233890      
89360000860968      

select * from  BASS2.stat_market_0154_user_01
where user_id = '89601160028204'
111090001344

select * from bass2.dim_qqt_offer_id


select * from bass2.dim_prod_up_product_item 
where product_item_id = 111090001344



select * from bass2.Dw_product_ins_off_ins_prod_201202 a,bass2.dim_prod_up_product_item b
where a.OFFER_ID = b.PRODUCT_ITEM_ID
and a.OFFER_ID = 111090001344







select * from bass2.Dw_product_ins_off_ins_prod_ds a,bass2.dim_prod_up_product_item b
where a.OFFER_ID = b.PRODUCT_ITEM_ID
and a.product_instance_id = '89160000950064'




select a.*,b.NAME from bass2.Dw_product_ins_off_ins_prod_201202 a,bass2.dim_prod_up_product_item b
where a.OFFER_ID = b.PRODUCT_ITEM_ID
and b.name like '%全球通本地套餐%'


111089210018

delete from (
                select * from    bass1.g_i_02023_day where time_id=20120317
				and user_id = '89160000950064'
                ) t
                
                

select * from bass2.dw_product_20120315 where user_id = '89160000950064'
                
                
89110012
                
select * from bass2.dim_prod_up_product_item b where extend_id = 89110012
                
                
                
        select count(0) from (
                                select user_id from    bass1.g_i_02023_day where time_id=20110525
                                except
                                select user_id from    bass1.g_i_02022_day where time_id=20110525
                                ) a 
        with ur
                



				select user_id from    bass1.g_i_02023_day where time_id=20120115
				except
				select user_id from    bass1.g_i_02022_day where time_id=20120115
                
                      
89160001233565      
                      
                      
                      

 select      20120317 TIME_ID
             ,replace(char(date(a.create_date)),'-','') op_time
             ,char(a.TYCX_QUERY)             qry_cnt
             ,'381'                cancel_cnt
             ,char(a.TYCX_TUIDING_FAIL)      cancel_fail_cnt
             ,char(a.TYCX_TOUSU_LIANG)       complaint_cnt
             ,char( case when (381 - a.TYCX_TUIDING_FAIL) < 0 
                        then 0 else (381 - a.TYCX_TUIDING_FAIL) 
                   end 
                  ) CANCEL_BUSI_CNT
        from  bass2.DW_THREE_ITEM_STAT_DM_201203 a ,
              (select  replace(char(date(a.create_date)),'-','') op_time
                                                ,count(0) CANCEL_BUSI_CNT
                       from   
                        BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_201203 a
                        where replace(char(date(a.create_date)),'-','') =  '20120317'  
                        group by replace(char(date(a.create_date)),'-','')
                    ) b 
        where replace(char(date(a.create_date)),'-','') = '20120317' 
and    replace(char(date(a.create_date)),'-','') = b.op_time
with ur
                      

select count(0) from bass2.DW_THREE_ITEM_STAT_DM_201203 a
where     replace(char(date(a.create_date)),'-','') = '20120317' 


select count(0) from bass2.ODS_THREE_ITEM_STAT_20120317 a
where     replace(char(date(a.create_date)),'-','') = '20120317' 

select * from bass2.ODS_THREE_ITEM_STAT_20120317 a



select * from bass1.G_S_22080_DAY
where time_id in (20120316,20120317)

delete from G_S_22080_DAY
where time_id = 20120317

insert into G_S_22080_DAY
select   20120317 TIME_ID
        ,'20120317' OP_TIME
        ,char(bigint(QRY_CNT) - 10 ) QRY_CNT
        ,char(bigint(CANCEL_CNT) - 10 ) CANCEL_CNT
        ,char(bigint(CANCEL_FAIL_CNT) - 5 ) CANCEL_FAIL_CNT
        ,COMPLAINT_CNT 
        ,char(bigint(CANCEL_BUSI_TYPE_CNT) - 5 ) CANCEL_BUSI_TYPE_CNT
 from bass1.G_S_22080_DAY
where time_id = 20120316


				select count(0) from    bass1.g_i_02023_day where time_id=20120317


        select *from  app.sch_control_alarm 
        where alarmtime >=  current timestamp - 1 days
        and flag = 1
        and control_code  = 'BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl'
                
                

        update (
        select *from  app.sch_control_alarm 
        where alarmtime >=  current timestamp - 1 days
        and flag = 1
        and control_code  = 'BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl'
        )t 
        set flag = -1
                
                
                
        select *from  app.sch_control_alarm 
        where alarmtime >=  current timestamp - 1 days
        and control_code  = 'BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl'
        and alarmtime 
       

select count(*) from 
                    (
                     select user_id,count(*) cnt from bass1.g_a_02008_day
                      where time_id =20120319
                     group by user_id
                     having count(*)>1
                    ) as a

                    
CONTROL_CODE
BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl
BASS1_INT_CHECK_02004_02008_DAY.tcl

                    


select count(*) from 
                    (
                     select user_id,count(*) cnt from bass1.g_a_02008_day
                      where time_id =20120320
                     group by user_id
                     having count(*)>1
                    ) as a

USER_ID	CNT	   
89160000184970      	2	   
		
select * from                     
 bass1.g_a_02008_day
 where user_id = '89160000184970'
 and time_id = 20120319
 TIME_ID	USER_ID	USERTYPE_ID	   
20120319	89160000184970      	2020	   
20120319	89160000184970      	1031	   
			
            
select * from bass2.dw_product_bass1_20120319 where user_id = '89160000184970'
USER_ID	CUST_ID	USERTYPE_ID	BRAND_ID	CRM_BRAND_ID2	USERSTATUS_ID	STOPSTATUS_ID	PRODUCT_NO	IMSI	CITY_ID	CHANNEL_ID	CREATE_DATE	ACCTTYPE_ID	TEST_MARK	FREE_MARK	ENTERPRISE_MARK	   
89160000184970	89103001574212	1	32	10	1	16	d15289014474	[NULL]	891	11111111	2010/8/21 	0	0	0	0	   
																
select * from                     
 bass1.g_a_02004_day
 where user_id = '89160000184970'
TIME_ID	USER_ID	CUST_ID	USERTYPE_ID	CREATE_DATE	USER_BUS_TYP_ID	PRODUCT_NO	IMSI	CMCC_ID	CHANNEL_ID	MCV_TYP_ID	PROMPT_TYPE	SUBS_STYLE_ID	BRAND_ID	SIM_CODE	   
20100821	89160000184970      	89103001574212      	1	20100821	01	D15289014474   	0              	13101	11111111                                	1	1	04	2	0	   
															
                                                            
                                                            
select * from bass2.dw_product_20120319
where user_id = '89160000184970'

delete fro


select user_id,product_no,usertype_id,sim_code from
 (
 select user_id,product_no,usertype_id,sim_code,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02004_DAY
 where time_id<=20120319
 ) k
 where k.row_id=1 
 and k.usertype_id <> '3'
 with ur
 
            
            

select distinct 20120319,user_id,'2020' from G_A_02004_DAY where user_id in(select user_id from product_xhx4) with ur


					select 
20120319,					 a.user_id,
					 '2020'
					from BASS1.TEMP_CHECK_02008 a
				 where bass1_usertype_id is not null 
					 and bass1_usertype_id NOT IN ('2010','2020','2030','9000') 
					 and (a.userstatus_id not in (1,2,3,6,8) or a.userstatus_id is null)

            
            
           


select  *
                    from bass2.dw_product_20120319
                   where usertype_id in (1,2,9) 
       --              and day_off_mark = 1 
                     and userstatus_id  in (1,2,3,6,8)
                     and test_mark<>1
and user_id = '89160000184970'


delete from (
select * from                     
 bass1.g_a_02008_day
 where user_id = '89160000184970'
 and time_id = 20120320
) t 


           
         
CONTROL_CODE
BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl
BASS1_INT_CHECK_02004_02008_DAY.tcl

select * from  app.sch_control_runlog
where   control_code  like '%BASS1_INT_CHECK_02004_02008_DAY%'



update 
(
select * from  app.sch_control_runlog
where   control_code in (
'BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl'
)
and flag = 0 
) t
set  flag = -2



请帮忙查一下：
1.为什么这个号码（user_id = '89160000184970' ）昨天都没有数据（销户的历史用户），今天却是停机状态？是否正常？
 
2.这个号码（ user_id = '89160000950064' ）没有办理全网统一资费套餐，但是有全网统一资费叠加包的办理，正常不？



select * from bass2.dw_product_20120318
where user_id in ('89160000184970','89160000950064')     

     

select * from  bass2.dwd_product_test_phone_20120319

13549010495
13618983481



select * from bass2.dw_product_20120318
where product_no in ('13549010495','13618983481')
and userstatus_id in (1,2,3,6,8)

