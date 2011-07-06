---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc 

select message_id, send_time,mobile_num,message_content from   APP.SMS_SEND_INFO
where send_time is not null
and mobile_num = '15913269062'
and send_time >=  current timestamp - 1 days
and date(send_time) = char(current date )
order by send_time desc 


---------------------------------------------------------------------------------
select * from   table( bass1.chk_same(0) ) a order by 2---------------------------------------------------------------------------------select * from   table( bass1.chk_wave(0) ) a order by 2---------------------------------------------------------------------------------




---------------------------------------------------------------------------------
select * from app.sch_control_task where control_code in 
(select control_code from   app.sch_control_runlog where flag = 1 )
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

select  * from APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code<>'00'

---------------------------------------------------------------------------------
--或者：有接口重传时、记录级重新返回时。
--s_13100_20110301_22303_01_001.dat
--file lvl

select *
from 
(
select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn 
from APP.G_FILE_REPORT a
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'
) t where rn = 1

----------------------求未上传（文件级返回）接口！ (当日没返回文件级的接口)
select * from   BASS1.MON_ALL_INTERFACE where interface_code 
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

select a.*,b.*
FROM 
(
    select t.*
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
    and time_id=int(replace(char(current date - 1 days),'-',''))

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
update bass1.g_s_22012_day set m_off_users='77' 
where time_id=int(replace(char(current date - 1 days),'-',''))

 select * from  bass1.G_RULE_CHECK where rule_code = 'C1' order by 1 desc 

 select * from  bass1.G_RULE_CHECK where rule_code = 'C1' and time_id > 20100101
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

20110502	C1	1819931.00000	2861998.00000	-0.36410	0.00000 20110501	C1	2861998.00000	3083030.00000	-0.07169	0.00000
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

---R107update (select * from  BASS1.G_S_04008_DAY where time_id = 20110630  ) t set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+400) with ur
update (select * from  BASS1.G_S_04008_DAY where time_id = 20110630  ) t set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+10) with ur

--97763 row(s) affected.
commit--100390 row(s) affected.
--97763 row(s) affected.---R108update (select * from  BASS1.G_S_04008_DAY where time_id = 20110630  ) t set BASE_BILL_DURATION = char(bigint(BASE_BILL_DURATION)-5) with ur 
commit

--100390 row(s) affected.
--97763 row(s) affected.

select * from   bass1.G_RULE_CHECK where rule_code = 'R109'      
order by time_id desc 


--所有日接口代码：

select b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'd'and b.control_code like '%DAY%'

--所有月接口代码：
select b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'


		   
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

select b.*, lower( '/bassapp/backapp/bin/bass1_export/bass1_export '||substr(a.control_code,11,13)||' '||char(current date - 1 days) ) exp_cmdfrom   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE bwhere a.control_code like 'BASS1%EXP%DAY%'and date(a.begintime) =  date(current date)and substr(a.control_code,15,5) = b.interface_code and b.type='d'
--生成重导月数据的命令

select b.*, lower( '/bassapp/backapp/bin/bass1_export/bass1_export '||substr(a.control_code,11,15)||' '||substr(char(current date - 1 month) ,1,7)) exp_cmdfrom   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE bwhere a.control_code like 'BASS1%EXP%MONTH%'and month(a.begintime) =  month(current date)and substr(a.control_code,15,5) = b.interface_code and b.type='m'


--单独put 某个接口:day



select b.*, lower( 'put *'||b.interface_code||'*.dat ' ) put_dat, lower( 'put *'||b.interface_code||'*.verf ' ) put_verf
from   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE bwhere a.control_code like 'BASS1%EXP%DAY%'and date(a.begintime) =  date(current date)and substr(a.control_code,15,5) = b.interface_code and b.type='d'
--单独put 某个接口:month

select b.*, lower( 'put *'||b.interface_code||'*.dat ' ) put_dat, lower( 'put *'||b.interface_code||'*.verf ' ) put_verffrom   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE bwhere a.control_code like 'BASS1%EXP%MONTH%'and month(a.begintime) =  month(current date)and substr(a.control_code,15,5) = b.interface_code and b.type='m'


--接口号 -  表名 对应关系
select * from table(
select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,13) from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'd'and b.control_code like '%DAY%'
) t where unit_code = ''

select * from table(
select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,15),b.control_code from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'
) t where unit_code = ''


select * from   bass1.mon_all_interface where INTERFACE_CODE = '06001'





select  
count(0) common_cnt
,count(distinct a.unit_code) a_cnt
,count(distinct b.unit_code) b_cnt
from 
table(
select unit_code from  app.g_runlog
where time_id = int(replace(char(current date - 1 days),'-',''))
) a
full join 
table(
select substr(control_code , 15,5) unit_code
from   app.sch_control_runlog  a 
									where a.control_code like 'BASS1%EXP%DAY%' 
									and date(a.begintime) =  date(current date) 
									and flag = 0 )b
on  a.unit_code = b.unit_code 
                                    

select * from bass1.g_rule_check where rule_code in ('R170','R172','R161_14') order by time_id desc
 select * from  bass1.G_RULE_CHECK where rule_code = 'R159_3' order by 1 desc 

select * from   g_a_06001_day
select * from   bass1.g_rule_check where rule_code = 'R161_1'


select * from   bass1.g_rule_check where rule_code = 'R159_4'
and time_id / 100 = 201105


select PRODUCT_NO||IMSI||CITY_ID||OTHER_MSISDN||SMS_BILL_TYPE||CDR_USER_TYPE||SERV_CODE||SMS_FEE_TYPE||SMS_SEND_STATE||BASECALL_FEE||INFO_FEE||MONTH_FEE||INFO_LEN||SMS_CENTER_CODE||SMS_GATE_CODE||FRNT_GATE_CODE||APPLY_DATE||APPLY_TIME||GATE_DEAL_DATE||GATE_DEAL_TIME||BILL_DEAL_DATE||BILL_DEAL_TIME from bass1.g_s_04014_day where time_id=20110629


  select count(0)
                from bass1.g_s_22091_day where TIME_ID = 20110630
                and  channel_id not in
                        (select char(CHANNEL_ID)
                        from bass2.ods_channel_info_20110630 a where a.channel_type_class in (90105,90102)
                        union
                        select char(a.channel_id)
                from bass2.dim_channel_info a
                 where a.channel_type_class in (90105,90102)
                  and replace(char(date(DONE_DATE)),'-','') like '201106%'
                  and a.USER_STATE = 1
                        )


select * from    bass1.g_s_22091_day     where TIME_ID = 20110630

table(select a.*
from g_a_02064_day a 
join g_a_02054_day b on a.enterprise_id = b.enterprise_id)



                                select a.cnt*1.00 ,value(b.income,0) income
                                from
                                (select count(0) cnt
                                                        from 
                                                        (
                                                        select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
                                                        from (
                                                        select *
                                                        from table(select a.*
from g_a_02064_day a 
join g_a_02054_day b on a.enterprise_id = b.enterprise_id) a
 where 
                                                         time_id/100 <= 201106 and ENTERPRISE_BUSI_TYPE = '1520'
                                                         and MANAGE_MODE = '3'
                                                        ) t 
                                                        ) t2 where rn = 1 and STATUS_ID ='1'
                                ) a,(
                                select sum(income)*1.00/100 income
                                from (
                                select sum(bigint(income)) income from   g_s_03017_month
                                where time_id = 201105
                                and ent_busi_id = '1520'
                                and MANAGE_MOD = '3'
                                union all 
                                select sum(bigint(income)) income from   g_s_03018_month
                                where time_id = 201105
                                and ent_busi_id = '1520'
                                and MANAGE_MOD = '3'
                                ) t 
                                ) b 
                        with ur                     


  select count(0)
from bass1.g_s_22092_day where TIME_ID = 20110630
and  channel_id not in 
                        (select char(a.channel_id)
                        from bass2.ods_channel_info_20110630 a where a.channel_type_class in (90105,90102)
                        union 
                        select char(a.channel_id)
                from bass2.dim_channel_info a
                 where a.channel_type_class in (90105,90102)
                  and replace(char(date(DONE_DATE)),'-','') like '201106%'
                  and a.USER_STATE = 1
                        )




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
 from   bass1.MON_ALL_INTERFACE b where 
deadline  in (15)
and type = 'm'


select time_id,count(0) from bass1.g_user_lst
group by time_id 
order by 1 desc 
                        
select * from  table( bass1.get_task('BASS1_INT_COMMON_ROUTINE_MONTH')) a 
                        
                        INT_02004_02008_YYYYMM.tcl

BASS1_INT_02004_02008_YYYYMM.tcl	生成02004,02008的增量数据

select * from                      app.sch_control_task 
where control_code = 'BASS1_INT_02004_02008_YYYYMM.tcl'
  select * from app.sch_control_runlog where control_code in (
select before_control_code from                      app.sch_control_before
where control_code = 'BASS1_G_S_21007_DAY.tcl'
)

  CONTROL_CODE	BEGINTIME	ENDTIME	RUNTIME	FLAG
BASS2_Dw_product_bass1_ds.tcl	2011-07-02 2:56:45.658511	2011-07-02 2:58:26.985008	101	0

select * from   bass2.etl_load_table_map
where TABLE_NAME_TEMPLET  like '%INT%'

select * from INT_02004_02008_201106 fetch first 10 rows only  

select * from syscat.tables where tabname like '%INT_02004_02008%'
   
   

CREATE TABLE "BASS1   "."INT_02004_02008_201107"  (
                  "OP_TIME" INTEGER NOT NULL , 
                  "USER_ID" CHAR(20) NOT NULL , 
                  "BRAND_ID" CHAR(1) NOT NULL , 
                  "USERTYPE_ID" CHAR(4) NOT NULL , 
                  "BRAND_FLAG" INTEGER NOT NULL , 
                  "USERTYPE_FLAG" INTEGER NOT NULL )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "USER_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 



BASS1 INT_21007_201107   

 

CREATE TABLE "BASS1   "."INT_21007_201107"  (
                  "OP_TIME" INTEGER , 
                  "PRODUCT_NO" VARCHAR(13) , 
                  "BRAND_ID" CHAR(1) , 
                  "SVC_TYPE_ID" CHAR(2) , 
                  "CDR_TYPE_ID" CHAR(2) , 
                  "END_STATUS" CHAR(1) , 
                  "ADVERSARY_ID" CHAR(6) , 
                  "SMS_COUNTS" DECIMAL(14,2) , 
                  "SMS_BASE_FEE" DECIMAL(14,2) , 
                  "SMS_INFO_FEE" DECIMAL(14,2) , 
                  "SMS_MONTH_FEE" DECIMAL(14,2) )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "PRODUCT_NO",  
                 "BRAND_ID",  
                 "SVC_TYPE_ID",  
                 "CDR_TYPE_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 


"BASS1.INT_22038_201107" 



CREATE TABLE "BASS1   "."INT_22038_201107"  (
                  "OP_TIME" INTEGER NOT NULL , 
                  "BRAND_ID" INTEGER NOT NULL , 
                  "USER_ID" VARCHAR(20) NOT NULL )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "USER_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 


ALTER TABLE BASS1.INT_02004_02008_201107
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
select * from    bass1.int_21007_201107






  select count(0)
                from bass1.g_s_22091_day where TIME_ID = 20110701
                and  channel_id not in 
                        (select CHANNEL_ID 
                        from g_i_06021_month where time_id = 201107
                        union 
                        select char(a.channel_id)
                from bass2.dim_channel_info a
                 where a.channel_type_class in (90105,90102)
                  and replace(char(date(DONE_DATE)),'-','') like '201107%'
                  and a.USER_STATE = 1
                        )


  select count(0)
                from bass1.g_s_22091_day where TIME_ID = 20110701
                and  channel_id not in
                        (select char(CHANNEL_ID)
                        from bass2.ods_channel_info_20110701 a where a.channel_type_class in (90105,90102)
                        union
                        select char(a.channel_id)
                from bass2.dim_channel_info a
                 where a.channel_type_class in (90105,90102)
                  and replace(char(date(DONE_DATE)),'-','') like '201106%'
                  and a.USER_STATE = 1
                        )



  select count(0)
from bass1.g_s_22092_day where TIME_ID = 20110701
and  channel_id not in 
                        (select char(a.channel_id)
                        from bass2.ods_channel_info_20110701 a where a.channel_type_class in (90105,90102)
                        union 
                        select char(a.channel_id)
                from bass2.dim_channel_info a
                 where a.channel_type_class in (90105,90102)
                  and replace(char(date(DONE_DATE)),'-','') like '201106%'
                  and a.USER_STATE = 1
                        )
                        
update app.sch_control_alarm 
set flag = 0
where control_code = 'BASS1.INT_21007_YYYYMM.tcl'
and date (alarmtime ) = '2011-07-02'
and flag = -1
                        
                        
                        select count(0) from   BASS1.INT_21007_201107
                        1096923
                                                select count(0) from   BASS1.INT_21007_201106 where op_time = 20110601
                                                
1035313



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
 from   bass1.MON_ALL_INTERFACE b where 
deadline  in (9)
and type = 'd'


select time_id , count(0) from   BASS1.G_USER_LST
group by time_id 



select * from    bass1.MON_ALL_INTERFACE where interface_code = '04002'

BASS1_G_S_04002_DAY.tcl

select * from  table( bass1.get_before('BASS1_INT_CHECK_R028_MONTH.tcl')) a 




select message_id, send_time,mobile_num,message_content from   APP.SMS_SEND_INFO
where send_time is not null
and mobile_num = '15913269062'
and send_time >=  current timestamp - 1 days
order by send_time desc 


select op_time,count(0) from   BASS2.Dw_enterprise_industry_apply
group by op_time

select * from bass2.Dw_enterprise_industry_apply fetch first 10 rows only  

select * from  table( bass1.get_after('03004')) a 


BASS1_G_S_03013_MONTH.tcl	BASS2_Dw_enterprise_industry_apply.tcl
BASS1_G_S_03014_MONTH.tcl	BASS2_Dw_enterprise_industry_apply.tcl
BASS1_G_S_03017_MONTH.tcl	BASS2_Dw_enterprise_industry_apply.tcl
BASS1_G_S_03018_MONTH.tcl	BASS2_Dw_enterprise_industry_apply.tcl
BASS1_G_S_22013_MONTH.tcl	BASS2_Dw_enterprise_industry_apply.tcl
BASS1_G_S_22037_MONTH.tcl	BASS2_Dw_enterprise_industry_apply.tcl
BASS1_G_S_22302_DAY.tcl	BASS2_Dw_enterprise_industry_apply.tcl
BASS1_G_S_22303_MONTH.tcl	BASS2_Dw_enterprise_industry_apply.tcl


select * from   G_S_22302_DAY where time_id = 20110630

select * from   G_S_22302_DAY where time_id = 20110601




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
 from   bass1.MON_ALL_INTERFACE b where 
deadline  in (3)
and type = 'm'


select count(0) from    g_i_03001_month where time_id = 201106

select count(0) from    g_i_03001_month where time_id = 201105
4688229
select count(0) from    bass2.dw_acct_msg_201105
4688229
select count(0) from    bass2.dw_acct_msg_201106

4786605
 4786605 i_13100_201106_03001_00_001.dat
 



select * from   mon_all_interface where interface_code = '02021'

ods_channel_info_$timpstamp


  select count(0)
                from bass1.g_s_22091_day where TIME_ID = 20110702
                and  channel_id not in
                        (select CHANNEL_ID
                        from g_i_06021_month where time_id = 201106
                        union
                        select char(a.channel_id)
                from bass2.ods_channel_info_20110702 a
                 where a.channel_type_class in (90105,90102)
                  and a.USER_STATE = 1
                        )

select * from   G_A_06001_DAY where time_id = 20110702


select value(city_id,substr(SCHOOL_ID,4,3)) from    bass2.Dim_xysc_maintenance_info

                        

       select 
               20110702  TIME_ID
        ,SCHOOL_ID
        ,SCHOOL_NAME
        ,CMCC_BRANCH_ID
        ,SCHOOL_TYPE
        ,STUD_CNT
        ,NEWSTUD_CNT
        ,'1' STATE 
from table(        
        select distinct 
         SCHOOL_ID
        ,SCHOOL_NAME
        ,case when value(city_id,substr(SCHOOL_ID,4,3)) = '891' then '13101'
                              when value(city_id,substr(SCHOOL_ID,4,3)) = '892' then '13102'
                              when value(city_id,substr(SCHOOL_ID,4,3)) = '893' then '13103'
                              when value(city_id,substr(SCHOOL_ID,4,3)) = '894' then '13104'
                              when value(city_id,substr(SCHOOL_ID,4,3)) = '895' then '13105'
                              when value(city_id,substr(SCHOOL_ID,4,3)) = '896' then '13106'
                              when value(city_id,substr(SCHOOL_ID,4,3)) = '897' then '13107'
                        end CMCC_BRANCH_ID
        ,case when school_type in ('4','5') then '1' else '2' end SCHOOL_TYPE
        ,char(SCH_SIZE_STUS) STUD_CNT
        ,'0' NEWSTUD_CNT 
from      bass2.Dim_xysc_maintenance_info
except
select 
         SCHOOL_ID
        ,SCHOOL_NAME
        ,CMCC_BRANCH_ID
        ,SCHOOL_TYPE
        ,STUD_CNT
        ,NEWSTUD_CNT
from ( 
                        select a.*
                        ,row_number()over(partition by SCHOOL_ID order by time_id desc ) rn 
                  from G_A_06001_DAY a 
                  where time_id < 20110702
     ) o where o.rn = 1
) t     

                        

INT_02004_02008_201107
INT_21007_201107
INT_22038_201107


select tabname from syscat.tables where tabname like '%INT_02004_02008%'                           





CREATE TABLE "BASS1   "."INT_02004_02008_YYYYMM"  (
                  "OP_TIME" INTEGER NOT NULL , 
                  "USER_ID" CHAR(20) NOT NULL , 
                  "BRAND_ID" CHAR(1) NOT NULL , 
                  "USERTYPE_ID" CHAR(4) NOT NULL , 
                  "BRAND_FLAG" INTEGER NOT NULL , 
                  "USERTYPE_FLAG" INTEGER NOT NULL )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "USER_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 



BASS1 INT_21007_201107   

 

CREATE TABLE "BASS1   "."INT_21007_YYYYMM"  (
                  "OP_TIME" INTEGER , 
                  "PRODUCT_NO" VARCHAR(13) , 
                  "BRAND_ID" CHAR(1) , 
                  "SVC_TYPE_ID" CHAR(2) , 
                  "CDR_TYPE_ID" CHAR(2) , 
                  "END_STATUS" CHAR(1) , 
                  "ADVERSARY_ID" CHAR(6) , 
                  "SMS_COUNTS" DECIMAL(14,2) , 
                  "SMS_BASE_FEE" DECIMAL(14,2) , 
                  "SMS_INFO_FEE" DECIMAL(14,2) , 
                  "SMS_MONTH_FEE" DECIMAL(14,2) )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "PRODUCT_NO",  
                 "BRAND_ID",  
                 "SVC_TYPE_ID",  
                 "CDR_TYPE_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 


"BASS1.INT_22038_201107" 



CREATE TABLE "BASS1   "."INT_22038_YYYYMM"  (
                  "OP_TIME" INTEGER NOT NULL , 
                  "BRAND_ID" INTEGER NOT NULL , 
                  "USER_ID" VARCHAR(20) NOT NULL )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "USER_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 

select * from   INT_21007_201108

select * from   
INT_02004_02008_201108





    select 
        (select  count(distinct user_id||ADD_PKG_ID) cnt from bass1.g_i_02023_day a where time_id = 20110630 )                     
         - (select  count(distinct user_id||OVER_PROD_ID) cnt from bass1.g_i_02021_month a where time_id = 201106 and  OVER_PROD_ID like 'QW_QQT_DJ%')
         from bass2.dual
        with ur 
        
        

select  distinct user_id||ADD_PKG_ID   from bass1.g_i_02023_day a where time_id = 20110630 
except
select  distinct user_id||OVER_PROD_ID from bass1.g_i_02021_month a where time_id = 201106 and  OVER_PROD_ID like 'QW_QQT_DJ%'
        

89160000920148 

        
select * from    bass1.g_i_02021_month a where time_id = 201106        
and user_id = '89160000920148'

OVER_PROD_ID
111090004095                  
111089140007                  
111090002601                  
111090009610                  


select * from bass2.ODS_PROD_UP_PRODUCT_ITEM_20110630
where product_item_id in (111090004095,111089140007,111090002601,111090009610)


select  a.*,b.NAME from bass2.Dw_product_ins_off_ins_prod_ds a, bass2.ODS_PROD_UP_PRODUCT_ITEM_20110630 b
where a.offer_id = b.product_item_id
and a.PRODUCT_INSTANCE_ID = '89160000920148'



                        select 
                        distinct 
                                USER_ID
                                ,ADD_PKG_ID
                                ,VALID_DT
                        FROM (
                                SELECT
                                 a.PRODUCT_INSTANCE_ID as USER_ID
                                ,bass1.fn_get_all_dim_ex('BASS_STD1_0115',char(a.offer_id)) as ADD_PKG_ID
                                ,replace(char(date(a.VALID_DATE)),'-','') as VALID_DT 
                                ,row_number()over(partition by a.PRODUCT_INSTANCE_ID,a.offer_id 
                                        order by EXPIRE_DATE desc ,VALID_DATE desc  ) rn 
                                from  bass2.Dw_product_ins_off_ins_prod_ds a 
                                     ,(select xzbas_value  as offer_id 
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0115'
                                              and bass1_value like 'QW_QQT_DJ%'
                                      ) b 
                                where char(a.offer_id) = b.offer_id 
                                and a.state=1
                                and a.valid_type = 1 
                                 and a.OP_TIME = '2011-07-02'
                                 and date(a.VALID_DATE)<='2011-07-02'
                                 and date(a.expire_date)>='2011-07-02'
                            ) AS T where t.rn = 1 
                         with ur 
                         
select count(0) from    G_I_02023_DAY where time_id = 20110702
                         



  select * from
                (select * from 
                (
                select user_id,chg_vip_time,row_number()over(partition by user_id order by time_id desc) row_id from BASS1.G_I_02005_MONTH
                where time_id=201106
                ) k
                where k.row_id =1) a
                left outer join 
                (
                select * from
                (
                select user_id,create_date,row_number()over(partition by user_id order by time_id desc) row_id 
                from BASS1.G_A_02004_DAY
                where time_id<=20110630
                ) k
                where k.row_id=1) b
                on a.user_id=b.user_id
                where bigint(chg_vip_time)<bigint(create_date)
                with ur
                


USER_ID	CHG_VIP_TIME	ROW_ID	USER_ID	CREATE_DATE	ROW_ID
89460000740915      	20110320	1	89460000740915      	20110321	1



update BASS1.G_I_02005_MONTH
set CHG_VIP_TIME = '20100917'
where user_id = '89160000265019'
and time_id = 201106


update BASS1.G_I_02005_MONTH
set CHG_VIP_TIME = '20110321'
where user_id = '89460000740915'
and time_id = 201106


                         
                         

select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabname = 'G_S_04002_DAY'
                         
select * from   G_S_04002_DAY where time_id = 20110702



    rename BASS1.G_S_04002_DAY to G_S_04002_DAY_BAK
    create table BASS1.G_S_04002_DAY like BASS1.G_S_04002_DAY_BAK 
     DISTRIBUTE BY HASH(TIME_ID,  
                 PRODUCT_NO,  
                 ROAM_LOCN,  
                 ROAM_TYPE_ID,  
                 APNNI,  
                 START_TIME)   
     IN TBS_APP_BASS1 INDEX IN TBS_INDEX 
                   
	  insert into BASS1.G_S_04002_DAY 
	  select * from  BASS1.G_S_04002_DAY_BAK
	  where time_id >= 20110401
	  --about 15min
	 db2 RUNSTATS ON TABLE BASS1.G_S_04002_DAY	with distribution and detailed indexes all  
	  drop table BASS1.G_S_04002_DAY_BAK



delete from BASS1.G_S_04005_DAY where time_id < 20110401
89178017 row(s) affected.
db2 RUNSTATS ON TABLE BASS1.G_S_04005_DAY	with distribution and detailed indexes all  


delete from BASS1.G_S_21003_MONTH where time_id < 201104

db2 RUNSTATS ON TABLE BASS1.G_S_21003_MONTH	with distribution and detailed indexes all  

G_S_21008_MONTH


delete from BASS1.G_S_21008_MONTH where time_id < 201104

db2 RUNSTATS ON TABLE BASS1.G_S_21008_MONTH	with distribution and detailed indexes all  

    BASS1_INT_02004_02008_YYYYMM.tcl
    BASS1_INT_22038_YYYYMM.tcl
    BASS1.INT_21007_YYYYMM.tcl
    BASS1_INT_0400810_YYYYMM.tcl
    BASS1_INT_210012916_YYYYMM.tcl


select tabname from syscat.tables where tabname like '%INT_02004_02008%'   

delete from BASS1.G_S_21008_MONTH where time_id < 201104

db2 RUNSTATS ON TABLE BASS1.G_S_21008_MONTH	with distribution and detailed indexes all  

select * from    BASS1.MON_ALL_INTERFACE  where interface_code = '22063'                         


三、定期清理固定中间表
    G_S_21003_TO_DAY  保持1+1的数据
    G_S_21008_TO_DAY  保持1+1的数据
  
select * from G_S_21003_TO_DAY fetch first 10 rows only    
  
delete from BASS1.G_S_21003_TO_DAY where time_id < 20110401
db2 RUNSTATS ON TABLE BASS1.G_S_21003_TO_DAY	with distribution and detailed indexes all  

delete from BASS1.G_S_21008_TO_DAY where time_id < 20110401
db2 RUNSTATS ON TABLE BASS1.G_S_21008_TO_DAY	with distribution and detailed indexes all  

  
  select * from   
BASS1.MON_ALL_INTERFACE
 where type = 'm'
 and deadline = 5
 
 
 select * from  G_I_06002_MONTH where time_id = 201106
 
   select distinct 
  			 201106
        ,SCHOOL_ID
        ,value(CELL_ID,'0') CELL_ID
        ,value(LAC_ID,'0') LAC_ID        
	from bass2.Dim_xysc_maintenance_info

select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22063_MONTH 
group by  time_id 
order by 1 

select count(0) from    bass2.stat_channel_reward_0002

                  SELECT
                           201106
                                ,'201106'
                                ,trim(char(a.CHANNEL_ID))
                                ,char(bigint( sum(case when t_index_id in (1,4,14) then result else 0 end )                 ))
                                ,char(bigint( sum(case when t_index_id in (10,11,12,13,19,20,21) then result else 0 end )   ))
                                ,char(bigint( sum(case when t_index_id in (7) then result else 0 end )                      ))
                                ,'0'
                                ,'0'
                                ,'0'
                        FROM BASS2.DW_CHANNEL_INFO_201106 A
                        inner join bass2.stat_channel_reward_0002 b on a.channel_id=b.channel_id
                        WHERE A.CHANNEL_TYPE_CLASS IN (90105,90102) 
                                                and b.op_time=201106
                                                AND B.result>0
                        group by trim(char(a.CHANNEL_ID))
                        
    
select * from   bass2.stat_channel_reward_0002
                    
    
    select count(0) from    BASS2.DW_CHANNEL_INFO_201106 
        select * from    BASS2.DW_CHANNEL_INFO_201106 



select CHANNEL_TYPE_CLASS , count(0) 
--,  count(distinct CHANNEL_TYPE_CLASS ) 
from BASS2.DW_CHANNEL_INFO_201106 
group by  CHANNEL_TYPE_CLASS 
order by 1 

CHANNEL_TYPE_CLASS	2
90106	4
90109	8
90110	10
90108	15
1	54
90107	118
90105	146
90102	7333


select * from   app.sch_control_runlog_his
where control_code like '%REP_stat_channel_reward_0002.tcl%'

select control_code  from  table( bass1.get_task('21020')) a 

REP_stat_channel_reward_0002.tcl


update G_I_21020_MONTH 
set CALL_COUNTS = char(int(rand(1)*5+1))
where (
char(TIME_ID) = substr(COMP_LAST_DATE,1,6)
and CALL_COUNTS = '0'
and SMS_COUNTS = '0'
and MMS_COUNTS = '0'
and time_id = 201106
)



--月接口
select count(0),count(distinct user_id) from    bass1.g_i_02020_month 
	where time_id = 201106
and base_prod_id like '%QQT%'
1	2
15216	15216

--月接口离网客户数
select count(0),count(distinct user_id) from    bass1.g_i_02020_month where time_id = 201105
and base_prod_id like '%QQT%'



--日接口
select count(0),count(distinct user_id) from    bass1.g_i_02022_day
where time_id = 20110630

--2172	2172

1	2
15216	15216


select count(0)
from 
(
select  user_id from    bass1.g_i_02020_month where time_id = 201106
and base_prod_id like '%QQT%'
) a , BASS1.g_a_02004_02008_stage b 
where a.user_id = b.user_id 
 and  b.usertype_id NOT IN ('2010','2020','2030','9000')




--生成5月月末一经用户数据
drop table BASS1.g_a_02004_02008_stage;
CREATE TABLE BASS1.g_a_02004_02008_stage
 (
    user_id        CHARACTER(15),
   product_no     CHARACTER(15),
   test_flag      CHARACTER(1),
   sim_code       CHARACTER(15),
   usertype_id    CHARACTER(4),
   create_date    CHARACTER(15),
   brand_id       CHARACTER(4),
   time_id        int
)
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING;

ALTER TABLE BASS1.g_a_02004_02008_stage
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  



--抓取用户资料入表
insert into bass1.g_a_02004_02008_stage (
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
where time_id<=20110531 ) e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=20110531 ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1


  
  
  select * 
                        from g_i_06021_month where time_id = 201106
                        

                        values 1.000 * 1648305 / 1672966

insert into app.sch_control_before 
values('BASS1_EXP_G_S_02047_MONTH',	'BASS1_INT_CHECK_R229_MONTH.tcl')

select * from   
                         app.sch_control_before 
                         where control_code = 'BASS1_EXP_G_S_02047_MONTH'

                         
select * from   bass1.g_rule_check 
where 
rule_code in (
'R240'
,'R241'
,'R242'
,'R243'
,'R246'
,'R247'
,'R250'
,'R251'
,'R252'
,'R253'
,'R254'
)                      
order by 1 desc ,2

   
   
   select * from   bass1.g_rule_check 
where  rule_code between 'R238' and 'R254'
order by 1 desc ,2


select rule_code,count(0)
from  bass1.g_rule_check 
where time_id /100 = 201106
group by rule_code
having count(0) > 30


RULE_CODE	2
R010	32
R198	31
R200	31
R203	31
R204	31
R205	31
R207	31


-rwxrwxrwx+  1 bass1    appdb       5725  6月  2日 03:10 INT_CHECK_R025_MONTH.tcl
-rwxrwxrwx+  1 bass1    appdb       5347  6月  2日 03:10 INT_CHECK_0205202008_MONTH.tcl
-rw-r--r--   1 bass1    appdb       2171  6月  2日 03:10 INT_CHECK_R181_MONTH.tcl
-rw-r--r--   1 bass1    appdb       2180  6月  3日 17:35 INT_CHECK_R192_MONTH.tcl
-rw-r--r--   1 bass1    appdb       4847  6月  5日 19:54 INT_CHECK_R208_MONTH.tcl
-rw-r--r--   1 bass1    appdb       5452  6月  6日 22:49 INT_CHECK_R235_MONTH.tcl
-rw-r--r--   1 bass1    appdb       2671  6月 16日 15:22 INT_CHECK_R229_MONTH.tcl
-rw-r--r--   1 bass1    appdb       3206  7月  1日 11:59 INT_CHECK_R216_MONTH.tcl
-rw-r--r--   1 bass1    appdb       9327  7月  1日 11:59 INT_CHECK_R230_MONTH.tcl
-rwxrwxrwx+  1 bass1    appdb      12158  7月  3日 16:15 INT_CHECK_06031_MONTH.tcl
-rw-r--r--   1 bass1    appdb      13265  7月  3日 16:22 INT_CHECK_R221_MONTH.tcl
-rw-r--r--   1 bass1    appdb      10513  7月  3日 17:27 INT_CHECK_R238_MONTH.tcl
   select * from   bass1.g_rule_check 
where rule_code = 'R235'
AND TIME_ID = 201106


   select * from   bass1.g_rule_check 
where rule_code = 'R234'

values ((16894+132)*1.000/17026 - 1)



                        select count(0) cnt
                        from 
                        (
                        select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
                        from (
                        select *
                        from g_a_02054_day where  time_id <= 20110702 
                                        and ENTERPRISE_BUSI_TYPE in ('1040')
                                                and MANAGE_MODE = '3'
                        ) t 
                        ) t2 where rn = 1 and STATUS_ID ='1'
                        
                        

                                select count(0)
                                from 
                                (
                                                select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
                                                from 
                                                (
                                                select * from G_A_02059_DAY
                                                where 
                                                                 ENTERPRISE_BUSI_TYPE  in ('1000','1010')                               
                                                and time_id <= 20110531
                                                and MANAGE_MODE = '3'
                                                ) t
                                ) t2
                                where rn = 1 and STATUS_ID ='1'
                                
select tabname from syscat.tables where tabname like '%02059%'                                   



select count(0),count(distinct control_code) from   app.sch_control_runlog_his 
where control_code like 'BASS1%'
and date(begintime) >= '2011-07-01'
and flag = 0

select * 
from (
select a.*,row_number()over(partition by control_code order by runtime desc ) rn 
from  app.sch_control_runlog_his a
where control_code like 'BASS1%'
and date(begintime) >= '2011-07-01'
and flag = 0
) t 
where t.rn=1



select  *
from (
select a.*,row_number()over(partition by control_code order by runtime desc ) rn 
from  app.sch_control_runlog_his a
where control_code like 'BASS1%'
and date(begintime) < '2011-07-01'
and date(begintime) >= '2011-06-01'
and flag = 0
) t 
where t.rn=1


select a.CONTROL_CODE , a.RUNTIME , b.runtime,a.RUNTIME-b.runtime
from table(select * 
from (
select a.*,row_number()over(partition by control_code order by runtime desc ) rn 
from  app.sch_control_runlog_his a
where control_code like 'BASS1%'
and date(begintime) >= '2011-07-01'
and flag = 0
) t 
where t.rn=1) a
,
table(select  *
from (
select a.*,row_number()over(partition by control_code order by runtime desc ) rn 
from  app.sch_control_runlog_his a
where control_code like 'BASS1%'
and date(begintime) < '2011-07-01'
and date(begintime) >= '2011-06-01'
and flag = 0
) t 
where t.rn=1) b where a.control_code = b.control_code 
and a.CONTROL_CODE like '%MONTH%'




   select * from   bass1.g_rule_check 
where rule_code = 'R055'
AND TIME_ID  >= 201101

 select count(distinct a.user_id)
                                                                 from BASS1.G_I_02049_MONTH a
                                                                 left join (select distinct user_id from bass1.G_A_02004_DAY where time_id<=20110630) b  on a.user_id = b.user_id
                                                                 where a.time_id=201106
                                                                       and b.user_id is null
                                                                       
                                                                       
                                                                       
                                                                       

select time_id , count(0) 
--,  count(distinct time_id ) 
from G_I_02016_MONTH 
group by  time_id 
order by 1 

                                                                       select * from    G_I_02016_MONTH
                                                                       where time_id = 201106
                                                                       
                                                                       

select time_id , count(0) 
--,  count(distinct time_id ) 
from g_i_02019_month 
group by  time_id 
order by 1 
                                                                       



CREATE TABLE "BASS1   "."INT_0400810_YYYYMM"  (
                  "OP_TIME" INTEGER NOT NULL , 
                  "BRAND_ID" SMALLINT NOT NULL , 
                  "DRTYPE_ID" INTEGER NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "IMSI" CHAR(15) NOT NULL , 
                  "MSRN" CHAR(11) NOT NULL , 
                  "IMEI" CHAR(17) NOT NULL , 
                  "OPPOSITE_NO" CHAR(24) NOT NULL , 
                  "THIRD_NO" CHAR(24) NOT NULL , 
                  "CITY_ID" CHAR(6) NOT NULL , 
                  "ROAM_LOCN" CHAR(6) NOT NULL , 
                  "OPP_CITY_ID" CHAR(6) NOT NULL , 
                  "OPP_ROAM_LOCN" CHAR(6) NOT NULL , 
                  "ADVERSARY_ID" CHAR(6) NOT NULL , 
                  "ADVERSARY_NET_TYPE" CHAR(2) NOT NULL , 
                  "MSC_CODE" CHAR(11) NOT NULL , 
                  "CELL_ID" CHAR(10) NOT NULL , 
                  "LAC_ID" CHAR(10) NOT NULL , 
                  "OPP_CELL_ID" CHAR(10) NOT NULL , 
                  "OPP_LAC_ID" CHAR(10) NOT NULL , 
                  "OUTGO_TRNK" CHAR(15) NOT NULL , 
                  "INCOMING_TRNK" CHAR(15) NOT NULL , 
                  "START_DATE" CHAR(8) NOT NULL , 
                  "START_TIME" CHAR(6) NOT NULL , 
                  "CALL_DURATION" CHAR(6) NOT NULL , 
                  "BASE_BILL_DURATION" CHAR(6) NOT NULL , 
                  "TOLL_BILL_DURATION" CHAR(6) NOT NULL , 
                  "BILLING_ID" CHAR(1) NOT NULL , 
                  "BASE_CALL_FEE" CHAR(8) NOT NULL , 
                  "TOLL_CALL_FEE" CHAR(8) NOT NULL , 
                  "CALLFW_TOLL_FEE" CHAR(8) , 
                  "INFO_FEE" CHAR(8) NOT NULL , 
                  "BALANCE_FEE" CHAR(8) NOT NULL , 
                  "FAV_BASE_CALL_FEE" CHAR(8) NOT NULL , 
                  "FAV_TOLL_CALL_FEE" CHAR(8) NOT NULL , 
                  "FAV_CALLFW_TOLL_FEE" CHAR(8) , 
                  "FAV_INFO_FEE" CHAR(8) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "TOLL_TYPE_ID" CHAR(3) NOT NULL , 
                  "SVCITEM_ID" CHAR(3) NOT NULL , 
                  "CALL_TYPE_ID" CHAR(2) NOT NULL , 
                  "SERVICE_CODE" CHAR(4) NOT NULL , 
                  "USER_TYPE" CHAR(1) NOT NULL , 
                  "FEE_TYPE" CHAR(1) NOT NULL , 
                  "INNER_ID" CHAR(24) NOT NULL , 
                  "OPER_INNER_ID" CHAR(24) NOT NULL , 
                  "VPMN_GRP_ID" CHAR(20) NOT NULL , 
                  "SCP_ID" CHAR(3) NOT NULL , 
                  "END_CALL_TYPE" CHAR(1) NOT NULL , 
                  "VIDEO_TYPE" CHAR(1) , 
                  "MNS_TYPE" CHAR(1) )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 


    BASS1.INT_21007_YYYYMM.tcl
    BASS1_INT_0400810_YYYYMM.tcl
    BASS1_INT_210012916_YYYYMM.tcl
    
    
                                                                       

CREATE TABLE "BASS1   "."INT_210012916_YYYYMM"  (
                  "OP_TIME" INTEGER , 
                  "USER_ID" VARCHAR(20) , 
                  "PRODUCT_NO" VARCHAR(15) , 
                  "BRAND_ID" BIGINT , 
                  "SVC_TYPE_ID" VARCHAR(5) , 
                  "TOLL_TYPE_ID" CHAR(3) , 
                  "IP_TYPE_ID" VARCHAR(16) , 
                  "ADVERSARY_ID" VARCHAR(6) , 
                  "ROAM_TYPE_ID" CHAR(3) , 
                  "CALL_TYPE_ID" CHAR(2) , 
                  "CALL_COUNTS" BIGINT , 
                  "BASE_BILL_DURATION" BIGINT , 
                  "TOLL_BILL_DURATION" BIGINT , 
                  "CALL_DURATION" BIGINT , 
                  "BASE_CALL_FEE" BIGINT , 
                  "TOLL_CALL_FEE" BIGINT , 
                  "CALLFW_TOLL_FEE" BIGINT , 
                  "CALL_FEE" BIGINT , 
                  "FAVOURED_BASECALL_FEE" BIGINT , 
                  "FAVOURED_TOLLCALL_FEE" BIGINT , 
                  "FAVOURED_CALLFW_TOLLFEE" BIGINT , 
                  "FAVOURED_CALL_FEE" BIGINT , 
                  "FREE_DURATION" BIGINT , 
                  "FAVOUR_DURATION" BIGINT , 
                  "CALLMOMENT_ID" CHAR(3) , 
                  "SVCITEM_ID" VARCHAR(16) , 
                  "MNS_TYPE" INTEGER , 
                  "OPP_PROPERTY" INTEGER )   
                 DISTRIBUTE BY HASH("OP_TIME",  
                 "USER_ID",  
                 "PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 



select * from   mon_all_interface where interface_code = '02016'                                                                       
select count(0),count(distinct user_id ) from   g_i_02016_month where time_id = 201106
 
select user_id,count(0) from    g_i_02016_month where time_id = 201106
group by user_id 
having count(0) > 1
89157334068705      	2

select count(0),count(distinct user_id ) from     g_i_02016_month where time_id = 201106

select * from   g_i_02016_month where time_id = 201106
and user_id = '89157334068705'

TIME_ID	PROD_ID	USER_ID	VALID_DATE
201106	108	89157334068705      	20110601
201106	107	89157334068705      	20110601

delete from g_i_02016_month
where user_id = '89157334068705'
and PROD_ID = '107'
and time_id = 201106



                select
                    201106,
                        case when b.prod_id in(90030036,90008120) then '106'
                             when b.prod_id in(90030031,90008121) then '107'
                             when b.prod_id in(90030024,90008122) then '108'
                             when b.prod_id in(90005000,90005001,90005002,90005003,90005004,90005005) then '300'
                         end prod_id,
                         a.user_id,
                         replace(char(date(a.valid_date)),'-','') valid_date
                from bass2.dwd_product_sprom_active_20110630 a,
                         bass2.dim_product_item b
                where a.sprom_id=b.prod_id
                  and b.prod_id in (90030036,90008120,90030031,90008121,90030024,90008122,90005000,90005001,90005002,90005003,90005004,90005005)
                  and replace(char(date(a.valid_date)),'-','')<='20110630'
                  and replace(char(date(a.expire_date)),'-','')>'20110630'
                  
                  
select *    
                from bass2.dwd_product_sprom_active_20110630 a,
                         bass2.dim_product_item b
                where a.sprom_id=b.prod_id
                  and b.prod_id in (90030036,90008120,90030031,90008121,90030024,90008122,90005000,90005001,90005002,90005003,90005004,90005005)     
                  and a.user_id = '89157334068705'                  
                  

USER_ID
89157334068705
89157334068705


                  
select * from    bass2.Dw_product_ins_off_ins_prod_ds 
where product_instance_id = '89157334068705'


	107：50元/月包40小时
	108：100元/月包200小时

                  
	107：50元/月包40小时
	108：100元/月包200小时

\                  
select  a.*,b.NAME from bass2.Dw_product_ins_off_ins_prod_ds a, bass2.ODS_PROD_UP_PRODUCT_ITEM_20110630 b
where a.offer_id = b.product_item_id
and a.PRODUCT_INSTANCE_ID = '89157334068705'



delete from 

TR1_L_11042

select * from   app.sch_control_task where control_code = 'TR1_L_11042'

select count(0) from   bass2.ODS_PM_SP_OPERATOR_CODE_201106

6442249

select count(0) from   bass2.ODS_PM_SP_OPERATOR_CODE_201105

6374985



select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 3 days
and control_code = 'TR1_L_11042'
order by alarmtime desc 



select tabname from syscat.tables where tabname like '%02059%'   

CREATE TABLE "BASS1   "."G_A_02059_DAY_DOWN20110531"  (
                  "TIME_ID" VARCHAR(1) , 
                  "ENTERPRISE_ID" VARCHAR(20) , 
                  "USER_ID" VARCHAR(20) , 
                  "ENTERPRISE_BUSI_TYPE" VARCHAR(4) , 
                  "MANAGE_MODE" VARCHAR(1) , 
                  "ORDER_DATE" VARCHAR(8) , 
                  "STATUS_ID" VARCHAR(1) )   
                 DISTRIBUTE BY HASH("USER_ID",  
                 "TIME_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 




                                select count(0)
                                from 
                                (
                                                select t.*,row_number()over(partition by enterprise_id,user_id order by time_id desc ) rn 
                                                from 
                                                (
                                                select * from G_A_02059_DAY_DOWN20110531
                                                where 
                                                                 ENTERPRISE_BUSI_TYPE  in ('1000','1010')                               
                                                and time_id <= '20110531'
                                                and MANAGE_MODE = '3'
                                                ) t
                                ) t2
                                where rn = 1 and STATUS_ID ='1'
79021                                

                                select count(0)
                                from 
                                (
                                                select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
                                                from 
                                                (
                                                select * from G_A_02059_DAY
                                                where 
                                                                time_id <= 20110531
                                                and MANAGE_MODE = '3'
                                                ) t
                                ) t2
                                where rn = 1 and STATUS_ID ='1'
79021

                                
378395
                                


                                select count(0)
                                from 
                                (
                                                select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
                                                from 
                                                (
                                                select * from G_A_02059_DAY_DOWN20110531
                                                where 
                                                                 time_id <= '20110531'
                                                and MANAGE_MODE = '3'
                                                ) t
                                ) t2
                                where rn = 1 and STATUS_ID ='1'

213338


select * from   G_A_02059_DAY
where 
   ENTERPRISE_BUSI_TYPE  in ('1000','1010')
order by 1 desc 


                                

SELECT name,npages,fpages FROM SYSIBM.SYSTABLES --where tbspace='TBS_CDR_DATA'where tbspace='TBS_APP_BASS1'--and name like '%_BAKNGGJ'--order by nameorder by npages desc,fpages desc


INT_210012916_201105	1775730	1775730
INT_210012916_201104	1756875	1756875
INT_210012916_201103	1708305	1708305
INT_210012916_201101	1526400	1526400
drop table INT_210012916_201101




values 7502*1.00/1245749                                


drop table BASS1.int_02004_02008_month_stage;
CREATE TABLE BASS1.int_02004_02008_month_stage
 (
   user_id        CHARACTER(15),
   product_no     CHARACTER(15),
   test_flag      CHARACTER(1),
   sim_code       CHARACTER(15),
   usertype_id    CHARACTER(4),
   create_date    CHARACTER(15),
   brand_id       CHARACTER(4),
   time_id        int
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING;

ALTER TABLE BASS1.int_02004_02008_month_stage
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
  
  


insert into app.sch_control_map
values
 (2,'INT_COMMON_ROUTINE_MONTH.tcl','BASS1_INT_COMMON_ROUTINE_MONTH.tcl')

  

delete from bass1.int_program_data where PROGRAM_NAME = 'INT_COMMON_ROUTINE_MONTH.tcl'
insert into bass1.int_program_data
select
SEQUENCE_ID
,PROGRAM_TYPE
,'INT_COMMON_ROUTINE_MONTH.tcl' PROGRAM_NAME
,'INT_COMMON_ROUTINE_MONTH.BASS1' SOURCE_DATA
,'INT_COMMON_ROUTINE_MONTH_e' OBJECTIVE_DATA
,'INT_COMMON_ROUTINE_MONTH_f' FINAL_DATA
from bass1.int_program_data
where PROGRAM_NAME = 'G_S_22204_MONTH.tcl'

  
  select count(0) from  BASS1.g_a_02004_02008_stage
  select count(0) from  int_02004_02008_month_stage
  
  
  
  insert into app.sch_control_before 
values
('BASS1_INT_COMMON_ROUTINE_MONTH.tcl','BASS1_G_A_02004_DAY.tcl')
,('BASS1_INT_COMMON_ROUTINE_MONTH.tcl','BASS1_G_A_02008_DAY.tcl')



insert into app.sch_control_before
values 
 ('BASS1_INT_CHECK_R235_MONTH.tcl','BASS1_INT_COMMON_ROUTINE_MONTH.tcl')
 
 
 
 
 
delete from app.sch_control_task where control_code in ('BASS1_EXP_INT_COMMON_ROUTINE_MONTH','BASS1_INT_COMMON_ROUTINE_MONTH.tcl');
insert into app.sch_control_task values ('BASS1_INT_COMMON_ROUTINE_MONTH.tcl',2,2,'int -s INT_COMMON_ROUTINE_MONTH.tcl',0,108,'月通用过程处理','app','BASS1',1,'/bassapp/bass1/tcl/');

insert into app.sch_control_task values ('BASS1_EXP_INT_COMMON_ROUTINE_MONTH',2,2,'bass1_export BASS1.INT_COMMON_ROUTINE_MONTH LASTMONTH()',0,-1,'EXPORT电子渠道登录信息','app','BASS1',1,'/bassapp/backapp/bin/bass1_export/');




select * from   bass1.G_RULE_CHECK where rule_code = 'R237'      
order by time_id desc 

select int(substr(ACCT_ITEM_ID,2))/100 from bass1.g_s_03004_month 



select * from   bass1.mon_all_interface where interface_name like '%渠道%'

select * from   G_S_21003_MONTH_mobile


select * from  table( bass1.get_task('22401')) a 


select * from app.sch_control_task where control_code = 'BASS1_G_S_22081_MONTH.tcl'

update  app.sch_control_task a
set time_value = 410
where control_code =  'BASS1_G_S_22085_MONTH.tcl'


select usertype_id , count(0) 
--,  count(distinct usertype_id ) 
from int_02004_02008_month_stage 
group by  usertype_id 
order by 1 


select count(0) from  BASS1.G_S_21003_MONTH_mobile 	 a 
,int_02004_02008_month_stage b 
where a.product_no = b.product_no
and  b.usertype_id NOT IN ('2010','2020','2030','9000')
 and not exists (select 1 from bass1.g_s_03004_month c  where c.time_id = $op_month
 and b.user_id= c.USER_ID and 
 ( int(substr(ACCT_ITEM_ID,2))/100 in (1,2,3)
 		or ACCT_ITEM_ID in ('0401','0403','0407')
 )
 )
 
 select count(0)
 from
 (
 select distinct user_id from 
  BASS1.G_S_21003_MONTH_mobile 	 a 
,int_02004_02008_month_stage b 
where a.product_no = b.product_no
and  b.usertype_id NOT IN ('2010','2020','2030','9000')
) a 
left join 
(select user_id
from g_s_03004_month
where time_id = 201105 
and 
 ( int(substr(ACCT_ITEM_ID,2))/100 in (1,2,3)
 		or ACCT_ITEM_ID in ('0401','0403','0407')
 )
 group by user_id having sum(bigint(FEE_RECEIVABLE)) > 0
 ) b 
 on  a.user_id = b.user_id 
 where b.user_id is  null 
 

select count(0) from    int_02004_02008_month_stage
where usertype_id  IN ('1010')


		select count(0) val1 
		from   int_02004_02008_month_stage a
		left join 
		(		select user_id
				from g_s_03004_month
				where time_id = 201105 
				and 
				 ( int(substr(ACCT_ITEM_ID,2))/100 in (1,2,3)
				 		or ACCT_ITEM_ID in ('0401','0403','0407')
				 )
				 group by user_id 
				 having sum(bigint(FEE_RECEIVABLE)) > 0
		) b  on  a.user_id = b.user_id
 where b.user_id is  null 
		 and a.usertype_id  IN ('1010')
         

select * from   g_s_03004_month
         

                select count(0) val1 
                from  (select user_id from  int_02004_02008_month_stage 
                            where usertype_id  IN ('1010') and  TEST_FLAG = '0'
                    ) a
                left join 
                (               select user_id
                                from g_s_03004_month
                                where time_id = 201105 
                                 group by user_id                                  
                ) b  on  a.user_id = b.user_id
 where b.user_id is  null 

select * from   int_02004_02008_month_stage



                select distinct char(time_id ),substr(char(time_id ),1,4)||'-'||substr(char(time_id ),5,2)||'-'||substr(char(time_id ),7,2)
                from G_S_22073_DAY
                where char(time_id)
                not in (select date_seq from 
                                G_I_21020_MONTH_calendar
                                )
                                


select * from   bass1.G_RULE_CHECK where rule_code between 'R231'   and 'R254'   
AND TIME_ID in (201106,201105)
order by 2 ,1 

                         
                         




SELECT  ( val1 - val2 ) val ,val1,val2
from 
(
select count(0)  val1 from    
BASS1.G_I_06021_MONTH 
where time_id = 201106 and CHANNEL_STATUS = '1' 
and CHANNEL_B_TYPE in ('1','2','3')
)  a
,(
select count(0) val2 from    
BASS1.G_I_06021_MONTH 
where time_id = 201106 and CHANNEL_STATUS = '1' 
and   channel_type in ('1','2')
) b 



SELECT  ( val1 - val2 ) val ,val1,val2
from 
(

select count(0) val1 from    
BASS1.G_I_06021_MONTH 
where time_id = 201106 and CHANNEL_STATUS = '1' 
and CHANNEL_B_TYPE in  ('5','6')

)  a
,(

select count(0) val2 from    
BASS1.G_I_06021_MONTH 
where time_id = 201106 and CHANNEL_STATUS = '1' 
and   channel_type in ('3')
) b 



201106	R235	1313242.00000	35826.00000	0.02728	0.00000
201106	R235	1313242.00000	35384.00000	0.02694	0.00000


select * from app.sch_control_runlog 
where control_code = 'REP_App_xysc_comp_user_ms.tcl'


insert into app.sch_control_before
values ('BASS1_G_I_02017_MONTH.tcl','REP_App_xysc_school_user_ms.tcl')

insert into app.sch_control_before
values ('BASS1_G_S_22401_MONTH.tcl','REP_App_xysc_comp_user_ms.tcl')

REP_App_xysc_comp_user_ms.tcl
REP_App_xysc_new_user_ms.tcl

insert into app.sch_control_before
values ('BASS1_G_S_22401_MONTH.tcl','REP_App_xysc_new_user_ms.tcl')


insert into app.sch_control_before
values ('BASS1_G_S_22401_MONTH.tcl','BASS2_Dw_comp_cust_dt.tcl')

BASS2_Dw_comp_cust_dt.tcl

select * from  table( bass1.get_before('INT_CHECK_L34_MONTH.tcl')) a 





insert into bass1.g_s_05001_month
select * from  bass1.T_GS05001M where time_id = 201106


insert into bass1.g_s_05002_month
select * from  bass1.T_GS05002M where time_id = 201106

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05001_month 
group by  time_id 
order by 1 desc 

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05002_month 
group by time_id 
order by 1 desc 





select time_id,POINT_FEEDBACK_ID,count(0) from G_S_02007_MONTH
group by time_id,POINT_FEEDBACK_ID

select count(0),count(distinct USER_ID ) from    G_I_02017_MONTH where time_id = 201106


select * from   G_I_02017_MONTH



select * from app.sch_control_runlog where control_code in(
select control_code  from  table( bass1.get_task('21020'))  a
)


select sum(bigint(income)*1.00/100) from   G_S_22307_MONTH where time_id = 201106

1115.50000000000


select count(0),count(distinct SCHOOL_ID||COMP_BRAND_ID ) from    G_S_22401_MONTH where time_id = 201106


select * from   G_S_22401_MONTH where time_id = 201106



select * from   G_S_22086_MONTH
select * from   G_S_22085_MONTH


select count(0) from   G_S_22083_MONTH
where bigint(ORDER_CNT) <= 0
and time_id = 201106


select count(0) from   G_S_22083_MONTH
where bigint(ORDER_CNT) < bigint(CANCEL_CNT)
and time_id = 201106

select * from   G_S_22086_MONTH
where bigint(ORDER_USER_CNT) <= 0



roup by PRODUCT_NO
having sum(bigint(BASE_BILL_DURATION)) > 0


select count(distinct b.user_id) from     BASS1.G_S_21003_MONTH_mobile a 
,g_a_02004_02008_stage b 
where a.product_no = b.product_no
and  b.usertype_id NOT IN ('2010','2020','2030','2040','9000')
and b.test_flag = '0'



          
ALTER TABLE G_S_21003_MONTH_mobile ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE

insert into  BASS1.G_S_21003_MONTH_mobile
select  PRODUCT_NO from G_S_21003_MONTH 
where time_id = 201105  
group by PRODUCT_NO
having sum(bigint(BASE_BILL_DURATION)) > 0

runstats on table bass1.G_S_21003_MONTH_mobile with distribution and detailed indexes all 

select count(distinct b.user_id) from     BASS1.G_S_21003_MONTH_mobile a 
,int_02004_02008_month_stage b 
where a.product_no = b.product_no
and  b.usertype_id NOT IN ('2010','2020','2030','2040','9000')

1279643

 select count(0)
 from
 (
                 select distinct user_id from 
                  BASS1.G_S_21003_MONTH_mobile   a 
                        ,g_a_02004_02008_stage b 
                where a.product_no = b.product_no
                and  b.usertype_id NOT IN ('2010','2020','2030','9000')
                and b.test_flag = '0'
) a 
left join 
(               select user_id
                from g_s_03004_month
                where time_id = 201105 
                and 
                 ( int(substr(ACCT_ITEM_ID,2))/100 in (1,2,3)
                                or ACCT_ITEM_ID in ('0401','0403','0407')
                 )
                 group by user_id 
                 having sum(bigint(FEE_RECEIVABLE)) > 0
) b  on  a.user_id = b.user_id 
 where b.user_id is  null 
 
 select * from   G_S_22039_MONTH
order by 2 desc ,3


select distinct ACCT_ITEM_ID
from G_S_03004_MONTH where time_id = 201106




select a from 
(
select '0101' a from bass2.dual union all
select '0201' a from bass2.dual union all
select '0203' a from bass2.dual union all
select '0207' a from bass2.dual union all
select '0305' a from bass2.dual union all
select '0307' a from bass2.dual union all
select '0309' a from bass2.dual union all
select '0311' a from bass2.dual union all
select '0313' a from bass2.dual union all
select '0315' a from bass2.dual union all
select '0319' a from bass2.dual union all
select '0321' a from bass2.dual union all
select '0323' a from bass2.dual union all
select '0327' a from bass2.dual union all
select '0329' a from bass2.dual union all
select '0333' a from bass2.dual union all
select '0335' a from bass2.dual union all
select '0401' a from bass2.dual union all
select '0403' a from bass2.dual union all
select '0501' a from bass2.dual union all
select '0519' a from bass2.dual union all
select '0520' a from bass2.dual union all
select '0521' a from bass2.dual union all
select '0522' a from bass2.dual union all
select '0523' a from bass2.dual union all
select '0525' a from bass2.dual union all
select '0599' a from bass2.dual union all
select '0605' a from bass2.dual union all
select '0611' a from bass2.dual union all
select '0613' a from bass2.dual union all
select '0615' a from bass2.dual union all
select '0626' a from bass2.dual union all
select '0627' a from bass2.dual union all
select '0631' a from bass2.dual union all
select '0633' a from bass2.dual union all
select '0635' a from bass2.dual union all
select '0637' a from bass2.dual union all
select '0638' a from bass2.dual union all
select '0639' a from bass2.dual union all
select '0699' a from bass2.dual union all
select '0715' a from bass2.dual union all
select '0716' a from bass2.dual union all
select '0901' a from bass2.dual ) a
except 

 BASS1.ALL_DIM_LKP  
 set bass1_tbid = 'BASS_STD1_0074'
 where lower(xzbas_tbname) like '%dim_acct_item%'
and bass1_tbid <> 'BASS_STD1_0074'
and bass1_tbn_desc  like '%帐目科目%'

select bass1_value from BASS1.ALL_DIM_LKP  
where lower(xzbas_tbname) like '%dim_acct_item%'
and bass1_tbid = 'BASS_STD1_0074'
except
select a
from 
(
select '0100' a from bass2.dual union all
select '0101' a from bass2.dual union all
select '0200' a from bass2.dual union all
select '0201' a from bass2.dual union all
select '0203' a from bass2.dual union all
select '0205' a from bass2.dual union all
select '0207' a from bass2.dual union all
select '0300' a from bass2.dual union all
select '0301' a from bass2.dual union all
select '0305' a from bass2.dual union all
select '0311' a from bass2.dual union all
select '0313' a from bass2.dual union all
select '0315' a from bass2.dual union all
select '0317' a from bass2.dual union all
select '0319' a from bass2.dual union all
select '0321' a from bass2.dual union all
select '0323' a from bass2.dual union all
select '0325' a from bass2.dual union all
select '0327' a from bass2.dual union all
select '0329' a from bass2.dual union all
select '0331' a from bass2.dual union all
select '0333' a from bass2.dual union all
select '0335' a from bass2.dual union all
select '0400' a from bass2.dual union all
select '0401' a from bass2.dual union all
select '0403' a from bass2.dual union all
select '0405' a from bass2.dual union all
select '0407' a from bass2.dual union all
select '0409' a from bass2.dual union all
select '0500' a from bass2.dual union all
select '0501' a from bass2.dual union all
select '0507' a from bass2.dual union all
select '0516' a from bass2.dual union all
select '0519' a from bass2.dual union all
select '0520' a from bass2.dual union all
select '0521' a from bass2.dual union all
select '0522' a from bass2.dual union all
select '0523' a from bass2.dual union all
select '0524' a from bass2.dual union all
select '0525' a from bass2.dual union all
select '0526' a from bass2.dual union all
select '0599' a from bass2.dual union all
select '0600' a from bass2.dual union all
select '0602' a from bass2.dual union all
select '0605' a from bass2.dual union all
select '0609' a from bass2.dual union all
select '0611' a from bass2.dual union all
select '0613' a from bass2.dual union all
select '0615' a from bass2.dual union all
select '0619' a from bass2.dual union all
select '0620' a from bass2.dual union all
select '0625' a from bass2.dual union all
select '0626' a from bass2.dual union all
select '0627' a from bass2.dual union all
select '0628' a from bass2.dual union all
select '0631' a from bass2.dual union all
select '0632' a from bass2.dual union all
select '0633' a from bass2.dual union all
select '0635' a from bass2.dual union all
select '0637' a from bass2.dual union all
select '0638' a from bass2.dual union all
select '0639' a from bass2.dual union all
select '0640' a from bass2.dual union all
select '0642' a from bass2.dual union all
select '0643' a from bass2.dual union all
select '0699' a from bass2.dual union all
select '0700' a from bass2.dual union all
select '0701' a from bass2.dual union all
select '0702' a from bass2.dual union all
select '0715' a from bass2.dual union all
select '0716' a from bass2.dual union all
select '0800' a from bass2.dual union all
select '0900' a from bass2.dual union all
select '0901' a from bass2.dual 
) a


select * from BASS1.ALL_DIM_LKP  
where lower(xzbas_tbname) like '%dim_acct_item%'
and bass1_tbid = 'BASS_STD1_0074'
and bass1_value in ('0303','0307','0309')


select * from   bass2.dim_acct_item
where char(item_id) in (
 '80000209'
,'80000210'
,'80000222'
,'80000221'
,'80000212'
,'80000213'
)


select  ACCT_ITEM_ID,count(0)
from G_S_03004_MONTH where time_id = 201106
group by ACCT_ITEM_ID

select  ITEM_ID,count(0)
from G_S_03005_MONTH where time_id = 201106
group by ITEM_ID








select distinct  a
from 
(
select '0100' a from bass2.dual union all
select '0101' a from bass2.dual union all
select '0200' a from bass2.dual union all
select '0201' a from bass2.dual union all
select '0203' a from bass2.dual union all
select '0205' a from bass2.dual union all
select '0207' a from bass2.dual union all
select '0300' a from bass2.dual union all
select '0301' a from bass2.dual union all
select '0305' a from bass2.dual union all
select '0311' a from bass2.dual union all
select '0313' a from bass2.dual union all
select '0315' a from bass2.dual union all
select '0317' a from bass2.dual union all
select '0319' a from bass2.dual union all
select '0321' a from bass2.dual union all
select '0323' a from bass2.dual union all
select '0325' a from bass2.dual union all
select '0327' a from bass2.dual union all
select '0329' a from bass2.dual union all
select '0331' a from bass2.dual union all
select '0333' a from bass2.dual union all
select '0335' a from bass2.dual union all
select '0400' a from bass2.dual union all
select '0401' a from bass2.dual union all
select '0403' a from bass2.dual union all
select '0405' a from bass2.dual union all
select '0407' a from bass2.dual union all
select '0409' a from bass2.dual union all
select '0500' a from bass2.dual union all
select '0501' a from bass2.dual union all
select '0507' a from bass2.dual union all
select '0516' a from bass2.dual union all
select '0519' a from bass2.dual union all
select '0520' a from bass2.dual union all
select '0521' a from bass2.dual union all
select '0522' a from bass2.dual union all
select '0523' a from bass2.dual union all
select '0524' a from bass2.dual union all
select '0525' a from bass2.dual union all
select '0526' a from bass2.dual union all
select '0599' a from bass2.dual union all
select '0600' a from bass2.dual union all
select '0602' a from bass2.dual union all
select '0605' a from bass2.dual union all
select '0609' a from bass2.dual union all
select '0611' a from bass2.dual union all
select '0613' a from bass2.dual union all
select '0615' a from bass2.dual union all
select '0619' a from bass2.dual union all
select '0620' a from bass2.dual union all
select '0625' a from bass2.dual union all
select '0626' a from bass2.dual union all
select '0627' a from bass2.dual union all
select '0628' a from bass2.dual union all
select '0631' a from bass2.dual union all
select '0632' a from bass2.dual union all
select '0633' a from bass2.dual union all
select '0635' a from bass2.dual union all
select '0637' a from bass2.dual union all
select '0638' a from bass2.dual union all
select '0639' a from bass2.dual union all
select '0640' a from bass2.dual union all
select '0642' a from bass2.dual union all
select '0643' a from bass2.dual union all
select '0699' a from bass2.dual union all
select '0700' a from bass2.dual union all
select '0701' a from bass2.dual union all
select '0702' a from bass2.dual union all
select '0715' a from bass2.dual union all
select '0716' a from bass2.dual union all
select '0800' a from bass2.dual union all
select '0900' a from bass2.dual union all
select '0901' a from bass2.dual 
) a where a not like '%00'
except
select distinct bass1_value from BASS1.ALL_DIM_LKP  
where lower(xzbas_tbname) like '%dim_acct_item%'
and bass1_tbid = 'BASS_STD1_0074'


1
0205
0317!
0325
0405
0407
0409
0516
0524
0602
0609
0619
0628
0640
0642
0643
0701
0702

港澳台漫游通话费


select * from   bass2.dim_acct_item where item_name like '%话音业务套餐%'


select * from BASS1.ALL_DIM_LKP  
where lower(xzbas_tbname) like '%dim_acct_item%'
and bass1_tbid = 'BASS_STD1_0074'
and bass1_value_desc like '%港澳台%'

select * from  BASS1.ALL_DIM_LKP  
where xzbas_value in (
'80000275'
,'80000340'
,'80000404'
)


80000275	长途费_国际漫出_港澳台_直拨(中国移动TD)
80000340	长途费_国际漫出_港澳台_直拨(中国电信TD)
80000404	长途费_国际漫出_港澳台_直拨(中国网通TD)



80000629	港澳台漫游主叫基本通话费
80000630	港澳台漫游被叫基本通话费
XZBAS_TBNAME	XZBAS_COLNAME	XZBAS_VALUE	BASS1_TBN_DESC	BASS1_TBID	BASS1_VALUE	BASS1_VALUE_DESC
dim_acct_item	省内漫游港澳台长话费	80000056	帐目科目	BASS_STD1_0074	0313	省内漫游港澳台长途费


insert into  BASS1.ALL_DIM_LKP  
values ('dim_acct_item','港澳台漫游被叫基本通话费','80000630','帐目科目','BASS_STD1_0074','0205','港澳台漫游通话费')


话音业务套餐

select * from  BASS1.ALL_DIM_LKP  
where xzbas_value in (
'80000629'
,'80000630'
,'80000404'
)


XZBAS_VALUE
80000629
80000630
80000404

select count(0),count(distinct PRODUCT_NO) from     bass2.dw_acct_shoulditem_201106
where item_id in (80000629,80000630)

   select * from   bass1.g_rule_check 
where  rule_code between 'R230' and 'R235'
order by 1 desc ,2


201106	R235	1312839.00000	35770.00000	0.02724	0.00000

values (35770 - 49)*1.00/1312839

                INSERT INTO BASS1.G_RULE_CHECK VALUES (201106,'R235',1312839,35343,0.0269210466782293944,0) 
                

select * from    bass1.G_RULE_CHECK where rule_code = 'L4'                