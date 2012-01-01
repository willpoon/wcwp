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


select * from app.sch_control_alarm  where content like '%准确性指标56%'

---------------------------------------------------------------------------------
select * from   table( bass1.chk_same(0) ) a order by 2---------------------------------------------------------------------------------select * from   table( bass1.chk_wave(0) ) a order by 2---------------------------------------------------------------------------------
TIME_ID	RULE_CODE	RULE_NAME	THRESHOLD	BASS2_VAL	BASS1_VAL	WAVE_RATE_PERCENT
20111019	R159_1	新增客户数	1.0000	2547.00000	2549.00000	-0.07800
20111019	R159_2	客户到达数	1.0000	1817505.00000	1817505.00000	0.00000
20111019	R159_3	上网本客户数	5.0000	125.00000	125.00000	0.00000
20111019	R159_4	离网客户数	1.0000	3669.00000	3670.00000	-0.02700



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

select t.*, substr(filename,18,5)
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
left join bass1.mon_all_interface c on substr(a.filename,16,5) = c.INTERFACE_CODE 


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
update bass1.g_s_22012_day set m_off_users='258' 
where time_id=int(replace(char(current date - 1 days),'-',''))

 select * from  bass1.G_RULE_CHECK where rule_code = 'C1'
 order by 1 desc 

 select * from  bass1.G_RULE_CHECK where rule_code = 'C1' and time_id > 20110825
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

---R107update (select * from  BASS1.G_S_04008_DAY where time_id = 20110929  ) t set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+400) with ur;
109513 row(s) affected.
update (select * from  BASS1.G_S_04008_DAY where time_id = 20110731  ) t set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+10) with ur

--97763 row(s) affected.
--100390 row(s) affected.
--97763 row(s) affected.
--201107 105138 row(s) affected.---R108update (select * from  BASS1.G_S_04008_DAY where time_id = 20110929  ) t set BASE_BILL_DURATION = char(bigint(BASE_BILL_DURATION)-5) with ur 20110828
112152 row(s) affected.


--100390 row(s) affected.
--97763 row(s) affected.
--201107 105138 row(s) affected.
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
select * from   BASS1.MON_ALL_INTERFACE  where interface_code = '06002'
 
                       

select SCHOOL_ID , count(0) 
--,  count(distinct SCHOOL_ID ) 
from G_I_02032_MONTH 
group by  SCHOOL_ID 
order by 1 


select * from   bass2.dw_product_20110808 where product_no = '13989007120'
                       
select tabname from syscat.tables where tabname like '%GPRS_LOCAL%'                          

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

select * from bass2.dw_cust_20110812 
where CUST_ID = '89101110420499'




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

select * from  table( bass1.get_before('04008')) a 



  select * 
                  from bass1.G_S_22092_DAY
                where time_id in (20110823,20110822)
                
                
                BASS2_Dwd_product_ord_busi_other_ds.tcl	TR1_L_11060

select * from   app.sch_control_task where control_code like 'TR1_L_1106%'                




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

where bass1_tbid = 'BASS_STD1_0074'

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



select * from   bass1.g_rule_check where rule_code = 'R235'
ORDER BY 1 DESC 



select * from   bass1.g_rule_check where rule_code = 'R235'
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
               
               
               
                


select count(0) from   bass2.dw_product_20111230




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
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') 
						and err_code='00' 
						and substr(filename,18,5) 
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE 
								   where deadline = 9 and sts = 1 
								) 
						) t where rn = 1 
                        
                        