---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc 
---------------------------------------------------------------------------------
select * from   table( bass1.chk_same() ) a order by 2---------------------------------------------------------------------------------select * from   table( bass1.chk_wave() ) a order by 2---------------------------------------------------------------------------------



---------------------------------------------------------------------------------
select * from   app.sch_control_runlog where flag = 1 
select * from app.sch_control_task where control_code in 
(select control_code from   app.sch_control_runlog where flag = 1 )
and cc_flag = 1

---------------------------------------------------------------------------------
select * from table(bass1.fn_dn_flret_cnt(13)) a


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
21007短信涉及的校验C1(调度BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl)超标，数据正常生成的，不要进行任何数据调整，调度直接点击完成后运行后续；

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

20110519	20110519	2306      	1675939     	22918403    	501612      	3834942     	68        	257121      

20110520	20110520	2330      	1678173     	23580986    	476230      	3903368     	80        	284164      
  
 
 20110528	20110528	1888      	1645201     	22985709    	515685      	3774471     	83        	274209      

20110601	20110601	4520      	1646351     	24447434    	520729      	4689422     	120       	492684      --当日离网 ，二经没有打标

20110602	20110602	3423      	1649676     	22326259    	471251      	3738425     	99        	261862      
 
  --调整脚本，''里更新一定的值就是
--离网客户数
update bass1.g_s_22012_day set m_off_users='100' 
where time_id=int(replace(char(current date - 1 days),'-',''))


 select * from  bass1.G_RULE_CHECK where rule_code = 'C1' and time_id > 20110101
 order by 1 desc 
 
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

---R107update (select * from  BASS1.G_S_04008_DAY where time_id = 20110531  ) t set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+400) with ur
update (select * from  BASS1.G_S_04008_DAY where time_id = 20110531  ) t set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+10) with ur

--97763 row(s) affected.
commit--100390 row(s) affected.
--97763 row(s) affected.---R108update (select * from  BASS1.G_S_04008_DAY where time_id = 20110531  ) t set BASE_BILL_DURATION = char(bigint(BASE_BILL_DURATION)-5) with ur 
commit

--100390 row(s) affected.
--97763 row(s) affected.
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


select * from   bass1.mon_all_interface where INTERFACE_CODE = '04007'


--检查：01007 ， 02058 ， 02059， 02054 ， 

select count(0) from    g_a_01007_day
where time_id = 20110531

select count(0) from    g_a_02058_day
where time_id = 20110531

select count(0) from    g_a_02059_day
where time_id = 20110531

select count(0) from    g_a_02054_day
where time_id = 20110531


select * from   g_a_02054_day_t
except
select * from   g_a_02054_day
where time_id = 20110531


select * from   g_a_02059_day_t
except
select * from   g_a_02059_day
where time_id = 20110531



select * from   g_a_02058_day_t
except
select * from   g_a_02058_day
where time_id = 20110531


select * from   g_a_01007_day_t
except
select * from   g_a_01007_day
where time_id = 20110531

select * from 
g_a_01007_day
where enterprise_id = '519155287'



        select count(0) from 
        (
                        select t.*
                        ,row_number()over(partition by t.user_id order by time_id desc ) rn 
                        from  G_A_02059_DAY  t
                        where time_id >= 20100626
          ) a
        where rn = 1    and     STATUS_ID = '1'
                and not exists (select 1 from (select distinct user_id from bass1.g_a_02004_day where time_id >= 20100626 ) t where a.user_id = t.user_id )
                with ur
                
                
                        select count(0) from 
                        (
                                        select t.*
                                        ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
                                        from 
                                        G_A_02059_DAY  t
                                                                                        where time_id >= 20100626
                          ) a
                        where rn = 1    and STATUS_ID = '1'
                        AND trim(A.enterprise_id) <> ''
                        and not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN BASS2.TRANS_ENTERPRISE_ID_20100625 B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.enterprise_id = t.enterprise_id )
                with ur

        select * from 
        (
                        select t.*
                        ,row_number()over(partition by t.user_id order by time_id desc ) rn 
                        from  G_A_02061_DAY  t 
          ) a
        where rn = 1    and     STATUS_ID = '1'
                and not exists (select 1 from (select distinct user_id from bass1.g_a_02004_day where time_id >= 20100626 ) t where a.user_id = t.user_id )
                with ur 



                        select * from 
                        (
                                        select t.*
                                        ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
                                        from 
                                        G_A_02061_DAY  t
                                                                                    
                          ) a
                        where rn = 1    and STATUS_ID = '1'
                        and not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN BASS2.TRANS_ENTERPRISE_ID_20100625 B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.enterprise_id = t.enterprise_id )
                with ur         
                
                
                
select count(0) from    G_A_02061_DAY
where time_id = 20110531


insert into G_A_02061_DAY
        select 
         20110531 TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,EXPIRE_DATE
        ,PAY_TYPE
        ,ORDER_DATE
        ,'2' STATUS_ID
        from 
        (
                        select t.*
                        ,row_number()over(partition by t.user_id order by time_id desc ) rn 
                        from  G_A_02061_DAY  t 
          ) a
        where rn = 1    and     STATUS_ID = '1'
                and not exists (select 1 from (select distinct user_id from bass1.g_a_02004_day where time_id >= 20100626 ) t where a.user_id = t.user_id )
                with ur 






insert into G_A_02061_DAY
        select 
         20110531 TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,EXPIRE_DATE
        ,PAY_TYPE
        ,ORDER_DATE
        ,'2' STATUS_ID
 from 
                        (
                                        select t.*
                                        ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
                                        from 
                                        G_A_02061_DAY  t
                                                                                    
                          ) a
                        where rn = 1    and STATUS_ID = '1'
                        and not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN BASS2.TRANS_ENTERPRISE_ID_20100625 B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.enterprise_id = t.enterprise_id )
                with ur         
                
                

                

        select count(0) from 
        (
                        select t.*
                        ,row_number()over(partition by t.user_id order by time_id desc ) rn 
                        from  G_A_02062_DAY  t
          ) a
        where rn = 1    and     STATUS_ID = '1'
                and not exists (select 1 from (select distinct user_id from bass1.g_a_02004_day where time_id >= 20100626 ) t where a.user_id = t.user_id )
                with ur 

                
                
                

                        select count(0) from 
                        (
                                        select t.*
                                        ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
                                        from 
                                        G_A_02062_DAY  t
                                                                                     
                          ) a
                        where rn = 1    and STATUS_ID = '1'
                        and not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN BASS2.TRANS_ENTERPRISE_ID_20100625 B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.enterprise_id = t.enterprise_id )
                with ur   

                
                
                
                      select count(0) from 
                        (
                                        select t.*
                                        ,row_number()over(partition by t.enterprise_id ,ENTERPRISE_BUSI_TYPE order by time_id desc ) rn 
                                        from 
                                        G_A_02054_DAY  t
                          ) a
                        where rn = 1    and STATUS_ID = '1'
                        and not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN BASS2.TRANS_ENTERPRISE_ID_20100625 B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.enterprise_id = t.enterprise_id )
                with ur


                

select * from   G_A_02058_DAY_T                      

select * from   G_A_02054_DAY_T        

select * from   G_A_02059_DAY_T         


 
 
 
select * from app.sch_control_task a 
where control_code like  '%G_S_04007_DAY%'
and cc_flag = 1

update app.sch_control_task a 
set cc_flag = 2
where control_code like  '%G_S_04007_DAY%'
and cc_flag = 1

update app.sch_control_task a 
set FUNCTION_DESC = '[作废]'||FUNCTION_DESC
where control_code like  '%%'
and cc_flag = 2



select * from   G_S_04007_DAY

select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_04007_DAY 
group by  time_id 
order by 1 desc 





select      20110601 TIME_ID
             ,replace(char(date(a.create_date)),'-','') op_time
             ,char(a.TYCX_QUERY)             qry_cnt
             ,char(a.TYCX_TUIDING)           cancel_cnt
             ,char(a.TYCX_TUIDING_FAIL)      cancel_fail_cnt
             ,char(a.TYCX_TOUSU_LIANG)       complaint_cnt
             ,char(b.CANCEL_BUSI_CNT) 	 CANCEL_BUSI_CNT
        from  bass2.DW_THREE_ITEM_STAT_DM_201105 a ,
              (select  replace(char(date(a.create_date)),'-','') op_time
              					,count(0) CANCEL_BUSI_CNT
                       from   
                       	BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_201106 a
                        where replace(char(date(a.create_date)),'-','') =  '20110601'  
                        group by replace(char(date(a.create_date)),'-','')
                    ) b 
        where replace(char(date(a.create_date)),'-','') = '20110601' 
				and    replace(char(date(a.create_date)),'-','') = b.op_time
                

select sum(income)*1.00/100
from (
select sum(bigint(income)) income from   g_s_03017_month
where time_id = 201104
and ent_busi_id = '1520'
and MANAGE_MOD = '3'
union all 
select sum(bigint(income)) income from   g_s_03018_month
where time_id = 201104
and ent_busi_id = '1520'
and MANAGE_MOD = '3'
) t

                
                

			select count(0) cnt
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where 
			 time_id <= 20110430 and ENTERPRISE_BUSI_TYPE = '1520'
			 and MANAGE_MODE = '2'
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
                
                

select sum(income)*1.00/100
from (
select sum(bigint(income)) income from   g_s_03017_month
where time_id = 201104
and ent_busi_id = '1520'
and MANAGE_MOD = '2'
union all 
select sum(bigint(income)) income from   g_s_03018_month
where time_id = 201104
and ent_busi_id = '1520'
and MANAGE_MOD = '3'
) t

                
select *
	from bass2.dw_channel_info_201105 a
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)
and a.CHANNEL_ID = 1002               


select * from   G_A_02054_DAY
where time_id = 20110601

select count(0) from    bass1.mon_all_interface 
where sts= 1 and type = 'd'



except 
select code from 
(
select '01002'  code from bass2.dual union all
select '01004'  code from bass2.dual union all
select '01006'  code from bass2.dual union all
select '01007'  code from bass2.dual union all
select '02004'  code from bass2.dual union all
select '02008'  code from bass2.dual union all
select '02011'  code from bass2.dual union all
select '02013'  code from bass2.dual union all
select '02022'  code from bass2.dual union all
select '02023'  code from bass2.dual union all
select '02053'  code from bass2.dual union all
select '02054'  code from bass2.dual union all
select '02055'  code from bass2.dual union all
select '02056'  code from bass2.dual union all
select '02057'  code from bass2.dual union all
select '02058'  code from bass2.dual union all
select '02059'  code from bass2.dual union all
select '02060'  code from bass2.dual union all
select '02061'  code from bass2.dual union all
select '02062'  code from bass2.dual union all
select '02063'  code from bass2.dual union all
select '02064'  code from bass2.dual union all
select '04002'  code from bass2.dual union all
select '04003'  code from bass2.dual union all
select '04004'  code from bass2.dual union all
select '04005'  code from bass2.dual union all
select '04006'  code from bass2.dual union all
select '04014'  code from bass2.dual union all
select '04015'  code from bass2.dual union all
select '04016'  code from bass2.dual union all
select '04017'  code from bass2.dual union all
select '04018'  code from bass2.dual union all
select '06031'  code from bass2.dual union all
select '06032'  code from bass2.dual union all
select '21001'  code from bass2.dual union all
select '21002'  code from bass2.dual union all
select '21004'  code from bass2.dual union all
select '21005'  code from bass2.dual union all
select '21007'  code from bass2.dual union all
select '21009'  code from bass2.dual union all
select '21016'  code from bass2.dual union all
select '22012'  code from bass2.dual union all
select '22038'  code from bass2.dual union all
select '22073'  code from bass2.dual union all
select '22066'  code from bass2.dual union all
select '22091'  code from bass2.dual union all
select '22092'  code from bass2.dual union all
select '22102'  code from bass2.dual union all
select '22104'  code from bass2.dual union all
select '22201'  code from bass2.dual union all
select '22202'  code from bass2.dual union all
select '22203'  code from bass2.dual union all
select '22301'  code from bass2.dual union all
select '22302'  code from bass2.dual union all
select '22080'  code from bass2.dual union all
select '22082'  code from bass2.dual union all
select '22084'  code from bass2.dual union all
select '04008'  code from bass2.dual union all
select '04009'  code from bass2.dual union all
select '04010'  code from bass2.dual union all
select '04011'  code from bass2.dual union all
select '04012'  code from bass2.dual 
) a
except
select interface_code  from bass1.mon_all_interface 
where sts= 1 and type = 'd'

1
22066
22091
22092



CREATE TABLE BASS1.G_S_22086_MONTH(
        TIME_ID                INTEGER             ---- 记录行号      
        ,OP_MONTH               CHAR(6)             ---- 月份 主键      
        ,BUSI_CODE              CHAR(2)            ---- 业务代码 主键  
        ,BUSI_NAME              CHAR(6)            ---- 业务名称 主键  
        ,SP_CODE                CHAR(12)            ---- SP企业代码    
        ,BACK_SP_NAME           CHAR(5)            ---- 退费SP企业名称 
        ,BILLING_TYPE           CHAR(2)             ---- 业务计费类型  
        ,BACK_USER_CNT          CHAR(1)            ---- 业务退费客户数 
        ,ORDER_USER_CNT         CHAR(1)            ---- 订购用户数    
 )DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (OP_MONTH,BUSI_CODE,BUSI_NAME
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22086_MONTH
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;




CREATE TABLE BASS1.G_S_22091_DAY(

        TIME_ID                INTEGER             ---- 记录行号      
        ,DEAL_DATE              CHAR(8)             ---- 办理日期 主键  
        ,CHANNEL_ID             CHAR(4)            ---- 实体渠道标识 主键
        ,DEAL_TYPE              CHAR(1)             ---- 办理类型 主键  
        ,NEW_USER_CNT           CHAR(8)             ---- 新增客户数    
        ,PAYMENT_REC_CNT        CHAR(1)            ---- 缴费笔数      
        ,PAYMENT_REC_FEE        CHAR(1)            ---- 缴费金额      
        ,CARD_SALE_CNT          CHAR(8)             ---- 充值卡销售笔数 
        ,VAL_BUSI_REC_CNT       CHAR(1)            ---- 增值业务办理笔数 
        ,VAL_BUSI_OPEN_CNT      CHAR(1)            ---- 增值业务开通量 
        ,IMP_VAL_OPEN_CNT                           ---- 重点增值业务开通量 
        ,TERM_SALE_CNT          CHAR(1)            ---- 定制终端销售量 
        ,MOBILE_SALE_CNT        CHAR(1)            ---- 其中定制手机销售量 
        ,BASE_REC_CNT           CHAR(1)            ---- 办理类基础服务笔数 
        ,QRY_REC_CNT            CHAR(1)            ---- 查询类基础服务笔数 
)DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (DEAL_DATE,CHANNEL_ID,DEAL_TYPE
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22091_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
  
  
  



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
                                              where time_id<=20110601 ) e
                                        inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
                                                                       from bass1.g_a_02008_day
                                                                       where time_id<=20110601 ) f on f.user_id=e.user_id
                                        where e.row_id=1 and f.row_id=1


  
  
  

select user_id from   session.int_check_user_status    
                        where usertype_id IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id=20110601                       
except
select user_id 
from bass2.dw_product_20110601  
where usertype_id in (1,2,9) 
 and day_off_mark = 1 
 and userstatus_id not in (1,2,3,6,8)
 and test_mark<>1


select * from   session.int_check_user_status  
where user_id in 
(
 '89160000952191'
,'89160000952179'
)
 
 
select usertype_id,day_off_mark,userstatus_id,test_mark from   bass2.dw_product_20110601
where user_id in 
(
 '89160000952191'
,'89160000952179'
)
 

 
 select count(user_id) 
                    from bass2.dw_product_20110601  
                   where usertype_id in (1,2,9) 
                     and day_off_mark = 1 
                     and userstatus_id not in (1,2,3,6,8)
                     and test_mark<>1


select count(0) from   G_S_22303_MONTH where ENT_BUSI_ID = '1370'
and time_id = 201105
and bigint(BILL_CUST_NUMS) >  bigint(USE_CUST_NUMS)
                     
                     


                     

			select a.cnt*1.00 ,value(b.income,0) income
				from
				(select count(0) cnt
							from 
							(
							select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
							from (
							select *
							from g_a_02054_day where 
							 time_id/100 <= 201105 and ENTERPRISE_BUSI_TYPE = '1520'
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
                    
                    
                    select * from table(bass1.fn_dn_flret_cnt(15)) a
select * from table(bass1.fn_dn_flret_cnt(15)) a



select a.td_cust_t_dur*1.0000 , b.dur*1.0000
				from 
				(
					select  bigint(td_cust_t_dur) td_cust_t_dur from   G_S_22204_MONTH
					where time_id = 201104					and CYCLE_ID = '3'
			 )a,
				(
					select sum(bill_duration)  dur
					from bass1.g_s_21003_month_td
				   where mns_type='1'
				) b
			with ur


(
		sel sum(case when td_book_flg=1 
        and td_infophone_flg=0 
        and td_mobile_flg=0 
        and td_crd_flg=0 
        then g_up_strm_book+t_up_strm_book+g_down_strm_book+t_down_strm_book 
									else g_up_strm_book+t_up_strm_book+g_down_strm_book+t_down_strm_book+g_up_strm_nonbook+t_up_strm_nonbook+g_down_strm_nonbook+t_down_strm_nonbook
						 end)/1024/1024 as val2
		from dwbview.dm_sum_td_user_new
		where statis_mon=$Tx_Date and cmcc_prov_prvd_id=$Branch_ID		
) t2


select * from   decimal(1.0*($RESULT_VAL1+$RESULT_VAL2-$RESULT_VAL3)/1024/1024,20,2)


select (val1 + val2 - val3)/1024/1024
from 
(
select 
		sum(bigint(a.flows))*1.00 val3 
		from bass1.g_s_04002_day_flows a,
		bass1.td_check_user_flow b
		where a.product_no=b.product_no
) a
,(

		select 
		sum(bigint(flows))*1.00 val2 
		from bass1.g_s_04018_day_flows
) b
,(
		select 
		sum(bigint(flows))*1.00 val1 
		from bass1.g_s_04002_day_flows
) c
                    
                    


select a.TD_CUST_FLOWS*1.0000 , b.flows*1.0000
				from 
				(
					select  bigint(TD_CUST_FLOWS) TD_CUST_FLOWS from   G_S_22204_MONTH
					where time_id = 201104
					and CYCLE_ID = '3'
			 )a,
				(
												select (val1 + val2 - val3)/1024/1024 flows
												from 
												(
												select 
														sum(bigint(a.flows))*1.00 val3 
														from bass1.g_s_04002_day_flows a,
														bass1.td_check_user_flow b
														where a.product_no=b.product_no
												) a
												,(
												
														select 
														sum(bigint(flows))*1.00 val2 
														from bass1.g_s_04018_day_flows
												) b
												,(
														select 
														sum(bigint(flows))*1.00 val1 
														from bass1.g_s_04002_day_flows
												) c
				) b
			with ur                    



            