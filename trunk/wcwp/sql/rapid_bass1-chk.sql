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

select * from   BASS1.G_S_22068_MONTH



---------------------------------------------------------------------------------
select * from app.sch_control_task where control_code in 
(select control_code from   app.sch_control_runlog where flag = 1 )
and cc_flag = 1

---------------------------------------------------------------------------------
select * from table(bass1.fn_dn_flret_cnt(9)) a

select * from table(bass1.fn_dn_flret_cnt(11)) a

select * from table(bass1.fn_dn_flret_cnt(13)) a

select * from table(bass1.fn_dn_flret_cnt(15)) a


/*********��**************************************************************************************/

--�ӿ��ļ������ؼ��(�������Ϊ56����¼) file 
--file lvl
select  * from APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'

select  * from APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code<>'00'

---------------------------------------------------------------------------------
--���ߣ��нӿ��ش�ʱ����¼�����·���ʱ��
--s_13100_20110301_22303_01_001.dat
--file lvl

select *
from 
(
select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn 
from APP.G_FILE_REPORT a
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'
) t where rn = 1

----------------------��δ�ϴ����ļ������أ��ӿڣ� (����û�����ļ����Ľӿ�)
select * from   BASS1.MON_ALL_INTERFACE where interface_code 
not in (select substr(filename,18,5) from 
APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'
) and type='d'
and interface_code <> '04007'

--record �ӿڼ�¼�����ؼ��(�������Ϊ56����¼) record
---------------------------------------------------------------------------------

select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1


--record ��ѯδ���ؽӿ� (����ȫ�ϱ�ʱ) record

select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=0
---------------------------------------------------------------------------------


--��δ�����ӿ� (ͨ����һ���뱾�ձȽ�)
select * from app.g_runlog 
where time_id=int(replace(char(current date - 2 days),'-',''))
and unit_code 
not in (select unit_code  from  app.g_runlog where time_id=int(replace(char(current date - 1 days),'-',''))
 )


-- record δ���ؽӿڵ���ϸ��Ϣ (day)  record
select * from  BASS1.MON_ALL_INTERFACE 
where INTERFACE_CODE in (
select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=0
)
and type = 'd'

-- record �ѷ��ؽӿڵ���ϸ��Ϣ (day)  record
select * from  BASS1.MON_ALL_INTERFACE 
where INTERFACE_CODE in (
select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1
)
and type = 'd'


--��ѯδ���еĵ�������
SELECT * FROM bass1.MON_ALL_INTERFACE  t
WHERE t.INTERFACE_CODE not IN (
select substr(a.control_code,15,5) from   app.sch_control_runlog  A
where control_code like 'BASS1_EXP%DAY'
AND date(a.begintime) =  date(current date)
AND FLAG = 0
)
AND TYPE='d'


--��ѯδ�����ӿ� (δ��ȫ����ʱ)

select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 2 days),'-',''))
and return_flag=1
except
select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=0


/********��***************************************************************************************/

---�½ӿ��ļ������ؼ��(���ڽӿ����ͣ���Ӧ�Ľ�������)
--�����ش��ӿڣ������ش�������������Ӧ��¼��������ȥ�ش�����ֹ�ظ�ͳ��
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


--�ļ�������ʧ�ܼ��
select  * 
from APP.G_FILE_REPORT
where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
and err_code <> '00'
and length(filename)=length('s_13100_201002_03014_01_001.dat')
order by deal_time desc


--��ѯ�ѷ����½ӿ� ��¼��

select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1

--���ϴ�δ���� (b��ΪNULL)
--mreturn

select c.INTERFACE_NAME,a.*,b.*
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
left join bass1.mon_all_interface c on substr(a.filename,16,5) = c.INTERFACE_CODE 


/**
--��ѯδ�����½ӿ� ��¼��

select * from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=0

--δ���ؽӿڵ���ϸ��Ϣ (month)

select * from  BASS1.MON_ALL_INTERFACE 
where INTERFACE_CODE in (
select unit_code from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=0
)
and type = 'm'
**/

--�鿴deadlineX �Ľӿ��ж����ѷ��ء�


--��ʱ�νӿ�-�鿴�������
select a.* ,b.return_flag from    BASS1.MON_ALL_INTERFACE  a
left join (select * from  app.g_runlog b
where  time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and b.return_flag = 1 
) b on  a.INTERFACE_CODE = b.UNIT_CODE
where  upload_time='ÿ��3��ǰ'



select a.* ,b.return_flag from    BASS1.MON_ALL_INTERFACE  a
left join (select * from  app.g_runlog b
where  time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and b.return_flag = 1 
) b on  a.INTERFACE_CODE = b.UNIT_CODE
where  upload_time='ÿ��5��ǰ'



select a.* ,b.return_flag from    BASS1.MON_ALL_INTERFACE  a
left join (select * from  app.g_runlog b
where  time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and b.return_flag = 1 
) b on  a.INTERFACE_CODE = b.UNIT_CODE
where  upload_time='ÿ��8��ǰ'



select a.* ,b.return_flag from    BASS1.MON_ALL_INTERFACE  a
left join (select * from  app.g_runlog b
where  time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and b.return_flag = 1 
) b on  a.INTERFACE_CODE = b.UNIT_CODE
where  upload_time='ÿ��10��ǰ'



select a.* ,b.return_flag from    BASS1.MON_ALL_INTERFACE  a
left join (select * from  app.g_runlog b
where  time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and b.return_flag = 1 
) b on  a.INTERFACE_CODE = b.UNIT_CODE
where  upload_time='ÿ��15��ǰ'

12+15+30+17+10 = 84 







--���ȳ����ʱ:
select A.*,char(a.RUNTIME/60)||'min',char(a.RUNTIME/60/60)||'hr' from   app.sch_control_runlog A
where control_code like 'BASS1%DAY%'
and a.RUNTIME/60 > 5
ORDER BY RUNTIME DESC 



/**
1��22073����KPI�漰��У��R163��R164��R165��R166��R167��R168��R169��R170��R171��R172(����ΪBASS1_INT_CHECK_COMP_KPI_DAY.tcl)��
21007�����漰��У��(����BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl)���꣬�����������ɵģ���Ҫ�����κ����ݵ���������ֱ�ӵ����ɺ����к�����

�磺
**/
select * from bass1.g_rule_check where rule_code in ('R171','R172','R169') order by time_id desc
20110501	R169	1503.00000	580.00000	1.59130	0.00000
20110501	R171	1499.00000	573.00000	1.61600	0.00000
20110501	R172	633.00000	383.00000	0.65270	0.00000

/**
2��22012��kpi�ӿ��漰��һ���Լ�飬�糬�꣬������е���(����ΪBASS1_INT_CHECK_INDEX_SAME_DAY.tcl)������22012�ӿڵ�ָ�꣬���ܱ�����ȣ�
һ�����ܵ�����(��Ƶ�����ǡ������û��������ָ�꣬��һ���û��ͳ��꣬��û����ھ���һ�����������δ�����û����ϱ�)��

�磺
**/
select 
         time_id,
         case when rule_code='R159_1' then '�����ͻ���'
              when rule_code='R159_2' then '�ͻ�������'
              when rule_code='R159_3' then '�������ͻ���'
              when rule_code='R159_4' then '�����ͻ���'
         end,
         target1,
         target2,
         target3
from bass1.g_rule_check
where 
    rule_code in ('R159_1','R159_2','R159_3','R159_4')
    and time_id=int(replace(char(current date - 1 days),'-',''))

20110418	�����ͻ���	84.00000	85.00000	-0.01176

20110428	�����ͻ���	144.00000	134.00000	0.07462

select * from     
bass1.g_s_22012_day 
where time_id=int(replace(char(current date - 1 days),'-',''))

20110517	20110517	2888      	1671364     	23709586    	469322      	3798066     	77        	280936      

20110519	20110519	2306      	1675939     	22918403    	5312      	3834942     	68        	257121      

20110520	20110520	2330      	1678173     	23580986    	476230      	3903368     	80        	284164      
  
 
 20110528	20110528	1888      	1645201     	22985709    	515685      	3774471     	83        	274209      

20110601	20110601	4520      	1646351     	24447434    	520729      	4689422     	120       	492684      --�������� ������û�д��

20110602	20110602	3423      	1649676     	22326259    	471251      	3738425     	99        	261862      
 
 TIME_ID	BILL_DATE	M_NEW_USERS	M_DAO_USERS	M_BILL_DURATION	M_DATA_FLOWS	M_BILL_SMS	M_OFF_USERS	M_BILL_MMS
20110603	20110603	2900      	1652486     	23148301    	481514      	4124087     	90        	264431      

  --�����ű���''�����һ����ֵ����
--�����ͻ���
update bass1.g_s_22012_day set m_off_users='97' 
where time_id=int(replace(char(current date - 1 days),'-',''))

 select * from  bass1.G_RULE_CHECK where rule_code = 'C1'
 order by 1 desc 

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

2���µ׵���BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl��R107/R108���꣬�˵���ֻ�����³�1���������µ������������ʱ�����������У��ͨ����
�������ڵ��ȱ���ֱ�ӵ��������ɣ�

--�����ű�(��������update��Щ����Ҫ�����ӣ������Ժ��绹�����ٵ����С�400���͡�5����ֵ��ע��΢��)
---R107
**/

select * from bass1.g_rule_check where rule_code in ('R107') order by time_id desc
20110531	R107	33.14000	69.10000	-0.52041	0.05000

20110430	R107	67.13000	68.50000	-0.02000	0.05000
select * from bass1.g_rule_check where rule_code in ('R108') order by time_id desc
20110430	R108	544.84000	560.01000	-0.02709	0.05000

---R107update (select * from  BASS1.G_S_04008_DAY where time_id = 20110731  ) t set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+400) with ur
update (select * from  BASS1.G_S_04008_DAY where time_id = 20110731  ) t set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+10) with ur

--97763 row(s) affected.
--100390 row(s) affected.
--97763 row(s) affected.
--201107 105138 row(s) affected.---R108update (select * from  BASS1.G_S_04008_DAY where time_id = 20110731  ) t set BASE_BILL_DURATION = char(bigint(BASE_BILL_DURATION)-5) with ur 

--100390 row(s) affected.
--97763 row(s) affected.
--201107 105138 row(s) affected.
select * from   bass1.G_RULE_CHECK where rule_code = 'R109'      
order by time_id desc 


--�����սӿڴ��룺

select b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'd'and b.control_code like '%DAY%'

--�����½ӿڴ��룺
select b.CONTROL_CODE from    BASS1.MON_ALL_INTERFACE a, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)and a.TYPE = 'm'and b.control_code like '%MONTH%'


		   
--ÿ���³���
--G_S_21003_STORE_DAY
--ÿ���³���������ȫ������
delete from G_S_21003_STORE_DAY 
where TIME_ID/100 = int(substr(replace(char(current date - 1 month),'-',''),1,6))

insert into G_S_21003_STORE_DAY
select *
from G_S_21003_TO_DAY
where TIME_ID/100 = int(substr(replace(char(current date - 1 month),'-',''),1,6))

--ͳ�Ʊ������()
select count(0) from G_S_21003_STORE_DAY 
where TIME_ID/100 = int(substr(replace(char(current date - 1 month),'-',''),1,6))

select count(0) from G_S_21003_TO_DAY 
where TIME_ID/100 = int(substr(replace(char(current date - 1 month),'-',''),1,6))

--ͳ�Ʊ������()
select count(0) from G_S_21003_STORE_DAY 
where TIME_ID/100 = int(substr(replace(char(current date - 2 month),'-',''),1,6))

select count(0) from G_S_21003_TO_DAY 
where TIME_ID/100 = int(substr(replace(char(current date - 2 month),'-',''),1,6))

--����һ�£�����G_S_21003_TO_DAYɾ��ǰ������ǰ������(��������5�£�����3��4�£�ɾ��2������)
--��߳����ٶ�
delete from G_S_21003_TO_DAY 
where TIME_ID/100 = int(substr(replace(char(current date - 2 month),'-',''),1,6))





--�����ص������ݵ�����

select b.*, lower( '/bassapp/backapp/bin/bass1_export/bass1_export '||substr(a.control_code,11,13)||' '||char(current date - 1 days) ) exp_cmdfrom   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE bwhere a.control_code like 'BASS1%EXP%DAY%'and date(a.begintime) =  date(current date)and substr(a.control_code,15,5) = b.interface_code and b.type='d'
--�����ص������ݵ�����

select b.*, lower( '/bassapp/backapp/bin/bass1_export/bass1_export '||substr(a.control_code,11,15)||' '||substr(char(current date - 1 month) ,1,7)) exp_cmdfrom   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE bwhere a.control_code like 'BASS1%EXP%MONTH%'and month(a.begintime) =  month(current date)and substr(a.control_code,15,5) = b.interface_code and b.type='m'


--����put ĳ���ӿ�:day



select b.*, lower( 'put *'||b.interface_code||'*.dat ' ) put_dat, lower( 'put *'||b.interface_code||'*.verf ' ) put_verf
from   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE bwhere a.control_code like 'BASS1%EXP%DAY%'and date(a.begintime) =  date(current date)and substr(a.control_code,15,5) = b.interface_code and b.type='d'
--����put ĳ���ӿ�:month

select b.*, lower( 'put *'||b.interface_code||'*.dat ' ) put_dat, lower( 'put *'||b.interface_code||'*.verf ' ) put_verffrom   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE bwhere a.control_code like 'BASS1%EXP%MONTH%'and month(a.begintime) =  month(current date)and substr(a.control_code,15,5) = b.interface_code and b.type='m'


--�ӿں� -  ���� ��Ӧ��ϵ
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
                from G_S_04019_DAY where time_id = 20110810
                except 
                select country_code from  bass1.dim_country_city_code 
                select * from   bass1.dim_country_city_code  where country_code like '2%'
                
select tabname from syscat.tables where tabname like '%DIM%COU%'                   
select * from   bass2.DIM_PUB_COUNTRY

select *                 from G_S_04019_DAY where time_id = 20110810
and B_AREA_CD = '2'

1209	δ֪����

update 
 G_S_04019_DAY 
 set B_AREA_CD = '1209'
 where time_id = 20110810
and B_AREA_CD = '2'
