---------------------------------------------------------------------------------
select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc 
---------------------------------------------------------------------------------



---------------------------------------------------------------------------------


(
and cc_flag = 1

---------------------------------------------------------------------------------


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
select * from   BASS1.MON_ALL_INTERFACE 
not in (select substr(filename,18,5) from 
APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'
) and type='d'


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
--�����ش��ӿڣ������ش�������������Ӧ��¼��������ȥ�ش�������ֹ�ظ�ͳ��
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

--���ϴ�δ���� (b����ΪNULL)

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
21007�����漰��У��C1(����BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl)���꣬�����������ɵģ���Ҫ�����κ����ݵ���������ֱ�ӵ����ɺ����к�����

�磺
**/
select * from bass1.g_rule_check where rule_code in ('R171','R172','R169') order by time_id desc
20110501	R169	1503.00000	580.00000	1.59130	0.00000
20110501	R171	1499.00000	573.00000	1.61600	0.00000
20110501	R172	633.00000	383.00000	0.65270	0.00000

/**
2��22012��kpi�ӿ��漰��һ���Լ�飬�糬�꣬������е���(����ΪBASS1_INT_CHECK_INDEX_SAME_DAY.tcl)������22012�ӿڵ�ָ�꣬���ܱ������ȣ�
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

20110519	20110519	2306      	1675939     	22918403    	501612      	3834942     	68        	257121      

20110520	20110520	2330      	1678173     	23580986    	476230      	3903368     	80        	284164      
  
 
  
  --�����ű���''�����һ����ֵ����
--�����ͻ���
update bass1.g_s_22012_day set m_off_users='82' 
where time_id=int(replace(char(current date - 1 days),'-',''))


 select * from  bass1.G_RULE_CHECK where rule_code = 'C1'
 order by 1 desc 
 20110509	C1	2053251.00000	2472205.00000	-0.16947	0.00000

 20110508	C1	2472205.00000	2140554.00000	0.15494	0.00000

20110503	C1	1981494.00000	1819931.00000	0.08877	0.00000

20110502	C1	1819931.00000	2861998.00000	-0.36410	0.00000 
20110430	C1	3083030.00000	2551002.00000	0.20856	0.00000
20110429	C1	2551002.00000	2155029.00000	0.18374	0.00000
20110428	C1	2155029.00000	1990225.00000	0.08281	0.00000
20110427	C1	1990225.00000	1987809.00000	0.00122	0.00000


/**

2���µ׵���BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl��R107/R108���꣬�˵���ֻ�����³�1���������µ������������ʱ�����������У��ͨ����
�������ڵ��ȱ���ֱ�ӵ��������ɣ�

--�����ű�(��������update��Щ����Ҫ�����ӣ������Ժ��绹�������ٵ����С�400���͡�5����ֵ��ע��΢��)
---R107
**/

select * from bass1.g_rule_check where rule_code in ('R107') order by time_id desc
20110430	R107	67.13000	68.50000	-0.02000	0.05000
select * from bass1.g_rule_check where rule_code in ('R108') order by time_id desc
20110430	R108	544.84000	560.01000	-0.02709	0.05000

---R107
100390 row(s) affected.

--�����սӿڴ��룺

select b.CONTROL_CODE from    

--�����½ӿڴ��룺



		   
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

select b.*
--�����ص������ݵ�����

select b.*


--����put ĳ���ӿ�:day



select b.*
from   app.sch_control_runlog  a ,bass1.MON_ALL_INTERFACE b
--����put ĳ���ӿ�:month

select b.*


--�ӿں� -  ���� ��Ӧ��ϵ
select * from table(
select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,13) from    
) t where unit_code = ''

select * from table(
select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,15),b.control_code from    
) t where unit_code = ''


select * from   bass1.mon_all_interface where INTERFACE_CODE = '22063'



--���ܵ�������У��
select * from 
app.sch_control_runlog a
where control_code like 'BASS1_INT%'
AND date(a.begintime) =  date(current date)
AND FLAG = 0

update  app.sch_control_runlog a
set flag = -2 
where control_code like 'BASS1_INT_CHECK%'
AND date(a.begintime) =  date(current date)
AND FLAG = 0
                      
                                                                
              