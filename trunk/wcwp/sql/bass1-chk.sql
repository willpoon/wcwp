select 1 from bass2.dual

/**
select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20110322'||'000000') 
--and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc 
**/

select * from  app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc 

--��ѯδ�����ӿ� (δ��ȫ����ʱ)

select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 2 days),'-',''))
and return_flag=1
except
select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=0


/*********��***********/

--�ӿ��ļ������ؼ��(�������Ϊ56����¼) file 
--file lvl
select  * from APP.G_FILE_REPORT
where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00'

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

--record �ӿڼ�¼�����ؼ��(�������Ϊ56����¼) record
select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1


--record ��ѯδ���ؽӿ� (����ȫ����ʱ) record

select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=0


-- record δ���ؽӿڵ���ϸ��Ϣ (day)  record
select * from  BASS1.MON_ALL_INTERFACE 
where INTERFACE_CODE in (
select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=0
)
and type = 'd'

/********��***********/

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
select * from bass1.g_rule_check where rule_code in ('R171') order by time_id desc
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
where rule_code in ('R159_1','R159_2','R159_3','R159_4')
  and time_id=int(replace(char(current date - 1 days),'-',''))

select max(target3) from  bass1.g_rule_check where rule_code = 'R159_1'
select * from     
bass1.g_s_22012_day 
where time_id=int(replace(char(current date - 1 days),'-',''))
20110411	�����ͻ���	83.00000	85.00000	-0.02352
20110411	20110411	3133      	1634273     	23648119    	406854      	3882865     	83        	309424      

  
  --�����ű���''�����һ����ֵ����
--�����ͻ���
update bass1.g_s_22012_day set m_off_users='85' 
where time_id=int(replace(char(current date - 1 days),'-',''))


/**

2���µ׵���BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl��R107/R108���꣬�˵���ֻ�����³�1���������µ������������ʱ�����������У��ͨ����
�������ڵ��ȱ���ֱ�ӵ��������ɣ�

--�����ű�(��������update��Щ����Ҫ�����ӣ������Ժ��绹�����ٵ����С�400���͡�5����ֵ��ע��΢��)
---R107
**/


---R107update (select * from  BASS1.G_S_04008_DAY where time_id = 20110331  ) t set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+400) with urcommit---R108update (select * from  BASS1.G_S_04008_DAY where time_id = 20110331  ) t set BASE_BILL_DURATION = char(bigint(BASE_BILL_DURATION)-5) with ur commit
94470 row(s) affected.


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




