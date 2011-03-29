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




--�ӿ��ļ������ؼ��(�������Ϊ56����¼)
select  * from APP.G_FILE_REPORT
where filename like '%20110328%' and err_code='00';


--�ӿڼ�¼�����ؼ��(�������Ϊ56����¼)
select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=1


--��ѯδ���ؽӿ�
select * from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=0




---�½ӿ��ļ������ؼ��(���ڽӿ����ͣ���Ӧ�Ľ�������)
select  * 
from APP.G_FILE_REPORT
where filename like '%_201101_%' 
and err_code='00'
and length(filename)=length('s_13100_201002_03014_01_001.dat')
order by deal_time desc;



--���ȳ����ʱ:
select A.*,char(a.RUNTIME/60)||'min',char(a.RUNTIME/60/60)||'hr' from   app.sch_control_runlog A
where control_code like 'BASS1%01004%'
and a.RUNTIME/60 > 10
ORDER BY RUNTIME DESC 


 select distinct user_id from  G_A_02062_DAY
  where time_id <20110301
 and STATUS_ID = '1'
 except
 select distinct a.user_id from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and usertype_id NOT IN ('2010','2020','2030','9000')
        and test_flag='0'








1��22073����KPI�漰��У��R163��R164��R165��R166��R167��R168��R169��R170��R171��R172(����ΪBASS1_INT_CHECK_COMP_KPI_DAY.tcl)��
21007�����漰��У��C1(����BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl)���꣬�����������ɵģ���Ҫ�����κ����ݵ���������ֱ�ӵ����ɺ����к�����

�磺
select * from bass1.g_rule_check where rule_code in ('R171') order by time_id desc

2��22012��kpi�ӿ��漰��һ���Լ�飬�糬�꣬������е���(����ΪBASS1_INT_CHECK_INDEX_SAME_DAY.tcl)������22012�ӿڵ�ָ�꣬���ܱ�����ȣ�
һ�����ܵ�����(��Ƶ�����ǡ������û��������ָ�꣬��һ���û��ͳ��꣬��û����ھ���һ�����������δ�����û����ϱ�)��

�磺
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
;

20110322	�����ͻ���	109.00000	107.00000	0.01869
20110323	�����ͻ���	124.00000	120.00000	0.03333

select * from     
bass1.g_s_22012_day 
where time_id=int(replace(char(current date - 1 days),'-',''));

20110328	20110328	2364      	1627492     	22871900    	369353      	4151751     	137       	322376      
20110328	R159_4	137.00000	138.00000	-0.00724	0.00000
 



  
  --�����ű���''�����һ����ֵ����
--�����ͻ���
update bass1.g_s_22012_day set m_off_users='' 
where time_id=20110323;
commit;

select * from   APP.G_rule_check

select * from   APP.G_UNIT_INFO


select * from 
bass1.g_rule_check where rule_code = 'R159_4'
and time_id = 20110322


select * from 
bass1.g_rule_check where rule_code = 'R161_15'
where time_id = 20110317

select * from   app.sch_control_task where lower(cmd_line) like '%net%hlr%'

select * from   app.sch_control_runlog where control_code like '%06031%'

select count(0) from   BASS2.DIM_TACNUM_DEVID 
select count(0) from   bass1.G_A_02059_DAY_down20110321

select * from   G_A_02055_DAY

