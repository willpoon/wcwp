1��22073����KPI�漰��У��R163��R164��R165��R166��R167��R168��R169��R170��R171��R172(����ΪBASS1_INT_CHECK_COMP_KPI_DAY.tcl)��
21007�����漰��У��C1(����BASS1_INT_CHECK_C1K844TO46_TO_DAY.tcl)���꣬�����������ɵģ���Ҫ�����κ����ݵ���������ֱ�ӵ����ɺ����к�����

2��R173/R174 У��澯��֪ͨ�ң�������ַ�ȷ���Ƿ����������ϱ���

3. R159_4 ����澯 ������Ƚ�Ƶ����
22012��kpi�ӿ��漰��һ���Լ�飬�糬�꣬������е���(����ΪBASS1_INT_CHECK_INDEX_SAME_DAY.tcl)������22012�ӿڵ�ָ�꣬���ܱ�����ȣ�
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
  and time_id=int(replace(char(current date - 1 days),'-',''));

--target1 �Ƕ���ֵ
--target2 ��һ��ֵ
--һ��ֵ��Զ�ǶԵģ����õ���Ҫ�����Ƕ���ֵ�����Ļ����ǰѶ���ֵ���ɺ�һ��ֵһ����
--����������
update bass1.g_s_22012_day set m_off_users='��target2һ��' 
where time_id=int(replace(char(current date - 1 days),'-',''))



--�����ű���''�����һ����ֵ����
--�����ͻ���
update bass1.g_s_22012_day set m_off_users='' 
where time_id=int(replace(char(current date - 1 days),'-',''));
commit;

--�����ͻ���
update bass1.g_s_22012_day set m_new_users='' 
where time_id=int(replace(char(current date - 1 days),'-',''));
commit;

--�������ͻ���
update bass1.g_s_22201_day set mtl_td_3gbook_mark='' 
where time_id=int(replace(char(current date - 1 days),'-',''));
commit;




--�����쳣�뼰ʱ������������

