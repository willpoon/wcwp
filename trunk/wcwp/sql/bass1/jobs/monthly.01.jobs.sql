/**

2���µ׵���BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl��R107/R108���꣬�˵���ֻ�����³�1���������µ������������ʱ�����������У��ͨ����
�������ڵ��ȱ���ֱ�ӵ��������ɣ�

--�����ű�(��������update��Щ����Ҫ�����ӣ������Ժ��绹�����ٵ����С�400���͡�5����ֵ��ע��΢��)
---R107
**/


--���´��벻���޸�ֱ������

select * from bass1.g_rule_check 
where rule_code in ('R107') 
and time_id = int(replace(char(current date - 1 days),'-',''))
order by time_id desc


select * from bass1.g_rule_check 
where rule_code in ('R108') 
and time_id = int(replace(char(current date - 1 days),'-',''))
order by time_id desc

---R107
update (select * from  BASS1.G_S_04008_DAY where time_id = int(replace(char(current date - 1 days),'-',''))  ) t 
set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+400) with ur

---R108
update (select * from  BASS1.G_S_04008_DAY where time_id = int(replace(char(current date - 1 days),'-',''))  ) t 
set BASE_BILL_DURATION = char(bigint(BASE_BILL_DURATION)-5) with ur 



/**
3��1���սӿ���������֮���������Ҽ��Ž��·������û����ݣ������������(�������·�)����Ȼ�ڶ�����սӿ����ݽ�������
��app�û���¼ 172.16.5.44 
$ cd /bassapp/backapp/bin/bass1_lst
$ ./bass1_lst.sh yyyy-mm (��Ȼ��)

select time_id,count(0) from bass1.g_user_lst
group by time_id 
order by 1 desc 


**/
���

./bass1_lst.sh 2012-06





/**

���£�05001 05002 ���� �� ����ɡ�

**/

