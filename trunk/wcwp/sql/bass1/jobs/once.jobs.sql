/**
once:��4��������ʱ(date:5,6,7)�����03017��03018 �ֻ������Ƿ��޳���


select sum(income)*1.00/100
from (
select sum(bigint(income)) income from   g_s_03017_month
where time_id = 201104
and manage_mod = '2'
and ent_busi_id = '1220'
union all 
select sum(bigint(income)) income from   g_s_03018_month
where time_id = 201104
and manage_mod = '2'
and ent_busi_id = '1220'
) t
                    
                    
select count(0)
from 
(
select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
from 
(
select * from G_A_02061_DAY
where ENTERPRISE_BUSI_TYPE = '1220'
and  MANAGE_MODE = '2'
and length(trim(user_id)) = 14
) t
) t2
where rn = 1 and STATUS_ID ='1'
**/

/**
once:��4��������ʱ(date:3,4)�����06021��γ���޸������У���Ƿ�ͨ����
select * from   G_I_06021_MONTH
where time_id = 201104

**/

���gprs���������

select count(0)
from  bass1.g_s_03004_month
where time_id = 201102
and ACCT_ITEM_ID in ('0626','0627')

788


select count(0)
from  bass1.g_s_03004_month
where time_id = 201103
and ACCT_ITEM_ID in ('0626','0627')

864286

select count(0)
from  bass1.g_s_03004_month
where time_id = 201104
and ACCT_ITEM_ID in ('0626','0627')
784986
