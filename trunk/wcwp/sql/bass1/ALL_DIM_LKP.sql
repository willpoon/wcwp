
--~   82000009	���Ű���
--~   80000035	���Ű��·���
--~   80000110	���ŷѣ�����ţ�
--~   80000079	����ͨ�ŷ�
--~   82000109	�������Ŀ
--~   82000038	���ж��ŷ�
--~   82000005	���ڶ���

select * from   BASS1.ALL_DIM_LKP
where xzbas_value in ('82000009','80000035','80000110','80000079','82000109','82000038','82000005')
and BASS1_TBID in ('BASS_STD1_0074','BASS_STD1_0152')
--~   XZBAS_TBNAME	XZBAS_COLNAME	XZBAS_VALUE	BASS1_TBN_DESC	BASS1_TBID	BASS1_VALUE	BASS1_VALUE_DESC	   
--~   dim_acct_item	���Ű��·���	80000035	��Ŀ��Ŀ	BASS_STD1_0074	0633	��������ҵ��ͨ�ŷ�	   
--~   dim_acct_item	���ŷѣ�����ţ�	80000110	��Ŀ��Ŀ	BASS_STD1_0074	0633	��������ҵ��ͨ�ŷ�	   
--~   dim_acct_item	���Ű���	82000009	��Ŀ��Ŀ	BASS_STD1_0074	0633	��������ҵ��ͨ�ŷ�	   
--~   dim_acct_item	���ж��ŷ�	82000038	��Ŀ��Ŀ	BASS_STD1_0074	0901	��������	   
--~   dim_acct_item	�������Ŀ	82000109	��Ŀ��Ŀ	BASS_STD1_0152	0901	��������	   
							
--~   �����϶�����Ŀͳһӳ�䵽һ��0602�ϡ�
--~   BASS_STD1_0152 �ĳ� BASS_STD1_0074

--~   ���ж��ŷ�?
--~   82000038

select count(0)
from 
bass2.dw_acct_shoulditem_201202
where item_id = 82000038
0
--~   �����޴˿�Ŀ�ļ�¼���Ѵ˿�Ŀ�޳���������0602
 
 select * from   BASS1.ALL_DIM_LKP
where xzbas_value in ('82000009','80000035','80000110','80000079','82000109','82000038','82000005')
and BASS1_TBID in ('BASS_STD1_0074','BASS_STD1_0152')
and xzbas_value not in ('82000038')

XZBAS_TBNAME	XZBAS_COLNAME	XZBAS_VALUE	BASS1_TBN_DESC	BASS1_TBID	BASS1_VALUE	BASS1_VALUE_DESC	   
dim_acct_item	���Ű���	82000009	��Ŀ��Ŀ	BASS_STD1_0074	0633	��������ҵ��ͨ�ŷ�	   
dim_acct_item	���ŷѣ�����ţ�	80000110	��Ŀ��Ŀ	BASS_STD1_0074	0633	��������ҵ��ͨ�ŷ�	   
dim_acct_item	���Ű��·���	80000035	��Ŀ��Ŀ	BASS_STD1_0074	0633	��������ҵ��ͨ�ŷ�	   
dim_acct_item	�������Ŀ	82000109	��Ŀ��Ŀ	BASS_STD1_0152	0901	��������	   
							
update(
 select * from   BASS1.ALL_DIM_LKP
where xzbas_value in ('82000009','80000035','80000110','80000079','82000109','82000038','82000005')
and BASS1_TBID in ('BASS_STD1_0074','BASS_STD1_0152')
and xzbas_value not in ('82000038')
) a set BASS1_TBID = 'BASS_STD1_0074'
where BASS1_TBID = 'BASS_STD1_0152'


update(
 select * from   BASS1.ALL_DIM_LKP
where xzbas_value in ('82000009','80000035','80000110','80000079','82000109','82000038','82000005')
and BASS1_TBID in ('BASS_STD1_0074','BASS_STD1_0152')
and xzbas_value not in ('82000038')
) a set BASS1_VALUE = '0602'


--~   0602	���ڵ�Ե����	�û�������ֻ��û����Ͷ���ʱ������ͨ�ŷ����룬����ԭ���ڵ����ڡ������Ե����



