
--~   82000009	短信包月
--~   80000035	短信包月费用
--~   80000110	短信费（与电信）
--~   80000079	短信通信费
--~   82000109	短信虚科目
--~   82000038	所有短信费
--~   82000005	网内短信

select * from   BASS1.ALL_DIM_LKP
where xzbas_value in ('82000009','80000035','80000110','80000079','82000109','82000038','82000005')
and BASS1_TBID in ('BASS_STD1_0074','BASS_STD1_0152')
--~   XZBAS_TBNAME	XZBAS_COLNAME	XZBAS_VALUE	BASS1_TBN_DESC	BASS1_TBID	BASS1_VALUE	BASS1_VALUE_DESC	   
--~   dim_acct_item	短信包月费用	80000035	帐目科目	BASS_STD1_0074	0633	其他数据业务通信费	   
--~   dim_acct_item	短信费（与电信）	80000110	帐目科目	BASS_STD1_0074	0633	其他数据业务通信费	   
--~   dim_acct_item	短信包月	82000009	帐目科目	BASS_STD1_0074	0633	其他数据业务通信费	   
--~   dim_acct_item	所有短信费	82000038	帐目科目	BASS_STD1_0074	0901	其它费用	   
--~   dim_acct_item	短信虚科目	82000109	帐目科目	BASS_STD1_0152	0901	其它费用	   
							
--~   把以上二经科目统一映射到一经0602上。
--~   BASS_STD1_0152 改成 BASS_STD1_0074

--~   所有短信费?
--~   82000038

select count(0)
from 
bass2.dw_acct_shoulditem_201202
where item_id = 82000038
0
--~   几乎无此科目的记录。把此科目剔除，不加入0602
 
 select * from   BASS1.ALL_DIM_LKP
where xzbas_value in ('82000009','80000035','80000110','80000079','82000109','82000038','82000005')
and BASS1_TBID in ('BASS_STD1_0074','BASS_STD1_0152')
and xzbas_value not in ('82000038')

XZBAS_TBNAME	XZBAS_COLNAME	XZBAS_VALUE	BASS1_TBN_DESC	BASS1_TBID	BASS1_VALUE	BASS1_VALUE_DESC	   
dim_acct_item	短信包月	82000009	帐目科目	BASS_STD1_0074	0633	其他数据业务通信费	   
dim_acct_item	短信费（与电信）	80000110	帐目科目	BASS_STD1_0074	0633	其他数据业务通信费	   
dim_acct_item	短信包月费用	80000035	帐目科目	BASS_STD1_0074	0633	其他数据业务通信费	   
dim_acct_item	短信虚科目	82000109	帐目科目	BASS_STD1_0152	0901	其它费用	   
							
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


--~   0602	国内点对点短信	用户向国内手机用户发送短信时产生的通信费收入，包含原国内的网内、网间点对点短信



