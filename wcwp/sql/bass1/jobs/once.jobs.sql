/**
once:传4月月数据时(date:5,6,7)，检查03017，03018 手机邮箱是否剔除。


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
once:传4月月数据时(date:3,4)，检查06021经纬度修复情况！校验是否通过。
select * from   G_I_06021_MONTH
where time_id = 201104

**/
/**
检查gprs收入情况。

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
**/

/**
once : 积分数据修复？？有没有总部类？
**/

2011-06-03 10:16:56
INTERFACE_CODE
01005 待确认 (按集团下发报)
02005 老校验
02014 无
02015 无
02016 无
02018	无 检查新资费
02019 无 检查新资费
02020 无
02021 无
02047 R229
06002 无
06021 R238-R254
06022 R238-R254
06023 R238-R254
22009 NULL 
22101 NULL
22103 NULL
22105 NULL
22106 NULL




INTERFACE_CODE
02006 n
02007 n
02017 n
02052 o
03004 R235 ok
03005 n
03012 n
03015 n
03016 n
03017 R192 R216 !!!!!!!!!
03018 R181 R216 !!!!!!!!!!
21003 R229 R235 ok
21006 n
21008 O
21011 N
21012 N
21020 R230 R231 R232 R233 R234  !!!! ok
22036 N
22040 N
22072 N
22081 N
22083 N
22085 N
22086 N new!
22204 
R221
R222
R223
R224
R225
R226
R227
R228
ok!!!!
22303 R208 R210 R212 R214 ok!!
22304 N
22305 N
22306 N
22307 N
22401 N



INTERFACE_CODE
03007 n
05001 n
05002 n
05003 o
21010 n
21013 n
21014 n
21015 n
22013 n
22021 n
22025 n
22032 n
22033 n
22039 n
22041 n
22042 n
22043 n
22067 n
22068 n


INTERFACE_CODE
22049 n
22050 n
22052 n
22055 n
22056 n
22061 R242 R243 
22062 nn
22063 R246 R247
22064 nn
22065 n
