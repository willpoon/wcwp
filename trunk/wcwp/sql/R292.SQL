80000551 集团专线初装费
80000547 集团专线包年费
82000146 集团专线包月费、APN端口费
80000129 集团专线包月费
80000548 优惠集团专线包月费,APN端口费
80000472 企业邮箱功能费
80000473 企业邮箱通信费
80000735 商户管家功能费
80000736 商户管家语音通信费
80000737 商户管家短信通信费
80000738 商户管家彩信通信费
80000739 商户管家移动数据流量费
80000746 商户管家业务通信语音资费功能费
80000747 商户管家业务通信套餐资费功能费
------------
select bill_item,sum(primal_fee-bill_discount+adj_fee)/100 from (
select * from ngzc.user_bill_dtl_0891201205 union all
select * from ngzc.user_bill_dtl_0892201205 union all
select * from ngzc.user_bill_dtl_0893201205 union all
select * from ngzc.user_bill_dtl_0894201205 union all
select * from ngzc.user_bill_dtl_0895201205 union all
select * from ngzc.user_bill_dtl_0896201205 union all
select * from ngzc.user_bill_dtl_0897201205)
where bill_item in (
80000551, --集团专线初装费
80000547, --集团专线包年费
82000146, --集团专线包月费、APN端口费
80000129, --集团专线包月费
80000548 --优惠集团专线包月费,APN端口费
)group by bill_item


select count(0)
from dw_acct_shoulditem_201205
where item_id in 
 (
80000551, --集团专线初装费
80000547, --集团专线包年费
82000146, --集团专线包月费、APN端口费
80000129, --集团专线包月费
80000548 --优惠集团专线包月费,APN端口费
)
