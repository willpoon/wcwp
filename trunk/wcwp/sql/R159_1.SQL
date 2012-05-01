select user_id from G_A_02004_02008_STAGE
where create_date = '20120428'
and test_flag='0'
except
select user_id
from bass2.dw_product_20120428
where usertype_id in (1,2,9) 
and day_new_mark = 1 and test_mark<>1


select user_id,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE_id
from bass2.dw_product_20120428
where user_id 
in
(
 '89460002113930'
,'89760002116015'
,'89460002114117'
)



select user_id from G_A_02004_02008_STAGE
where create_date = '20120429'
and test_flag='0'
except
select user_id
from bass2.dw_product_20120429
where usertype_id in (1,2,9) 
and day_new_mark = 1 and test_mark<>1
USER_ID             
--------------------
89560002116390      
89160002117537 




select user_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE_id
from bass2.dw_product_20120429
where user_id 
in
(
 '89160002117537'
,'89560002116390'
)

dang tian ruwang , dangtian liwang 
