µ½´ïÊý£º

select user_id
                    from bass2.dw_product_20120527
                   where usertype_id in (1,2,9) 
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1
except
select user_id from G_A_02004_02008_STAGE
		where USERSTATUS NOT IN ('2010','2020','2030','9000')
		  and test_flag='0'
		  


USER_ID             
--------------------
89160002171967      
89160000184970     



select user_id from G_A_02004_02008_STAGE
		where USERSTATUS NOT IN ('2010','2020','2030','9000')
		  and test_flag='0'
except		  
select user_id
		from bass2.dw_product_20120607
	   where usertype_id in (1,2,9) 
		 and userstatus_id in (1,2,3,6,8)
		 and test_mark<>1


		  
		  
		  

select user_id from G_A_02004_02008_STAGE
		where usertype_id NOT IN ('2010','2020','2030','9000')
		  and test_flag='0'
except		  
select user_id
                    from bass2.dw_product_20120527
                   where usertype_id in (1,2,9) 
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1
					 
					 
					 
					 
					 


select count(0),count(distinct user_id) from G_A_02004_02008_STAGE
		where USERSTATUS NOT IN ('2010','2020','2030','9000')
		  and test_flag='0'

	1913997     1913997
	
select count(0),count(distinct user_id)
		from bass2.dw_product_20120527
	   where usertype_id in (1,2,9) 
		 and userstatus_id in (1,2,3,6,8)
		 and test_mark<>1


1           2          
----------- -----------
    1913999     1913999
	
		  
		  
		  


select user_id,product_no,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE ,AGE_id
from bass2.dw_product_20120527
where user_id 
in
(
 '89160002171967'
,'89160000184970'
)



89160002171967      
89160000184970     







select user_id,usertype_id,day_new_mark,test_mark,userstatus_id,day_off_mark,usertype_id ,RECREATE_MARK
,CREATE_DATE,VALID_DATE,STS_DATE,EXPIRE_DATE 
 from bass2.dw_product_20120607
where user_id in (
select user_id from G_A_02004_02008_STAGE
		where USERSTATUS NOT IN ('2010','2020','2030','9000')
		  and test_flag='0'
except		  
select user_id
		from bass2.dw_product_20120607
	   where usertype_id in (1,2,9) 
		 and userstatus_id in (1,2,3,6,8)
		 and test_mark<>1
)
