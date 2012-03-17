1.bass2¿ÎÕ¯±»bass1∂‡£∫
1.1 ≤‚ ‘ª˙¿ÎÕ¯£∫bass2À„¿ÎÕ¯£¨bass1≤ªÀ„¿ÎÕ¯


select count(user_id) 
                    from bass2.dw_product_20120314
                   where usertype_id in (1,2,9) 
                     and day_off_mark = 1 
                     and userstatus_id not in (1,2,3,6,8)
                     and test_mark<>1


1
137

        declare global temporary table session.int_check_user_status
                                (
                           user_id        CHARACTER(15),
                           product_no     CHARACTER(15),
                           test_flag      CHARACTER(1),
                           sim_code       CHARACTER(15),
                           usertype_id    CHARACTER(4),
                           create_date    CHARACTER(15),
                           time_id        int
                                )                            
                                partitioning key           
                                 (
                                   user_id    
                                 ) using hashing           
                                with replace on commit preserve rows not logged in tbs_user_temp

select count(0) from session.int_check_user_status

insert into session.int_check_user_status (
                     user_id    
                    ,product_no 
                    ,test_flag  
                    ,sim_code   
                    ,usertype_id  
                    ,create_date
                    ,time_id )
select e.user_id
                    ,e.product_no  
                    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
                    ,e.sim_code
                    ,f.usertype_id  
                    ,e.create_date  
                    ,f.time_id       
from (select user_id,create_date,product_no,sim_code,usertype_id
            ,row_number() over(partition by user_id order by time_id desc ) row_id   
  from bass1.g_a_02004_day
  where time_id<=20120314 ) e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
                           from bass1.g_a_02008_day
                           where time_id<=20120314 ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1

select tabname from syscat.tables where tabname like '%02004%'                        


select count(distinct user_id) from   bass1.g_a_02004_02008_stage 
                        where USERSTATUS IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id=20120314


                          


select user_id from   bass1.g_a_02004_02008_stage 
                        where USERSTATUS IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id=20120314
except
select user_id 
from bass2.dw_product_20120314
where usertype_id in (1,2,9) 
 and day_off_mark = 1 
 and userstatus_id not in (1,2,3,6,8)
 and test_mark<>1


USER_ID
89160001755027 
89160001755201 


select user_id 
from bass2.dw_product_20120314
where usertype_id in (1,2,9) 
 and day_off_mark = 1 
 and userstatus_id not in (1,2,3,6,8)
 and test_mark<>1
except
select user_id from   bass1.g_a_02004_02008_stage    
                        where USERSTATUS IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id=20120314                     

89157334192161	   
89160000665299	   
89160000667375	   

select a.* from 
 bass1.g_a_02004_02008_stage a
 where a.USER_ID 
 in(
 select user_id 
from bass2.dw_product_20120314
where usertype_id in (1,2,9) 
 and day_off_mark = 1 
 and userstatus_id not in (1,2,3,6,8)
 and test_mark<>1
except
select user_id from   bass1.g_a_02004_02008_stage    
                        where USERSTATUS IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id=20120314                     

 )
 
 
 

USER_ID
89160001741974 

select * from 

select *
from bass2.dw_product_bass1_20120314
where  user_id in ('89160001965964')

 
 
 20120101	89560001672362      	2020
20120101	89560001668643      	1010

dim_cust_status


89503000006732
89503000248463


select * from bass2.dw_cust_20120101
where cust_id in ('89503000006732','89503000248463')


select * from   bass1.g_a_02004_02008_stage 
 where user_id = '89160001965964'
 
 
 
select *
from bass2.dw_product_bass1_20120313
where  user_id in ('89160001965964')



select time_id,sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day 
where time_id / 100 = 201203
group by time_id
order by 1 desc 



select time_id,sum(bigint(td_tnet_data_flux)) from bass1.g_s_22203_day 
where time_id / 100 = 201203
group by time_id
order by 1 desc

