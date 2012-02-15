REF:INT_FIX_TMP.TCL
/**
ALTER TABLE g_s_03004_03005_R235_adj ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE

insert into g_s_03004_03005_R235_adj  
select '201201'
,a.user_id   
, case when c.user_id is null then '0' else '1' end iffeezero  
, case when d.user_id is null then '0' else '1' end if03004  
from  (                  
	select distinct user_id 
	from BASS1.G_S_21003_MONTH_mobile   a                          
	,int_02004_02008_month_stage b                  
	where a.product_no = b.product_no                 
	and  b.usertype_id NOT IN ('2010','2020','2030','9000')                 
	and b.test_flag = '0' 
	) a  --在网用户
left join  (
		select user_id                 
		from g_s_03004_month                 
		where time_id = 201201                 
		and                   ( int(substr(ACCT_ITEM_ID,2))/100 in (1,2,3)                                 
		or ACCT_ITEM_ID in ('0401','0403','0407')                  )                  
		group by user_id                   
		having sum(bigint(FEE_RECEIVABLE)) > 0 
	    ) b  on  a.user_id = b.user_id  --有语音收入的用户
left join (    select  user_id from  g_s_03004_month                   
		where time_id = 201201                
		and ( int(substr(ACCT_ITEM_ID,2))/100 in (1,2,3)                         
		or ACCT_ITEM_ID in ('0401','0403','0407')
		)                 
		group by user_id  having sum(bigint(FEE_RECEIVABLE)) <= 0                              
	    ) c on a.user_id = c.user_id --03004中语音费<=0的用户
left join (select distinct  user_id from  g_s_03004_month                   
		where time_id = 201201 
	    ) d on a.user_id = d.user_id  --用户是否在03004中
where b.user_id is  null
**/
 
 select count(0) from g_s_03004_03005_R235_adj
1
62331

select *from g_s_03004_03005_R235_adj

66688 row(s) affected.
75573 row(s) affected.2012.01
/**
ALTER TABLE G_S_03004_MONTH_ADJ_BAK ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE
ALTER TABLE G_S_03005_MONTH_ADJ_BAK ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE
**/

--抓取科目为0100的，费用为0的用户，作为调整的目标号码
select count(0) from G_S_03004_MONTH_ADJ_BAK
select count(0) from G_S_03005_MONTH_ADJ_BAK

select * from G_S_03004_MONTH a
where a.time_id = 201201 
fetch first 10 rows only

/**
insert into G_S_03004_MONTH_ADJ_BAK  
select a.* from G_S_03004_MONTH a
	,g_s_03004_03005_R235_adj b  
	where a.time_id = 201201 
	and a.ACCT_ITEM_ID = '0101' 
	and a.user_id = b.user_id  
	and b.IFFEEZERO = '1'
    and a.FEE_RECEIVABLE = '0'
 **/
 
 
27506 row(s) affected.


27466 row(s) affected.


select  ACCT_ITEM_ID ,count(0)
from G_S_03004_MONTH a
,g_s_03004_03005_R235_adj b
where a.time_id = 201201 
	and a.user_id = b.user_id  
	and b.IFFEEZERO = '1'
group by ACCT_ITEM_ID

/**
insert into G_S_03005_MONTH_ADJ_BAK  
select a.* from G_S_03005_MONTH a
	,g_s_03004_03005_R235_adj b  
	where a.time_id = 201201 
	and a.ITEM_ID = '0100'
	and a.user_id = b.user_id  
	and b.IFFEEZERO = '1'
	and a.SHOULD_FEE = '0'
**/


27572 row(s) affected.

select count(*) from G_S_03005_MONTH_ADJ_BAK a
,(select user_id,max(acct_id) acct_id from G_S_03005_MONTH_ADJ_BAK group by user_id) b 
where a.user_id = b.user_id and a.acct_id <> b.acct_id


/**
delete 
 from G_S_03005_MONTH_ADJ_BAK a
where (user_id,acct_id) not in  (select user_id,max(acct_id) acct_id from G_S_03005_MONTH_ADJ_BAK group by user_id)

**/

1
27506

106 row(s) affected.

 


select count(0) from 
(select * from G_S_03004_MONTH  a 
where time_id = 201201
and  a.FEE_RECEIVABLE = '0'
and  a.ACCT_ITEM_ID = '0101' 
) a
where exists (select 1 from g_s_03004_03005_R235_adj b where a.user_id = b.user_id )
1
27506



select a.* from G_S_03004_MONTH a
	,g_s_03004_03005_R235_adj b  
	where a.time_id = 201201 
	and a.ACCT_ITEM_ID = '0101' 
	and a.user_id = b.user_id  
	and b.IFFEEZERO = '1'
    and a.FEE_RECEIVABLE = '0'
 
27578 row(s) affected.

27586 row(s) affected.

select count(0) from G_S_03005_MONTH_ADJ_BAK

select *from G_S_03004_MONTH_ADJ_BAK


select count(0),count(distinct user_id),count(distinct user_id||ACCT_ITEM_ID)
,sum(bigint(FEE_RECEIVABLE)) 
,sum(bigint(FAV_CHRG)) 
from G_S_03004_MONTH_ADJ_BAK
1	2	3	4	5
27506	27506	27506	0	-308787360


select count(0),count(distinct user_id),count(distinct user_id||ITEM_ID) 
,sum(bigint(SHOULD_FEE)) 
from G_S_03005_MONTH_ADJ_BAK
1	2	3	4
27578	27506	27506	0


/**
delete from 
(select * from G_S_03004_MONTH  a 
where time_id = 201201
and  a.FEE_RECEIVABLE = '0'
and  a.ACCT_ITEM_ID = '0101' 
) a
where exists (select 1 from g_s_03004_03005_R235_adj b where a.user_id = b.user_id and b.IFFEEZERO = '1' )
 
27506 row(s) affected.
**/

27466 row(s) affected.



/**
insert into G_S_03004_MONTH
select 
         TIME_ID
        ,USER_ID
        ,ACCT_ITEM_ID
        ,BILL_CYC_ID
        ,'1' FEE_RECEIVABLE
        ,FAV_CHRG
from    G_S_03004_MONTH_ADJ_BAK
where time_id = 201201

 
27506 row(s) affected.

27466 row(s) affected.


**/
     

select a.* from G_S_03005_MONTH a
	,g_s_03004_03005_R235_adj b  
	where a.time_id = 201201 
	and a.ITEM_ID = '0100'
	and a.user_id = b.user_id  
	and b.IFFEEZERO = '1'
	and a.SHOULD_FEE = '0'


/**
delete from 
(select * from G_S_03005_MONTH  a 
where time_id = 201201
and  a.SHOULD_FEE = '0'
and  a.ITEM_ID = '0100' 
) a
where exists (select 1 from g_s_03004_03005_R235_adj b where a.user_id = b.user_id and b.IFFEEZERO = '1' )
**/
 
27578 row(s) affected.


27572 row(s) affected.


/**


insert into G_S_03005_MONTH
select 
         TIME_ID
        ,ACCT_ID
        ,USER_ID
        ,BILL_CYC_ID
        ,ITEM_ID
        ,'1' SHOULD_FEE
from    G_S_03005_MONTH_ADJ_BAK
where time_id = 201201

27506 row(s) affected.

27466 row(s) affected.


**/
     
     
     
select user_id,count(0)
from G_S_03005_MONTH_ADJ_BAK
group by user_id having count(0) > 1

89160000702503     

select *from   G_S_03005_MONTH_ADJ_BAK
where user_id = '89160000702503'


TIME_ID	ACCT_ID	USER_ID	BILL_CYC_ID	ITEM_ID	SHOULD_FEE
201201	89160001691263      	89160000702503      	201201	0100	0         
201201	89160000766575      	89160000702503      	201201	0100	0         



CU3320<=CU3321+CU3322+CU3323




delete from (select * from G_S_03004_MONTH where time_id = 201201) a 
where 


--update  G_S_03004_month set ACCT_ITEM_ID = '0401' where time_id = 201110 and ACCT_ITEM_ID = '0400'



UPDATE (select * from  g_i_06001_month where time_id = 201201 ) a 
SET STUD_CNT = '5546'
WHERE STUD_CNT = '5000'
AND SCHOOL_ID = '89189400000001'







select STMT_TEXT  from db2inst1.MONIT_sql
 where 
 upper(STMT_TEXT) like '%0300%'
 and upper(STMT_TEXT) like '%DELETE%'
 and upper(STMT_TEXT) NOT like '%||%'
 --AND HOUR(SNAPSHOT_TIME) > 12
order by snapshot_time



 where not exists (select 1 from bass1.g_s_03004_month b  where a.user_id= b.USER_ID and b.time_id = 201104)
 
 
 
 /**
select sum(bigint(FEE_RECEIVABLE)) from  G_S_03004_MONTH where time_id = 201201
**/

12643326679
1
14788994965
1
14789022471
1
13628555574
13628569761
 /**
select sum(bigint(SHOULD_FEE)) from  G_S_03005_MONTH where time_id = 201201
**/

12643326679
1
14788994965

1
14789022471
1
13628555574


13628569762


ACCT_ITEM_ID	2	3	   
0101	4	4	   
0201	5	5	   
0203	14189	14189	   
0401	40	40	   
0501	20795	20795	   
0519	11484	11484	   
0520	10938	10938	   
0521	137	137	   
0522	1	1	   
0525	4	4	   
0527	1	1	   
0599	12147	12147	   
0605	122	122	   
0611	13	13	   
0613	5256	5256	   
0615	244	244	   
0626	4815	4815	   
0627	11695	11695	   
0631	7	7	   
0633	4161	4161	   
0635	7201	7201	   
0637	3419	3419	   
0638	22	22	   
0639	1187	1187	   
0699	666	666	   
0715	5	5	   
0716	5	5	   
0901	20810	20810	   


select USER_ID,SHOULD_FEE
from G_S_03005_MONTH_ADJ_BAK2
except
select user_id,FEE_RECEIVABLE
from G_S_03004_MONTH_ADJ_BAK2
except
			

USER_ID	2	   
89160000391181      	0         	   

select * from 	G_S_03004_MONTH_ADJ_BAK2
where user_id = '89160000391181'
null

select * from 	G_S_03005_MONTH_ADJ_BAK2
where user_id = '89160000391181'

TIME_ID	ACCT_ID	USER_ID	BILL_CYC_ID	ITEM_ID	SHOULD_FEE	   
201201	89160000446991      	89160000391181      	201201	0200	0         	   
						
select * from g_s_03004_03005_R235_adj
where              user_id = '89160000391181'

TIME_ID	USER_ID	IFFEEZERO	IF03004	   
201201	89160000391181 	1	1	   


select * from 	G_S_03004_MONTH
where user_id = '89160000391181'				
and time_id = 201201
               
89160000391181


select * from 	G_S_03005_MONTH
where user_id = '89160000391181'				
and time_id = 201201
               
               
               
            
            select (
select sum(bigint(FEE_RECEIVABLE)) from  G_S_03004_MONTH where time_id = 201201
) 
-
(
select sum(bigint(SHOULD_FEE)) from  G_S_03005_MONTH where time_id = 201201
) diff030040_3005
from bass2.dual

