('R233','R234')  为同一个脚本！

1.日接口：20120408 日 有一批广告号 ，12位，到期失效。按离网处理。

2.月接口：12位的号码截取为11位处理。 12位截短后，与11位号码有重复。

3.重复的号码很多为在网，导致月计算得到的离网偏少。

解决方案： 把这一批重复的号码设法置为离网。而且是取同时存在于上月和当月接口中的号码。


调数依据：
Step2:取同时存在于上月和当月接口中的运营商品牌为联通移动的号码及其在上月中末次通信时间和当月的首次通信时间，取其中上月末次通信时间距统计日等于90天，且当月首次通信时间晚于统计日的记录数；
                                        
                                        
 select * from bass1.g_rule_check where rule_code in ('R233','R234') 
order by time_id desc

201204	R234	18216.00000	18367.00000	-0.00822	0.00000	   
201204	R233	20795.00000	20887.00000	-0.00440	0.00000	   
                INSERT INTO BASS1.G_RULE_CHECK VALUES (201204,'R233',(19399+702),20887,-0.0376310623833006177,0) 
                INSERT INTO BASS1.G_RULE_CHECK VALUES (201204,'R234',(16393+778),18367,-0.0651167855392824087,0) 
				
号码汇总计算出的"联通移动离网用户数"和汇总接口中的联通移动离网客户数的偏差≤3％

从月接口中取离网：


CREATE TABLE "BASS1   "."G_I_21020_MONTH_D_U_NUM"  (
                  "COMP_PRODUCT_NO" CHAR(15) NOT NULL , 
                  "TYPE" CHAR(1) NOT NULL  )   
                 DISTRIBUTE BY HASH(
                 "COMP_PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 

    INSERT INTO G_I_21020_MONTH_D_U_NUM
                                select distinct COMP_PRODUCT_NO,'1'  from    
                                table (
                                select COMP_PRODUCT_NO,COMP_LAST_DATE
                                from         G_I_21020_MONTH
                                where COMP_BRAND_ID = '021000' 
                                and time_id = 201203
                                and  COMP_PRODUCT_NO not in (
                                select COMP_PRODUCT_NO
                                from         G_I_21020_MONTH
                                where COMP_BRAND_ID = '021000' 
                                and time_id = 201204
                                )
                                ) a , (select * from  BASS1.G_I_21020_MONTH_calendar where date_seq like '201204%') b 
                                where 
                                (days(date(date2))-
                                 days(date(substr(COMP_LAST_DATE,1,4)||'-'
                                 ||substr(COMP_LAST_DATE,5,2)||'-'
                                 ||substr(COMP_LAST_DATE,7,2))))=90       
19399 row(s) affected.
    INSERT INTO G_I_21020_MONTH_D_U_NUM
                                select distinct A.COMP_PRODUCT_NO,'2'  from    
                        ( 
                         select COMP_PRODUCT_NO,COMP_LAST_DATE
                         from G_I_21020_MONTH where time_id = 201203 and  COMP_BRAND_ID = '021000' 
                        ) a 
                        ,
                        (
                         select COMP_PRODUCT_NO,COMP_BEGIN_DATE
                         from G_I_21020_MONTH where time_id = 201204 and  COMP_BRAND_ID = '021000' 
                        ) b 
                        ,BASS1.G_I_21020_MONTH_calendar c
                        where a.COMP_PRODUCT_NO  = b.COMP_PRODUCT_NO 
                        and 
                        (days(date(date2))-
                         days(date(substr(COMP_LAST_DATE,1,4)||'-'||substr(COMP_LAST_DATE,5,2)||'-'||substr(COMP_LAST_DATE,7,2))))=90
                         and  char(COMP_BEGIN_DATE  ) > char(date_seq)
                          and c.date_seq like '201204%'
702 row(s) affected.
SELECT COUNT(0),COUNT(DISTINCT COMP_PRODUCT_NO)
FROM 
G_I_21020_MONTH_D_U_NUM


SELECT *
FROM 
G_I_21020_MONTH_D_U_NUM
fetch first 10 rows only


 
select *
from G_I_21020_MONTH where time_id = 201203
and COMP_PRODUCT_NO = '13228922656'





         select count(0)  from    
                        ( 
                         select COMP_PRODUCT_NO,COMP_LAST_DATE
                         from G_I_21020_MONTH where time_id = 201203 and  COMP_BRAND_ID = '021000' 
                        ) a 
                        ,
                        (
                         select COMP_PRODUCT_NO,COMP_BEGIN_DATE
                         from G_I_21020_MONTH where time_id = 201204 and  COMP_BRAND_ID = '021000' 
                        ) b 
                        ,BASS1.G_I_21020_MONTH_calendar c
                        where a.COMP_PRODUCT_NO  = b.COMP_PRODUCT_NO 
                        and 
                        (days(date(date2))-
                         days(date(substr(COMP_LAST_DATE,1,4)||'-'||substr(COMP_LAST_DATE,5,2)||'-'||substr(COMP_LAST_DATE,7,2))))=90
                          and c.date_seq like '201204%'

1540

19399+702



CREATE TABLE "BASS1   "."G_I_21020_MONTH_D_T_NUM"  (
                  "COMP_PRODUCT_NO" CHAR(15) NOT NULL , 
                  "TYPE" CHAR(1) NOT NULL  )   
                 DISTRIBUTE BY HASH(
                 "COMP_PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 

    INSERT INTO G_I_21020_MONTH_D_T_NUM
                                select distinct COMP_PRODUCT_NO,'1'  from  
                                table (
                                select COMP_PRODUCT_NO,COMP_LAST_DATE
                                from         G_I_21020_MONTH
                                where COMP_BRAND_ID = '031000' 
                                and time_id = 201203
                                and  COMP_PRODUCT_NO not in (
                                select COMP_PRODUCT_NO
                                from         G_I_21020_MONTH
                                where COMP_BRAND_ID = '031000' 
                                and time_id = 201204
                                )
                                ) a , (select * from  BASS1.G_I_21020_MONTH_calendar where date_seq like '201204%') b 
                                where 
                                (days(date(date2))-
                                 days(date(substr(COMP_LAST_DATE,1,4)||'-'
                                 ||substr(COMP_LAST_DATE,5,2)||'-'
                                 ||substr(COMP_LAST_DATE,7,2))))=90   


                         
    INSERT INTO G_I_21020_MONTH_D_T_NUM
                                select distinct A.COMP_PRODUCT_NO,'2'  from     
                        ( 
                         select COMP_PRODUCT_NO,COMP_LAST_DATE
                         from G_I_21020_MONTH where time_id = 201203 and  COMP_BRAND_ID = '031000' 
                        ) a 
                        ,
                        (
                         select COMP_PRODUCT_NO,COMP_BEGIN_DATE
                         from G_I_21020_MONTH where time_id = 201204 and  COMP_BRAND_ID = '031000' 
                        ) b 
                        ,BASS1.G_I_21020_MONTH_calendar c
                        where a.COMP_PRODUCT_NO  = b.COMP_PRODUCT_NO 
                        and 
                        (days(date(date2))-
                                        days(date(substr(COMP_LAST_DATE,1,4)||'-'||substr(COMP_LAST_DATE,5,2)||'-'||substr(COMP_LAST_DATE,7,2))))
                         =90
                         and  char(COMP_BEGIN_DATE  ) > char(date_seq)
                          and c.date_seq like '201204%'
                                 
                                 

SELECT TYPE, COUNT(0),COUNT(DISTINCT COMP_PRODUCT_NO)
FROM 
G_I_21020_MONTH_D_T_NUM
group by TYPE
                                
TYPE	2	3	   
2	778	778	   
1	16393	16393	   
			
                                

 select count(0) from     
                        ( 
                         select COMP_PRODUCT_NO,COMP_LAST_DATE
                         from G_I_21020_MONTH where time_id = 201203 and  COMP_BRAND_ID = '031000' 
                        ) a 
                        ,
                        (
                         select COMP_PRODUCT_NO,COMP_BEGIN_DATE
                         from G_I_21020_MONTH where time_id = 201204 and  COMP_BRAND_ID = '031000' 
                        ) b 
                        ,BASS1.G_I_21020_MONTH_calendar c
                        where a.COMP_PRODUCT_NO  = b.COMP_PRODUCT_NO 
                        and 
                        (days(date(date2))-
                                        days(date(substr(COMP_LAST_DATE,1,4)||'-'||substr(COMP_LAST_DATE,5,2)||'-'||substr(COMP_LAST_DATE,7,2))))
                         =90
--                         and  char(COMP_BEGIN_DATE  ) > char(date_seq)
                          and c.date_seq like '201204%'
                                 
1933
       ;                          
      

CREATE TABLE "BASS1   "."G_I_21020_MONTH_D_ADJ_NUM"  (
                  "COMP_PRODUCT_NO" CHAR(15) NOT NULL , 
                  "BRAND" CHAR(1) NOT NULL  )   
                 DISTRIBUTE BY HASH(
                 "COMP_PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 

     
insert into G_I_21020_MONTH_D_ADJ_NUM
 select distinct A.COMP_PRODUCT_NO,'U'  from    
                        ( 
                         select COMP_PRODUCT_NO,COMP_LAST_DATE
                         from G_I_21020_MONTH where time_id = 201203 and  COMP_BRAND_ID = '021000' 
                        ) a 
                        ,
                        (
                         select COMP_PRODUCT_NO,COMP_BEGIN_DATE
                         from G_I_21020_MONTH where time_id = 201204 and  COMP_BRAND_ID = '021000' 
                        ) b 
                        ,BASS1.G_I_21020_MONTH_calendar c
                        where a.COMP_PRODUCT_NO  = b.COMP_PRODUCT_NO 
                        and 
                        (days(date(date2))-
                         days(date(substr(COMP_LAST_DATE,1,4)||'-'||substr(COMP_LAST_DATE,5,2)||'-'||substr(COMP_LAST_DATE,7,2))))=90
                         and  char(COMP_BEGIN_DATE  ) <= char(date_seq)
                          and c.date_seq like '201204%'    
    
    
         
insert into G_I_21020_MONTH_D_ADJ_NUM
 select distinct A.COMP_PRODUCT_NO,'T'  from    
                        ( 
                         select COMP_PRODUCT_NO,COMP_LAST_DATE
                         from G_I_21020_MONTH where time_id = 201203 and  COMP_BRAND_ID = '031000' 
                        ) a 
                        ,
                        (
                         select COMP_PRODUCT_NO,COMP_BEGIN_DATE
                         from G_I_21020_MONTH where time_id = 201204 and  COMP_BRAND_ID = '031000' 
                        ) b 
                        ,BASS1.G_I_21020_MONTH_calendar c
                        where a.COMP_PRODUCT_NO  = b.COMP_PRODUCT_NO 
                        and 
                        (days(date(date2))-
                                        days(date(substr(COMP_LAST_DATE,1,4)||'-'||substr(COMP_LAST_DATE,5,2)||'-'||substr(COMP_LAST_DATE,7,2))))
                         =90
                         and  char(COMP_BEGIN_DATE  ) <= char(date_seq)
                          and c.date_seq like '201204%'
                                 

SELECT brand, COUNT(0),COUNT(DISTINCT COMP_PRODUCT_NO)
FROM 
G_I_21020_MONTH_D_ADJ_NUM
group by brand                                 


select * from G_I_21020_MONTH_D_ADJ_NUM a , G_I_21020_MONTH b
where A.COMP_PRODUCT_NO = B.COMP_PRODUCT_NO AND B.TIME_ID = 201204




select * from G_I_21020_MONTH_D_ADJ_NUM a , G_I_21020_MONTH b
where A.COMP_PRODUCT_NO = B.COMP_PRODUCT_NO AND B.TIME_ID = 201204


update (select * from G_I_21020_MONTH where TIME_ID = 201204
and COMP_PRODUCT_NO in (select COMP_PRODUCT_NO from G_I_21020_MONTH_D_ADJ_NUM)
 ) t
set COMP_BEGIN_DATE = COMP_LAST_DATE


select count(0) from G_I_21020_MONTH where TIME_ID = 201204
738125


select time_id , count(0) 
--,  count(distinct time_id ) 
from G_I_21020_MONTH 
group by  time_id 
order by 1 desc
201204	738125	   
201203	710944	   
201202	715177	   
201201	734925	   


select * from app.sch_control_task where control_code like '%R230%'

cat  _INT_CHECK_R230_MONTH.tcl.out.r20120503204210|grep -i R23|grep INSERT
-rw-r--r--   1 app      appdb    3398841  5月  3日 20:12 i_13100_201204_21020_00_001.dat
-rw-r--r--   1 app      appdb        104  5月  3日 20:12 i_13100_201204_21020_00.verf


update (select * from G_I_21020_MONTH where TIME_ID = 201204
and COMP_PRODUCT_NO in (select COMP_PRODUCT_NO from G_I_21020_MONTH_D_ADJ_NUM)
 ) t
set COMP_LAST_DATE = '20120430'


update (select * from G_I_21020_MONTH where TIME_ID = 201204
and COMP_PRODUCT_NO in (select COMP_PRODUCT_NO from G_I_21020_MONTH_D_ADJ_NUM)
 ) t
set COMP_BEGIN_DATE = COMP_LAST_DATE



select *  from    G_I_21020_MONTH_D_ADJ_NUM t,
                        ( 
                         select COMP_PRODUCT_NO,COMP_LAST_DATE
                         from G_I_21020_MONTH where time_id = 201203 
                        ) a 
                        ,
                        (
                         select COMP_PRODUCT_NO,COMP_BEGIN_DATE
                         from G_I_21020_MONTH where time_id = 201204 
                        ) b 
                        ,BASS1.G_I_21020_MONTH_calendar c
                        where a.COMP_PRODUCT_NO  = b.COMP_PRODUCT_NO 
                        and t.COMP_PRODUCT_NO = a.COMP_PRODUCT_NO
                          and c.date_seq like '201204%'
                          

 days(date(substr(COMP_LAST_DATE,1,4)||'-'||substr(COMP_LAST_DATE,5,2)||'-'||substr(COMP_LAST_DATE,7,2))))
                          

values (days(date('2012-04-30'))-
                                        days(date(substr('20120131',1,4)||'-'||substr('20120131',5,2)||'-'||substr('20120131',7,2))))                          
                                        
