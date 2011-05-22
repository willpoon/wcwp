/**
**/
from  session.int_check_user_status 
group by product_no having count(0) > 1

from  session.int_check_user_status where 
usertype_id not in ('2010','2020','2030','9000')

	select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=20110315
		select * from bass1.G_A_02054_DAY_0317_1220repair
						            from app.sch_control_runlog 
						            where date(endtime)=date(current date)
						                  and flag=0
                    from bass2.dw_product_20110324 a ,bass2.dwd_cust_msg_20110324 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1
                    from bass2.dw_product_20110324 a ,bass2.dwd_cust_msg_20110324 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1
                    from bass2.dw_product_20110324 a ,bass2.dwd_cust_msg_20110324 b 
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1
for db25.bass1.G_I_77778_DAY
from 
(select distinct EC_TYPE EC_TYPE from   G_I_77778_DAY) a 
where EC_TYPE   in 
	(select code from BASS1.dim_bass1_std_map 
		where interface_code = '77780' 
		and dim_table_id ='BASS_STD1_0002'
  )
  
from 
(select distinct CONTROL_TYPE CONTROL_TYPE from   G_I_77778_DAY) a 
where CONTROL_TYPE  not in 
	(select code from BASS1.dim_bass1_std_map 
		where interface_code = '77780' 
		and dim_table_id ='BASS_STD1_0005'
  )
  
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,ENTERPRISE_ID) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY_MID
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
  
---�м��:
drop table  BASS1.G_I_77780_DAY_MID 

drop table  BASS1.G_I_77780_DAY_MID 
CREATE TABLE BASS1.G_I_77780_DAY_MID (
	 TIME_ID            		INTEGER               ----��������        
	,ENTERPRISE_ID      		CHAR(20)              ----���ſͻ���ʶ    
	,ID                 		CHAR(9)        								----ID*              
	,ENTERPRISE_NAME    		CHAR(60)       								----���ſͻ�����*    
	,ORG_TYPE           		CHAR(5)             					----�������� 							BASS_STD_0001       
		--GROUP_TYPE���淶CHAR(6) 
	,ADDR_CODE          		VARCHAR(10)             					----��ַ����*        
	,CITY               		CHAR(20)            					----���е���*        
	---REGION ���淶            		CHAR(20) 
	,REGION             		VARCHAR(100)            					----����*            
	---COUNTY ���淶            		CHAR(20) 	
	,COUNTY             		VARCHAR(100)            					----����*            
	,DOOR_NO            		CHAR(60)            					----����*            
	,AREA_CODE          		CHAR(5)             					----����*            
	--PHONE_NO1 ���淶  12       		CHAR(11)
	,PHONE_NO1          		varCHAR(111)            					----�绰1*           
	--,PHONE_NO2  ���淶  13        		CHAR(10) 
	,PHONE_NO2          		VARCHAR(110)            					----�绰2*           
	--POST_CODE ���淶  14         		CHAR(6)
	,POST_CODE          		varCHAR(16)             					----��������*        
--	,INDUSTRY_TYPE  ���淶  15    		CHAR(4) 
	,INDUSTRY_TYPE      		VARCHAR(14)             					----��ҵ���� 							BASS_STD1_0113       
	,EMPLOYEE_CNT       		CHAR(8)             					----ְԱ            
	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----��ҵ��λ��*      
	--ECONOMIC_TYPE  ���淶  18    		CHAR(3)
	,ECONOMIC_TYPE      		CHAR(13)             					----�������� 							BASS_STD_0002       
	--OPEN_YEAR  ���淶  19        		CHAR(4)
	,OPEN_YEAR          		varCHAR(14)             					----��ҵ1           
	--OPEN_MONTH ���淶  20        		CHAR(2) 
	,OPEN_MONTH         		varCHAR(22)             					----��ҵ2           
	--SHAREHOLDER���淶CHAR(1) 
	,SHAREHOLDER        		VARCHAR(4)             					----�ع�  								BASS_STD_0005          
	--GROUP_TYPE���淶CHAR(1) 
	,GROUP_TYPE         		VARCHAR(2)             					----�������� 							BASS_STD_0003       
	--MANAGE_STYLE���淶CHAR(1) 
	,MANAGE_STYLE       		VARCHAR(4)             					----��Ӫ��ʽ      				BASS_STD_0004  
	,OPERATE_REVENUE_CLASS	CHAR(2)           						----Ӫҵ������� 					BASS_STD_0006   
	,CAPITAL_CLASS      		CHAR(2)           						----�ʲ�����     					BASS_STD_0007   
	,INDUSTRY_CLASS_CODE		CHAR(2)            	----��ҵ������� 					BASS_STD1_0043   
	,CUST_STATUS        		CHAR(2)             					----���ſͻ�״̬    
	,CUST_INFO_SRC_ID   		CHAR(1)             					----���ſͻ�������Դ
	,UPLOAD_TYPE_ID     		CHAR(1)            	----�ϴ������ʶ    
)
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,ENTERPRISE_ID) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY_MID
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
  
  
    
--,  count(distinct ADDR_CODE ) 
from bass1.G_I_77780_DAY_MID 
group by  ADDR_CODE 
order by 1 


select length(ADDR_CODE) , count(0) 
--,  count(distinct length(ADDR_CODE) ) 
from bass1.G_I_77780_DAY_MID 
group by  length(ADDR_CODE) 
order by 1 

drop table BASS1.G_I_77780_DAY
CREATE TABLE BASS1.G_I_77780_DAY
 (
	 TIME_ID            		INTEGER        NOT NULL       ----��������        
	,ENTERPRISE_ID      		CHAR(20)       					       ----���ſͻ���ʶ    
	,ID                 		CHAR(9)        								----ID*              
	,ENTERPRISE_NAME    		CHAR(60)       								----���ſͻ�����*    
	,ORG_TYPE           		CHAR(5)             					----�������� 							BASS_STD_0001       
	,ADDR_CODE          		CHAR(6)             					----��ַ����*        
	,CITY               		CHAR(20)            					----���е���*        
	,REGION             		CHAR(20)            					----����*            
	,COUNTY             		CHAR(20)            					----����*            
	,DOOR_NO            		CHAR(60)            					----����*            
	,AREA_CODE          		CHAR(5)             					----����*            
	,PHONE_NO1          		CHAR(11)            					----�绰1*           
	,PHONE_NO2          		CHAR(10)            					----�绰2*           
	,POST_CODE          		CHAR(6)             					----��������*        
	,INDUSTRY_TYPE      		CHAR(4)             					----��ҵ���� 							BASS_STD1_0113       
	,EMPLOYEE_CNT       		CHAR(8)             					----ְԱ            
	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----��ҵ��λ��*      
	,ECONOMIC_TYPE      		CHAR(3)             					----�������� 							BASS_STD_0002       
	,OPEN_YEAR          		CHAR(4)             					----��ҵ1           
	,OPEN_MONTH         		CHAR(2)             					----��ҵ2           
	,SHAREHOLDER        		CHAR(1)             					----�ع�  								BASS_STD_0005          
	,GROUP_TYPE         		CHAR(1)             					----�������� 							BASS_STD_0003       
	,MANAGE_STYLE       		CHAR(1)             					----��Ӫ��ʽ      				BASS_STD_0004  
	,OPERATE_REVENUE_CLASS	CHAR(2)           						----Ӫҵ������� 					BASS_STD_0006   
	,CAPITAL_CLASS      		CHAR(2)           						----�ʲ�����     					BASS_STD_0007   
	,INDUSTRY_CLASS_CODE		CHAR(2)    NOT NULL         	----��ҵ������� 					BASS_STD1_0043   
	,CUST_STATUS        		CHAR(2)             					----���ſͻ�״̬    
	,CUST_INFO_SRC_ID   		CHAR(1)             					----���ſͻ�������Դ
	,UPLOAD_TYPE_ID     		CHAR(1)    NOT NULL         	----�ϴ������ʶ    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
select length(REGION) , count(0) 
--,  count(distinct length(REGION) ) 
from bass1.G_I_77780_DAY_MID 
group by  length(REGION) 
order by 1 

select REGION , count(0) 
--,  count(distinct REGION ) 
from bass1.G_I_77780_DAY_MID 
group by  REGION 
order by 1 


select COUNTY , count(0) 
--,  count(distinct COUNTY ) 
from bass1.G_I_77780_DAY_MID 
group by  COUNTY 
order by 1 

select PHONE_NO1 , count(0) 
--,  count(distinct PHONE_NO1 ) 
from bass1.G_I_77780_DAY_MID 
group by  PHONE_NO1 
order by 1 
select PHONE_NO2 , count(0) 
--,  count(distinct PHONE_NO2 ) 
from bass1.G_I_77780_DAY_MID 
group by  PHONE_NO2 
order by 1 

--,  count(distinct POST_CODE ) 
from bass1.G_I_77780_DAY_MID 
group by  POST_CODE 
order by 1 


select INDUSTRY_TYPE , count(0) 
--,  count(distinct INDUSTRY_TYPE ) 
from bass1.G_I_77780_DAY_MID 
group by  INDUSTRY_TYPE 
order by 1 

select INDUSTRY_TYPE , count(0) 
--,  count(distinct INDUSTRY_TYPE ) 
from bass1.G_I_77780_DAY_MID 
where  length(INDUSTRY_TYPE)  = 6
order by 1 

select length(INDUSTRY_TYPE) , count(0) 
--,  count(distinct length(INDUSTRY_TYPE) ) 
from bass1.G_I_77780_DAY_MID 
group by  length(INDUSTRY_TYPE) 
order by 1 
--,  count(distinct length(ECONOMIC_TYPE) ) 
from bass1.G_I_77780_DAY_MID 
group by  length(trim(ECONOMIC_TYPE))
order by 1 
--,  count(distinct ECONOMIC_TYPE ) 
from bass1.G_I_77780_DAY_MID 
group by  ECONOMIC_TYPE 
order by 1 

--,  count(distinct ECONOMIC_TYPE ) 
from bass1.G_I_77780_DAY_MID 
group by  ECONOMIC_TYPE 
order by 1 

select OPEN_YEAR , count(0) 
--,  count(distinct OPEN_YEAR ) 
from bass1.G_I_77780_DAY_MID 
group by  OPEN_YEAR 
order by 1 

select length(OPEN_YEAR) , count(0) 
--,  count(distinct length(OPEN_YEAR) ) 
from bass1.G_I_77780_DAY_MID 
group by  length(OPEN_YEAR) 
order by 1 

select OPEN_YEAR , count(0) 
--,  count(distinct OPEN_YEAR ) 
from bass1.G_I_77780_DAY_MID 
group by  OPEN_YEAR 
order by 1 
select length(OPEN_MONTH) , count(0) 
--,  count(distinct length(OPEN_MONTH) ) 
from bass1.G_I_77780_DAY_MID 
group by  length(OPEN_MONTH) 
order by 1 

select open_MONTH , count(0) 
--,  count(distinct open_MONTH ) 
from bass1.G_I_77780_DAY_MID 
where  length(OPEN_MONTH)  = 1
order by 1 

--,  count(distinct SHAREHOLDER ) 
from bass1.G_I_77780_DAY_MID 
group by  SHAREHOLDER 
order by 1 

select MANAGE_STYLE , count(0) 
--,  count(distinct MANAGE_STYLE ) 
from bass1.G_I_77780_DAY_MID 
group by  MANAGE_STYLE 
order by 1 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
      ,case when ADDR_CODE is null or length(ADDR_CODE) <> 6 then '540102' else ADDR_CODE end ADDR_CODE
        ,CITY
        ,substr(REGION,1,20) REGION
        ,substr(COUNTY,1,20) COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,replace(PHONE_NO1,'-','') PHONE_NO1
        ,replace(PHONE_NO2,'-','') PHONE_NO2
        ,case when POST_CODE is null or length(POST_CODE) <> 6 then '850000' else POST_CODE end POST_CODE
        ,case when INDUSTRY_TYPE is null or length(INDUSTRY_TYPE) <> 4 then '9421' else INDUSTRY_TYPE end INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,case when ECONOMIC_TYPE is null or length(trim(ECONOMIC_TYPE)) <> 3 then '190' else trim(ECONOMIC_TYPE) end ECONOMIC_TYPE
        ,case when OPEN_YEAR is null then '2002' when  length(OPEN_YEAR) <> 4  then substr(OPEN_YEAR,1,4) else OPEN_YEAR end OPEN_YEAR
        ,case when length(OPEN_MONTH) =1 then '0'||trim(OPEN_MONTH) when length(OPEN_MONTH) <> 2 then '01' else OPEN_MONTH end OPEN_MONTH
        ,case when SHAREHOLDER not in ('1','2','3','4','5') then '9' else SHAREHOLDER end SHAREHOLDER
        ,GROUP_TYPE
        ,case when MANAGE_STYLE is null then '9' 
        	when length(MANAGE_STYLE) = 2 and  substr(MANAGE_STYLE,2,1) in ('1','2','3') then substr(MANAGE_STYLE,2,1)
        	when length(MANAGE_STYLE) = 1 and  MANAGE_STYLE in ('1','2','3') then MANAGE_STYLE 
      		else '9' end MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from bass1.G_I_77780_DAY_MID a

insert into bass1.G_I_77780_DAY
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
      ,case when ADDR_CODE is null or length(ADDR_CODE) <> 6 then '540102' else ADDR_CODE end ADDR_CODE
        ,CITY
        ,substr(REGION,1,20) REGION
        ,substr(COUNTY,1,20) COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,replace(PHONE_NO1,'-','') PHONE_NO1
        ,replace(PHONE_NO2,'-','') PHONE_NO2
        ,case when POST_CODE is null or length(POST_CODE) <> 6 then '850000' else POST_CODE end POST_CODE
        ,case when INDUSTRY_TYPE is null or length(INDUSTRY_TYPE) <> 4 then '9421' else INDUSTRY_TYPE end INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,case when ECONOMIC_TYPE is null or length(trim(ECONOMIC_TYPE)) <> 3 then '190' else trim(ECONOMIC_TYPE) end ECONOMIC_TYPE
        ,case when OPEN_YEAR is null then '2002' when  length(OPEN_YEAR) <> 4  then substr(OPEN_YEAR,1,4) else OPEN_YEAR end OPEN_YEAR
        ,case when length(OPEN_MONTH) =1 then '0'||trim(OPEN_MONTH) when length(OPEN_MONTH) <> 2 then '01' else OPEN_MONTH end OPEN_MONTH
        ,case when SHAREHOLDER is null then '9' 
        	when SHAREHOLDER not in ('1','2','3','4','5') then '9' else SHAREHOLDER end SHAREHOLDER
        ,case when GROUP_TYPE not in ('1','2') then '2' else GROUP_TYPE end GROUP_TYPE
        ,case when MANAGE_STYLE is null then '9' 
        	when length(MANAGE_STYLE) = 2 and  substr(MANAGE_STYLE,2,1) in ('1','2','3') then substr(MANAGE_STYLE,2,1)
        	when length(MANAGE_STYLE) = 1 and  MANAGE_STYLE in ('1','2','3') then MANAGE_STYLE 
      		else '9' end MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from bass1.G_I_77780_DAY_MID a

13908934887
--,  count(distinct SHAREHOLDER ) 
from bass1.v_G_I_77780_DAY_MID 
group by  SHAREHOLDER 
order by 1 

--,  count(distinct GROUP_TYPE ) 
from bass1.v_G_I_77780_DAY_MID 
group by  GROUP_TYPE 
order by 1 
insert into bass1.G_I_77780_DAY
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
      ,case when ADDR_CODE is null or length(ADDR_CODE) <> 6 then '540102' else ADDR_CODE end ADDR_CODE
        ,CITY
        ,substr(REGION,1,20) REGION
        ,substr(COUNTY,1,20) COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,replace(PHONE_NO1,'-','') PHONE_NO1
        ,substr(replace(PHONE_NO2,'-',''),1,10) PHONE_NO2
        ,case when POST_CODE is null or length(POST_CODE) <> 6 then '850000' else POST_CODE end POST_CODE
        ,case when INDUSTRY_TYPE is null or length(INDUSTRY_TYPE) <> 4 then '9421' else INDUSTRY_TYPE end INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,case when ECONOMIC_TYPE is null or length(trim(ECONOMIC_TYPE)) <> 3 then '190' else trim(ECONOMIC_TYPE) end ECONOMIC_TYPE
        ,case when OPEN_YEAR is null then '2002' when  length(OPEN_YEAR) <> 4  then substr(OPEN_YEAR,1,4) else OPEN_YEAR end OPEN_YEAR
        ,case when length(OPEN_MONTH) =1 then '0'||trim(OPEN_MONTH) when length(OPEN_MONTH) <> 2 then '01' else OPEN_MONTH end OPEN_MONTH
        ,case when SHAREHOLDER is null then '9' 
        	when SHAREHOLDER not in ('1','2','3','4','5') then '9' else SHAREHOLDER end SHAREHOLDER
        ,case when GROUP_TYPE not in ('1','2') then '2' else GROUP_TYPE end GROUP_TYPE
        ,case when MANAGE_STYLE is null then '9' 
        	when length(MANAGE_STYLE) = 2 and  substr(MANAGE_STYLE,2,1) in ('1','2','3') then substr(MANAGE_STYLE,2,1)
        	when length(MANAGE_STYLE) = 1 and  MANAGE_STYLE in ('1','2','3') then MANAGE_STYLE 
      		else '9' end MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from bass1.G_I_77780_DAY_MID a



--,  count(distinct UPLOAD_TYPE_ID ) 
from bass1.v_G_I_77780_DAY_MID 
group by  UPLOAD_TYPE_ID 
order by 1 

insert into bass1.G_I_77780_DAY
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
      ,case when ADDR_CODE is null or length(ADDR_CODE) <> 6 then '540102' else ADDR_CODE end ADDR_CODE
        ,CITY
        ,substr(REGION,1,20) REGION
        ,substr(COUNTY,1,20) COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,replace(PHONE_NO1,'-','') PHONE_NO1
        ,substr(replace(PHONE_NO2,'-',''),1,10) PHONE_NO2
        ,case when POST_CODE is null or length(POST_CODE) <> 6 then '850000' else POST_CODE end POST_CODE
        ,case when INDUSTRY_TYPE is null or length(INDUSTRY_TYPE) <> 4 then '9421' else INDUSTRY_TYPE end INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,case when ECONOMIC_TYPE is null or length(trim(ECONOMIC_TYPE)) <> 3 then '190' else trim(ECONOMIC_TYPE) end ECONOMIC_TYPE
        ,case when OPEN_YEAR is null then '2002' when  length(OPEN_YEAR) <> 4  then substr(OPEN_YEAR,1,4) else OPEN_YEAR end OPEN_YEAR
        ,case when length(OPEN_MONTH) =1 then '0'||trim(OPEN_MONTH) when length(OPEN_MONTH) <> 2 then '01' else OPEN_MONTH end OPEN_MONTH
        ,case when SHAREHOLDER is null then '9' 
        	when SHAREHOLDER not in ('1','2','3','4','5') then '9' else SHAREHOLDER end SHAREHOLDER
        ,case when GROUP_TYPE not in ('1','2') then '2' else GROUP_TYPE end GROUP_TYPE
        ,case when MANAGE_STYLE is null then '9' 
        	when length(MANAGE_STYLE) = 2 and  substr(MANAGE_STYLE,2,1) in ('1','2','3') then substr(MANAGE_STYLE,2,1)
        	when length(MANAGE_STYLE) = 1 and  MANAGE_STYLE in ('1','2','3') then MANAGE_STYLE 
      		else '9' end MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
      ,case when INDUSTRY_CLASS_CODE is null then '99' else  INDUSTRY_CLASS_CODE end INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
      ,case when UPLOAD_TYPE_ID is null then '3' else UPLOAD_TYPE_ID end UPLOAD_TYPE_ID
from bass1.G_I_77780_DAY_MID a



select INDUSTRY_CLASS_CODE , count(0) 
--,  count(distinct INDUSTRY_CLASS_CODE ) 
from bass1.v_G_I_77780_DAY_MID 
group by  INDUSTRY_CLASS_CODE 
order by 1 
select count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a')) 
from  bass1.G_I_77780_DAY


89301560000994      724901496	2
89302999434694      43320587X	2

  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
  
insert into bass1.G_I_77780_DAY_MID2
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
      ,case when ADDR_CODE is null or length(ADDR_CODE) <> 6 then '540102' else ADDR_CODE end ADDR_CODE
        ,CITY
        ,substr(REGION,1,20) REGION
        ,substr(COUNTY,1,20) COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,replace(PHONE_NO1,'-','') PHONE_NO1
        ,substr(replace(PHONE_NO2,'-',''),1,10) PHONE_NO2
        ,case when POST_CODE is null or length(POST_CODE) <> 6 then '850000' else POST_CODE end POST_CODE
        ,case when INDUSTRY_TYPE is null or length(INDUSTRY_TYPE) <> 4 then '9421' else INDUSTRY_TYPE end INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,case when ECONOMIC_TYPE is null or length(trim(ECONOMIC_TYPE)) <> 3 then '190' else trim(ECONOMIC_TYPE) end ECONOMIC_TYPE
        ,case when OPEN_YEAR is null then '2002' when  length(OPEN_YEAR) <> 4  then substr(OPEN_YEAR,1,4) else OPEN_YEAR end OPEN_YEAR
        ,case when length(OPEN_MONTH) =1 then '0'||trim(OPEN_MONTH) when length(OPEN_MONTH) <> 2 then '01' else OPEN_MONTH end OPEN_MONTH
        ,case when SHAREHOLDER is null then '9' 
        	when SHAREHOLDER not in ('1','2','3','4','5') then '9' else SHAREHOLDER end SHAREHOLDER
        ,case when GROUP_TYPE not in ('1','2') then '2' else GROUP_TYPE end GROUP_TYPE
        ,case when MANAGE_STYLE is null then '9' 
        	when length(MANAGE_STYLE) = 2 and  substr(MANAGE_STYLE,2,1) in ('1','2','3') then substr(MANAGE_STYLE,2,1)
        	when length(MANAGE_STYLE) = 1 and  MANAGE_STYLE in ('1','2','3') then MANAGE_STYLE 
      		else '9' end MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
      ,case when INDUSTRY_CLASS_CODE is null then '99'  else INDUSTRY_CLASS_CODE end INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
      ,case when UPLOAD_TYPE_ID is null then '3' else UPLOAD_TYPE_ID end UPLOAD_TYPE_ID
from bass1.G_I_77780_DAY_MID a
 
create table bass1.G_I_77780_DAY_MID3 like bass1.G_I_77780_DAY
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING
insert into bass1.G_I_77780_DAY_MID3
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from   (select a.* , row_number()over(PARTITION by ENTERPRISE_ID,id order by  UPLOAD_TYPE_ID asc ) rn 
		from  bass1.G_I_77780_DAY_MID2 a ) t where t.rn = 1 
		
--,  count(distinct UPLOAD_TYPE_ID ) 
from bass1.G_I_77780_DAY_MID3 
group by  UPLOAD_TYPE_ID 
order by 1 
 select count(0)
 from bass1.G_I_77780_DAY_MID3 a ,
  BASS1.grp_id_old_new_map_20110330  b where a.ENTERPRISE_ID = b.old_enterprise_id
  
 from bass1.G_I_77780_DAY_MID3 a ,
  BASS1.grp_id_old_new_map_20110330  b where a.ENTERPRISE_ID = b.old_enterprise_id
  
 from bass1.G_I_77780_DAY_MID3 a ,
  BASS1.grp_id_old_new_map_20110330  b where a.ENTERPRISE_ID = b.old_enterprise_id
 
 from bass1.G_I_77780_DAY_MID3 a ,
  BASS1.grp_id_old_new_map_20110330  b where a.ENTERPRISE_ID = b.old_enterprise_id
 and  a.ENTERPRISE_NAME<>b.ENTERPRISE_NAME
 from bass1.G_I_77780_DAY_MID3 a ,
  BASS1.grp_id_old_new_map_20110330  b where a.ENTERPRISE_ID = b.old_enterprise_id
 and  a.ENTERPRISE_NAME<>b.ENTERPRISE_NAME
 from bass1.G_I_77780_DAY_MID3 a ,
  BASS1.grp_id_old_new_map_20110330  b where a.ENTERPRISE_ID = b.old_enterprise_id
select count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a')) 
from  bass1.G_I_77780_DAY_MID3

 set a.ENTERPRISE_ID = (select b.new_enterprise_id from  BASS1.grp_id_old_new_map_20110330  b 
 				where   a.ENTERPRISE_ID = b.old_enterprise_id )
insert into bass1.G_I_77780_DAY_MID3
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from   (select a.* , row_number()over(PARTITION by ENTERPRISE_ID,id order by  UPLOAD_TYPE_ID asc ) rn 
				from  bass1.G_I_77780_DAY_MID2 a
			 ) t where t.rn = 1 
		
 bass1.G_I_77780_DAY_MID3 a
 where  a.ENTERPRISE_ID = (select b.new_enterprise_id from  BASS1.grp_id_old_new_map_20110330 b 
 				where   a.ENTERPRISE_ID = b.old_enterprise_id )
 				
delete from bass1.G_I_77780_DAY_MID3
insert into bass1.G_I_77780_DAY_MID3
select 
         TIME_ID
        ,NEW_ENTERPRISE_ID
        ,ID
        ,NEW_ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from   (select a.* 
		,VALUE(b.new_enterprise_id,a.ENTERPRISE_ID) NEW_ENTERPRISE_ID
		,VALUE(b.ENTERPRISE_NAME,a.ENTERPRISE_NAME) NEW_ENTERPRISE_NAME
		, row_number()over(PARTITION by VALUE(b.new_enterprise_id,a.ENTERPRISE_ID),id order by  UPLOAD_TYPE_ID asc ) rn 
				from  bass1.G_I_77780_DAY_MID2 a
				left join  BASS1.grp_id_old_new_map_20110330  b on a.ENTERPRISE_ID = b.old_enterprise_id
			 ) t where t.rn = 1 
		
from  bass1.G_I_77780_DAY_MID3

--,  count(distinct UPLOAD_TYPE_ID ) 
from bass1.G_I_77780_DAY_MID3 
group by  UPLOAD_TYPE_ID 
order by 1 

         TIME_ID
        ,NEW_ENTERPRISE_ID
        ,ID
        ,NEW_ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from   (select a.* 
		,VALUE(b.new_enterprise_id,a.ENTERPRISE_ID) NEW_ENTERPRISE_ID
		,VALUE(b.ENTERPRISE_NAME,a.ENTERPRISE_NAME) NEW_ENTERPRISE_NAME
		, row_number()over(PARTITION by VALUE(b.new_enterprise_id,a.ENTERPRISE_ID),id order by  UPLOAD_TYPE_ID asc ) rn 
				from  bass1.G_I_77780_DAY_MID2 a
				left join  BASS1.grp_id_old_new_map_20110330  b on a.ENTERPRISE_ID = b.old_enterprise_id
			 ) t 
		
				left join  BASS1.grp_id_old_new_map_20110330  b on a.ENTERPRISE_ID = b.old_enterprise_id                
from  bass1.G_I_77780_DAY_MID3

 
delete from bass1.G_I_77780_DAY_MID3
insert into bass1.G_I_77780_DAY_MID3
select 
         TIME_ID
        ,NEW_ENTERPRISE_ID
        ,ID
        ,NEW_ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from   (select a.* 
		,VALUE(b.new_enterprise_id,a.ENTERPRISE_ID) NEW_ENTERPRISE_ID
		,VALUE(b.ENTERPRISE_NAME,a.ENTERPRISE_NAME) NEW_ENTERPRISE_NAME
		, row_number()over(PARTITION by ENTERPRISE_ID,id order by  UPLOAD_TYPE_ID asc ) rn 
				from  bass1.G_I_77780_DAY_MID2 a
				left join  BASS1.grp_id_old_new_map_20110330  b on a.ENTERPRISE_ID = b.old_enterprise_id
			 ) t where t.rn = 1 
		
,'89401560000324'
,'89401560000324'
,'89402999933152'
,'89401560000324'
,'89403000381097'
)                
                            ,'89401560000324'
                            ,'89401560000324'
                            ,'89402999933152'
                            ,'89401560000324'
                            ,'89403000381097'
                            )                
89402999933152      724904515
89401560000324      710905728
89401560000324      DX093350X
89402999933152      741930176
89401560000324      DX0933489
89403000381097      DX0933251

  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING
select * from bass1.G_I_77780_DAY_MID3
where ENTERPRISE_ID in ('89402999933152'
,'89401560000324'
,'89401560000324'
,'89402999933152'
,'89401560000324'
,'89403000381097'
)        

select UPLOAD_TYPE_ID , count(0) 
--,  count(distinct UPLOAD_TYPE_ID ) 
from bass1.G_I_77780_DAY_MID3 
group by  UPLOAD_TYPE_ID 
order by 1 

2	243 +3
3	505

delete from  bass1.G_I_77780_DAY_MID3
where ENTERPRISE_ID in ('89402999933152'
,'89401560000324'
,'89401560000324'
,'89402999933152'
,'89401560000324'
,'89403000381097'
)    
		
where ENTERPRISE_ID in ('89402999933152'
,'89401560000324'
,'89401560000324'
,'89402999933152'
,'89401560000324'
,'89403000381097'
)    
insert into     bass1.G_I_77780_DAY_MID3
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
 from 
(
select a.*,row_number()over(partition by ENTERPRISE_ID,id ) rn 
from  bass1.G_I_77780_DAY_MID4 a )
t where rn = 1 

		        
from  bass1.G_I_77780_DAY_MID3


from 
(select distinct ORG_TYPE ORG_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ORG_TYPE  not in (select code from BASS1.dim_bass1_std_map where interface_code = '77780' and dim_table_id ='BASS_STD1_0001')

select *
from 
(select distinct ORG_TYPE ORG_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ORG_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0001')


select ORG_TYPE , count(0) 
--,  count(distinct ORG_TYPE ) 
from bass1.G_I_77780_DAY_MID3 
group by  ORG_TYPE 
order by 1 

update  bass1.G_I_77780_DAY_MID3
set ORG_TYPE = '51'
where ORG_TYPE = '5'


--,  count(distinct INDUSTRY_TYPE ) 
from bass1.G_I_77780_DAY_MID3 
group by  INDUSTRY_TYPE 
order by 1 

drop table BASS1.t_bass1_std_0113
CREATE TABLE BASS1.t_bass1_std_0113
 (
	 code    char(4)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (code) USING HASHING
from 
(select distinct trim(INDUSTRY_TYPE) INDUSTRY_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_TYPE  not in 
(select code from BASS1.t_bass1_std_0113 )
select EMPLOYEE_CNT , count(0) 
--,  count(distinct EMPLOYEE_CNT ) 
from bass1.G_I_77780_DAY_MID3 
group by  EMPLOYEE_CNT 
order by 1 

set EMPLOYEE_CNT = '0'
where EMPLOYEE_CNT is null 

from 
(select distinct trim(ECONOMIC_TYPE) ECONOMIC_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ECONOMIC_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0002')

101
150
170

select ECONOMIC_TYPE , count(0) 
--,  count(distinct EMPLOYEE_CNT ) 
from bass1.G_I_77780_DAY_MID3 
group by  ECONOMIC_TYPE 
order by 1 

set ECONOMIC_TYPE = '110'
where ECONOMIC_TYPE in 
('100'
,'101'
,'150'
,'170'
)

from 
(select distinct SHAREHOLDER SHAREHOLDER from   bass1.G_I_77780_DAY_MID3) a 
where SHAREHOLDER  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0005')

select count(0)
from 
(select distinct GROUP_TYPE GROUP_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where GROUP_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0003')


from 
(select distinct MANAGE_STYLE MANAGE_STYLE from   bass1.G_I_77780_DAY_MID3) a 
where MANAGE_STYLE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0004')

from 
(select distinct OPERATE_REVENUE_CLASS OPERATE_REVENUE_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where OPERATE_REVENUE_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0006')



update G_I_77780_DAY_MID3 
set OPERATE_REVENUE_CLASS = '0'||OPERATE_REVENUE_CLASS
where length(trim(OPERATE_REVENUE_CLASS)) = 1


update G_I_77780_DAY_MID3 
set OPERATE_REVENUE_CLASS = '10'
where OPERATE_REVENUE_CLASS is null 

from 
(select distinct OPERATE_REVENUE_CLASS OPERATE_REVENUE_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where OPERATE_REVENUE_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0006')


from 
(select distinct CAPITAL_CLASS CAPITAL_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where CAPITAL_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')




update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '0'||trim(CAPITAL_CLASS)
where length(trim(CAPITAL_CLASS)) = 1


update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '05'
where CAPITAL_CLASS is null 


set CAPITAL_CLASS = '01'
where CAPITAL_CLASS = '00'




update G_I_77780_DAY_MID3 
set INDUSTRY_CLASS_CODE = '0'||TRIM(INDUSTRY_CLASS_CODE)
where length(trim(INDUSTRY_CLASS_CODE)) = 1


update G_I_77780_DAY_MID3 
set CUST_STATUS = '36'
where  CUST_STATUS IS NULL 



--,  count(distinct CUST_INFO_SRC_ID ) 
from bass1.G_I_77780_DAY_MID3 
group by  CUST_INFO_SRC_ID 
order by 1 

update G_I_77780_DAY_MID3 
set CUST_INFO_SRC_ID = '1'
where  CUST_INFO_SRC_ID IS NULL 


from 
(select distinct trim(INDUSTRY_TYPE) INDUSTRY_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_TYPE  not in 
(select code from BASS1.t_bass1_std_0113 )
delete from BASS1.G_I_77780_DAY

select * from bass1.G_I_77780_DAY_MID3
drop table BASS1.G_I_77780_DAY
CREATE TABLE BASS1.G_I_77780_DAY
 (
	 TIME_ID            		INTEGER        NOT NULL       ----��������        
	,ENTERPRISE_ID      		CHAR(20)       					       ----���ſͻ���ʶ    
	,ID                 		CHAR(9)        								----ID*              
	,ENTERPRISE_NAME    		CHAR(60)       								----���ſͻ�����*    
	,ORG_TYPE           		CHAR(5)             					----�������� 							BASS_STD_0001       
	,ADDR_CODE          		CHAR(6)             					----��ַ����*        
	,CITY               		CHAR(20)            					----���е���*        
	,REGION             		CHAR(20)            					----����*            
	,COUNTY             		CHAR(20)            					----����*            
	,DOOR_NO            		CHAR(60)            					----����*            
	,AREA_CODE          		CHAR(5)             					----����*            
	,PHONE_NO1          		CHAR(11)            					----�绰1*           
	,PHONE_NO2          		CHAR(10)            					----�绰2*           
	,POST_CODE          		CHAR(6)             					----��������*        
	,INDUSTRY_TYPE      		CHAR(4)             					----��ҵ���� 							BASS_STD1_0113       
	,EMPLOYEE_CNT       		CHAR(8)             					----ְԱ            
	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----��ҵ��λ��*      
	,ECONOMIC_TYPE      		CHAR(3)             					----�������� 							BASS_STD_0002       
	,OPEN_YEAR          		CHAR(4)             					----��ҵ1           
	,OPEN_MONTH         		CHAR(2)             					----��ҵ2           
	,SHAREHOLDER        		CHAR(1)             					----�ع�  								BASS_STD_0005          
	,GROUP_TYPE         		CHAR(1)             					----�������� 							BASS_STD_0003       
	,MANAGE_STYLE       		CHAR(1)             					----��Ӫ��ʽ      				BASS_STD_0004  
	,OPERATE_REVENUE_CLASS	CHAR(2)           						----Ӫҵ������� 					BASS_STD_0006   
	,CAPITAL_CLASS      		CHAR(2)           						----�ʲ�����     					BASS_STD_0007   
	,INDUSTRY_CLASS_CODE		CHAR(2)    NOT NULL         	----��ҵ������� 					BASS_STD1_0043   
	,CUST_STATUS        		CHAR(2)             					----���ſͻ�״̬    
	,CUST_INFO_SRC_ID   		CHAR(1)             					----���ſͻ�������Դ
	,UPLOAD_TYPE_ID     		CHAR(1)    NOT NULL         	----�ϴ������ʶ    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

        ,DEAL_TIME
        ,CMD_TYPE
        ,CMD_LINE
        ,PRIORITY_VAL
        ,TIME_VALUE
        ,FUNCTION_DESC
        ,CC_GROUP_CODE
        ,MO_GROUP_CODE
        ,CC_FLAG
        ,APP_DIR


insert into app.g_unit_info values ('77780',0,'��Ҫ���ſͻ������û��嵥','bass1.g_i_77780_day',1,0,0)


update BASS1.G_I_77780_DAY
set ENTERPRISE_ID = ' '
where ENTERPRISE_ID is null; 


update BASS1.G_I_77780_DAY
set ID = ' '
where ID is null; 


update BASS1.G_I_77780_DAY
set ENTERPRISE_NAME = ' '
where ENTERPRISE_NAME is null; 


update BASS1.G_I_77780_DAY
set ORG_TYPE = ' '
where ORG_TYPE is null; 



update BASS1.G_I_77780_DAY
set CITY = ' '
where CITY is null; 


update BASS1.G_I_77780_DAY
set REGION = ' '
where REGION is null; 


update BASS1.G_I_77780_DAY
set COUNTY = ' '
where COUNTY is null; 


update BASS1.G_I_77780_DAY
set DOOR_NO = ' '
where DOOR_NO is null; 





update BASS1.G_I_77780_DAY
set AREA_CODE = ' '
where AREA_CODE is null; 


update BASS1.G_I_77780_DAY
set PHONE_NO1 = ' '
where  PHONE_NO1 is null; 


update BASS1.G_I_77780_DAY
set PHONE_NO2 = ' '
where PHONE_NO2 is null; 


update BASS1.G_I_77780_DAY
set POST_CODE = ' '
where POST_CODE is null; 



update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = ' '
where INDUSTRY_TYPE is null; 


update BASS1.G_I_77780_DAY
set EMPLOYEE_CNT = ' '
where EMPLOYEE_CNT is null; 


update BASS1.G_I_77780_DAY
set INDUSTRY_UNIT_CNT = ' '
where INDUSTRY_UNIT_CNT is null; 


update BASS1.G_I_77780_DAY
set ECONOMIC_TYPE = ' '
where ECONOMIC_TYPE is null; 






update BASS1.G_I_77780_DAY
set OPEN_YEAR = ' '
where OPEN_YEAR is null; 


update BASS1.G_I_77780_DAY
set OPEN_MONTH = ' '
where OPEN_MONTH  is null; 


update BASS1.G_I_77780_DAY
set SHAREHOLDER = ' '
where SHAREHOLDER is null; 


update BASS1.G_I_77780_DAY
set GROUP_TYPE = ' '
where GROUP_TYPE is null; 



update BASS1.G_I_77780_DAY
set MANAGE_STYLE = ' '
where MANAGE_STYLE is null; 


update BASS1.G_I_77780_DAY
set OPERATE_REVENUE_CLASS = ' '
where OPERATE_REVENUE_CLASS is null; 


update BASS1.G_I_77780_DAY
set CAPITAL_CLASS = ' '
where CAPITAL_CLASS is null; 


update BASS1.G_I_77780_DAY
set INDUSTRY_CLASS_CODE = ' '
where INDUSTRY_CLASS_CODE is null; 




update BASS1.G_I_77780_DAY
set CUST_STATUS = ' '
where CUST_STATUS is null; 


update BASS1.G_I_77780_DAY
set CUST_INFO_SRC_ID = ' '
where CUST_INFO_SRC_ID is null; 


update BASS1.G_I_77780_DAY
set UPLOAD_TYPE_ID = ' '
where UPLOAD_TYPE_ID is null; 



from   BASS1.G_I_77780_DAY

where time_id=20101231
and return_flag=0

from 
(select distinct CAPITAL_CLASS CAPITAL_CLASS from    BASS1.G_I_77780_DAY) a 
where CAPITAL_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')



,'89108911013693'
,'89103001215411'
,'89101560001610'
,'89100000003719'
)
3000
7214

CUST_STATUS
10
12
20
30
36

set INDUSTRY_TYPE = '7210'
where INDUSTRY_TYPE = '7214'

select CUST_STATUS , count(0) 
--,  count(distinct CUST_STATUS ) 
from BASS1.G_I_77780_DAY 
group by  CUST_STATUS 
order by 1 

12	7
20	842
30	72
36	29

 11-����
 12-δ����
20-����
30-��Ч����
 31-���Ʋ�
 32-���沢
 33-��ҵǨ�����
 34-�ѽ�ҵ
 35-���廧�����༯��
 36-����
set CUST_STATUS = '11'
where CUST_STATUS = '10'

set CUST_STATUS = '31'
where CUST_STATUS = '30'

 select count(0)
from 
(select distinct trim(INDUSTRY_TYPE) INDUSTRY_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_TYPE  not in 
(select code from BASS1.t_bass1_std_0113 )


update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '9010'
where INDUSTRY_TYPE = '9011'


update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '3010'
where INDUSTRY_TYPE = '3000'


update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '2040'
where INDUSTRY_TYPE = '2050'



where alarmtime >=  current timestamp - 1 days
--and flag = -1
and control_code not  like 'BASS1%'
order by alarmtime desc 




control_code in (select  control_code from   app.sch_control_runlog where flag= 1)
 where control_code = 'TR1_L_A98012'
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabschema = 'BASS1'

 
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
and a.tabschema = 'BASS1'

		(
			select product_no,imei from bass1.G_S_04002_DAY
			where time_id between 20110101 and 20110301
		) a
		group by product_no,imei
CREATE TABLE BASS2.DIM_TERM_TAC
 (ID             INTEGER,
  TAC_NUM        VARCHAR(15),
  TERM_ID        VARCHAR(10),
  TERM_MODEL     VARCHAR(50),
  TERMPROD_ID    VARCHAR(10),
  TERMPROD_NAME  VARCHAR(200),
  NET_TYPE       CHARACTER(1),
  TERM_TYPE      CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_DIM
  PARTITIONING KEY
   (ID
   ) USING HASHING
ALTER TABLE BASS2.DIM_TERM_TAC
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE


CREATE TABLE BASS2.DIM_TERM_TAC_0331
 (ID             INTEGER,
  TAC_NUM        VARCHAR(15),
  TERM_ID        VARCHAR(10),
  TERM_MODEL     VARCHAR(50),
  TERMPROD_ID    VARCHAR(10),
  TERMPROD_NAME  VARCHAR(200),
  NET_TYPE       CHARACTER(1),
  TERM_TYPE      CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_DIM
  PARTITIONING KEY
   (ID
   ) USING HASHING
ALTER TABLE BASS2.DIM_TERM_TAC_0331
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
  
select 
ID,
TAC_NUM,
TERM_ID,
TERM_MODEL,
TERMPROD_ID,
TERMPROD_NAME,
NET_TYPE,
TERM_TYPE from BASS2.DIM_TERM_TAC_20110331BAK
where net_type <>'2'

group by tac_nuM
having count(*)>1
group by tac_nuM
having count(*)>1

delete from BASS2.DIM_TERM_TAC 
insert into BASS2.DIM_TERM_TAC
select * from BASS2.DIM_TERM_TAC_0331

select 
ID,
TAC_NUM,
TERM_ID,
TERM_MODEL,
TERMPROD_ID,
TERMPROD_NAME,
NET_TYPE,
TERM_TYPE from BASS2.DIM_TERM_TAC_20110331BAK
where net_type <>'2'
		from bass1.G_S_04002_DAY
		where time_id between  20110101 and 20110331
		  and mns_type='1'
		group by product_no,imei
(
select product_no,imei from bass1.G_S_04002_DAY
where time_id between  20110101 and 20110331
  and mns_type='1'
) a
group by product_no,imei
		  from bass1.G_S_04018_DAY a
		  where time_id between $last_last_month_day and $this_month_last_day
                sum(bigint(flows))
                from bass1.g_s_04002_day_flows
          
                from bass1.G_S_04002_DAY a
                where time_id between 20101201 and 20110228
                group by product_no,imei
          
--,  count(distinct time_id ) 
from bass1.g_s_22204_month 
group by  time_id 
order by 1 
where alarmtime >=  current timestamp - 1 days
--and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc 
          
for db25.app.sch_control_map
for db25.app.sch_control_task
for db25.bass1.int_program_data
	        case 
	          when para_value='0' then char(current date - 1 days)
	          else para_value
	        end
	      FROM bass1.usys_int_control 
	      WHERE para_name='op_time'
 from  bass2.Dw_comp_cust_dt
 where comp_day_new_mark=1 
  and comp_brand_id in (9,10,11)
 
TEL_MOBILE_NEW_ADD_CNT,
UNION_MOBILE_NEW_ADD_CNT
from 
bass1.G_S_22073_DAY
WHERE time_id >= 20110324 

 select distinct user_id from  G_A_02062_DAY
  where time_id <20110301
 and STATUS_ID = '1'
 except
 select distinct a.user_id from  
(select * from session.int_check_user_status   ) a 
,(select distinct user_id from   G_A_02062_DAY where time_id <20110301
 and STATUS_ID = '1') b 
where   a.user_id = b.user_id 
        and usertype_id NOT IN ('2010','2020','2030','9000')
        and test_flag='0'





select * from   APP.G_rule_check

select * from   APP.G_UNIT_INFO


select * from 
bass1.g_rule_check where rule_code = 'R159_4'
and time_id = 20110322


select * from 
bass1.g_rule_check where rule_code = 'R161_15'
where time_id = 20110317

select * from   app.sch_control_task where lower(cmd_line) like '%net%hlr%'

select * from   app.sch_control_runlog where control_code like '%06031%'

select count(0) from   BASS2.DIM_TACNUM_DEVID 
select count(0) from   bass1.G_A_02059_DAY_down20110321

select * from   G_A_02055_DAY


select * from   BASS2.DIM_CONTROL_INFO

select * from   BASS2.DIM_PROPERTY_VALUE_RANGE

select * from   BASS2.DIM_PROPERTY_INFO

select * from   BASS2.DIM_DEVICE_PROFILE

select * from   BASS2.DIM_DEVICE_INFO

select * from   BASS2.DIM_CONTROL_INFO

select * from   BASS2.DIM_TACNUM_DEVID

select count(0) from    BASS2.DIM_TERM_TAC

select * from   BASS2.DIM_TERM_TAC 

for db25.bass1.G_S_04008_DAY
CREATE TABLE "BASS1   "."G_S_04008_DAY0331"  (
		  "TIME_ID" INTEGER NOT NULL , 
		  "PRODUCT_NO" CHAR(15) NOT NULL , 
		  "IMSI" CHAR(15) NOT NULL , 
		  "MSRN" CHAR(11) NOT NULL , 
		  "IMEI" CHAR(17) NOT NULL , 
		  "OPPOSITE_NO" CHAR(24) NOT NULL , 
		  "THIRD_NO" CHAR(24) NOT NULL , 
		  "CITY_ID" CHAR(6) NOT NULL , 
		  "ROAM_LOCN" CHAR(6) NOT NULL , 
		  "OPP_CITY_ID" CHAR(6) NOT NULL , 
		  "OPP_ROAM_LOCN" CHAR(6) NOT NULL , 
		  "ADVERSARY_ID" CHAR(6) NOT NULL , 
		  "ADVERSARY_NET_TYPE" CHAR(2) NOT NULL , 
		  "MSC_CODE" CHAR(11) NOT NULL , 
		  "CELL_ID" CHAR(10) NOT NULL , 
		  "LAC_ID" CHAR(10) NOT NULL , 
		  "OPP_CELL_ID" CHAR(10) NOT NULL , 
		  "OPP_LAC_ID" CHAR(10) NOT NULL , 
		  "OUTGO_TRNK" CHAR(15) NOT NULL , 
		  "INCOMING_TRNK" CHAR(15) NOT NULL , 
		  "START_DATE" CHAR(8) NOT NULL , 
		  "START_TIME" CHAR(6) NOT NULL , 
		  "CALL_DURATION" CHAR(6) NOT NULL , 
		  "BASE_BILL_DURATION" CHAR(6) NOT NULL , 
		  "TOLL_BILL_DURATION" CHAR(6) NOT NULL , 
		  "BILLING_ID" CHAR(1) NOT NULL , 
		  "BASE_CALL_FEE" CHAR(8) NOT NULL , 
		  "TOLL_CALL_FEE" CHAR(8) NOT NULL , 
		  "CALLFW_TOLL_FEE" CHAR(8) , 
		  "INFO_FEE" CHAR(8) NOT NULL , 
		  "FAV_BASE_CALL_FEE" CHAR(8) NOT NULL , 
		  "FAV_TOLL_CALL_FEE" CHAR(8) NOT NULL , 
		  "FAV_CALLFW_TOLL_FEE" CHAR(8) , 
		  "FAV_INFO_FEE" CHAR(8) NOT NULL , 
		  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
		  "TOLL_TYPE_ID" CHAR(3) NOT NULL , 
		  "SVCITEM_ID" CHAR(3) NOT NULL , 
		  "CALL_TYPE_ID" CHAR(2) NOT NULL , 
		  "SERVICE_CODE" CHAR(4) NOT NULL , 
		  "USER_TYPE" CHAR(1) NOT NULL , 
		  "FEE_TYPE" CHAR(1) NOT NULL , 
		  "END_CALL_TYPE" CHAR(1) NOT NULL , 
		  "MNS_TYPE" CHAR(1) , 
		  "VIDEO_TYPE" CHAR(1) )   
		 DISTRIBUTE BY HASH("TIME_ID",  
		 "PRODUCT_NO")   
		   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  

ALTER TABLE "BASS1   "."G_S_04008_DAY0331" APPEND ON

ALTER TABLE "BASS1   "."G_S_04008_DAY0331" LOCKSIZE TABLE

insert into  bass1.G_S_04008_DAY0331

rule_code in ('R107','R108')
  AND RULE_CODE IN ('R107','R108')
 IN "TBS_APP_BASS1"

ALTER TABLE "BASS1"."DUAL"
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

for db25.bass1.G_S_04002_DAY
BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl	int -s INT_CHECK_SAMPLE_TO_DAY.tcl	2	�۷���ָ��R108(ԭR139)�������ſ��˷�Χ	2011-04-01 4:32:33.235691	[NULL]	-1	[NULL]
BASS1_INT_CHECK_SAMPLE_TO_DAY.tcl	int -s INT_CHECK_SAMPLE_TO_DAY.tcl	2	�۷���ָ��R107(ԭR138)�������ſ��˷�Χ	2011-04-01 4:20:19.310685	[NULL]	-1	[NULL]


select * from   bass1.G_RULE_CHECK where 
rule_code in ('R107')


select time_id,count(0),count(distinct user_id) from    BASS1.g_user_lst
group by time_id

201104	25702	25702
201103	24776	24776
201102	24247	24247

db2 runstats on table BASS1.g_user_lst with distribution and detailed indexes all
select * from   syscat.tables where tabname = 'G_USER_LST'


select a.tabname,decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2) as used_mb


select time_id,count(0) 
from   G_S_05002_MONTH
group by time_id

select * from   app.sch_control_before where control_code like '%05002%'
where filename like '%_201103_%' and err_code='00'
 (
 	interface_code varchar(5)
 	,interface_name varchar(128)
 )
  DATA CAPTURE NONE
		 DISTRIBUTE BY HASH(interface_code)   
		   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY  
ALTER TABLE BASS1.DIM_NOT_NULL_INTERFACE
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
  
('01006'
,'01007'
,'02013'
,'02055'
,'02056'
,'02057'
,'02058'
,'02060'
,'02061'
,'02064'
,'06001'
,'04009'
,'04010'
,'04012'
,'21004'
,'21005'
,'21009'
,'21016')
  where interface_code 
  in
('06001'
,'02017'
,'06002'
,'22401'
,'22403')


set flag = 0  
 where control_code in 
(
'BASS1_G_I_06031_DAY.tcl'
,'BASS1_G_I_06032_DAY.tcl'
,'BASS1_EXP_G_I_06032_DAY'
,'BASS1_EXP_G_I_06031_DAY'
)
(
'BASS1_G_I_06031_DAY.tcl'
,'BASS1_G_I_06032_DAY.tcl'
,'BASS1_EXP_G_I_06032_DAY'
,'BASS1_EXP_G_I_06031_DAY'
)
BASS1_G_S_22301_DAY.tcl
BASS1_G_S_22302_DAY.tcl

BASS1_EXP_G_S_22301_DAY
BASS1_EXP_G_S_22302_DAY

BASS1_G_S_03017_MONTH.tcl
BASS1_G_S_22013_MONTH.tcl
BASS1_G_S_22035_MONTH.tcl
BASS1_G_S_22037_MONTH.tcl
BASS1_G_S_22303_MONTH.tcl
BASS1_G_S_22305_MONTH.tcl

100

where filename like '%20110401%' and err_code='00'
,'04002'
,'04003'
,'04006'
,'04007'
,'04015'
,'04016'
,'04018'
,'22038'
,'22073'
,'22102'
,'22104'
,'04008'
,'04009'
,'04010'
,'04011'
,'04012'
,'06001')
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE before_control_code = 'BASS2_Dw_acct_should_dtl_today_ds.tcl'
           UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.before_control_code = n.control_code)
SELECT distinct c.control_code FROM n,app.sch_control_task c
where n.control_code = c.control_code
and c.deal_time = 1
and c.control_code not like 'OLAP_%'
where alarmtime >=  timestamp('20110402'||'000000') 
--and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc 
	                    and rule_code in ('R161_1','R161_2','R161_3','R161_4','R161_5','R161_6','R161_7','R161_8','R161_9','R161_10','R161_11','R161_12','R161_13','R161_14','R161_15','R161_16','R161_17')
('BASS1_G_I_02018_MONTH.tcl'
,'BASS1_G_I_02019_MONTH.tcl'
,'BASS1_G_I_02020_MONTH.tcl'
,'BASS1_G_I_02021_MONTH.tcl'
,'BASS1_G_I_02049_MONTH.tcl'
,'BASS1_G_I_02053_MONTH.tcl'
,'BASS1_G_I_03001_MONTH.tcl'
,'BASS1_G_I_03002_MONTH.tcl'
,'BASS1_G_I_03003_MONTH.tcl'
,'BASS1_G_I_06011_MONTH.tcl'
,'BASS1_G_I_06012_MONTH.tcl'
,'BASS1_G_I_06029_MONTH.tcl'
,'BASS1_INT_CHECK_0204902004_MONTH.tcl'
)

where filename like '%20110401%' and err_code='00'                       



,round(sum(bigint(flowdown))*1.00/1024/1024/1024,2)
from   bass1.G_S_04003_DAY
where time_id 


select 
,count(distinct a.user_id) user_cnt
,round(sum(wlan_flow),2) wlan_flow
from 
( select user_id,sum(int(FEE_RECEIVABLE))  wlan_fee
from BASS1.G_S_03004_MONTH 
where time_id  = 201012 and BILL_CYC_ID = '201012'
and ACCT_ITEM_ID in ('0715','0716')
group by user_id 
 ) a 
 ,session.int_check_user_status b 
 ,(
 		select  product_no 
 		,sum(bigint(flowup)+bigint(flowdown))*1.00/1024/1024/1024 wlan_flow
        from   bass1.G_S_04003_DAY
		where time_id between  20101201 and 20101231
	) c 
 where a.user_id = b.user_id and b.product_no = c.product_no
group by  
( select user_id,sum(int(SHOULD_FEE))  wlan_fee
from BASS1.G_S_03005_MONTH 
where time_id  = 201012 and BILL_CYC_ID = '201012'
and ITEM_ID in ('0715','0716')
group by user_id 
 ) a 
 ,session.int_check_user_status b 
 		select  product_no 
 		,round(sum(bigint(flowup)+bigint(flowdown))*1.00/1024/1024/1024,2) wlan_flow
        from   bass1.G_S_04003_DAY
		where time_id between  20101201 and 20101231
	) c where b.product_no = c.product_no
( select user_id,sum(int(SHOULD_FEE))  wlan_fee
from BASS1.G_S_03004_MONTH 
where time_id  = 201012 and BILL_CYC_ID = '201012'
and ACCT_ITEM_ID in ('0715','0716')
group by user_id 
 ) a 
 ,session.int_check_user_status b 
where ITEM_ID in ('0715','0716')
,a.user_id,a.wlan_fee,c.product_no,wlan_flow,b.*
--,count(distinct a.user_id) user_cnt
--,sum(wlan_flow) wlan_flow
from 
( select user_id,sum(int(FEE_RECEIVABLE))  wlan_fee
from BASS1.G_S_03004_MONTH 
where time_id  = 201012 and BILL_CYC_ID = '201012'
and ACCT_ITEM_ID in ('0715','0716')
group by user_id 
 ) a 
 ,session.int_check_user_status b 
 ,(
 		select  product_no 
 		,sum(bigint(flowup)+bigint(flowdown))*1.00/1024/1024/1024,2 wlan_flow
        from   bass1.G_S_04003_DAY
		where time_id between  20101201 and 20101231
        group by product_no
	) c 
 where a.user_id = b.user_id and b.product_no = c.product_no
201104	13638902959    	13638902959    	14	931067      	+MAILMF             	0	20101209	20101201	0	1	00


where alarmtime >=  current timestamp - 1 days
--and flag = -1
--and control_code like 'BASS1%'
order by alarmtime desc 

201102	18688

select * from app.sch_control_alarm where control_code like 'BASS1_%02005%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%02014%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%02015%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%02016%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%02047%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%06021%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%06022%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%06023%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%22009%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%22101%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%22103%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%22105%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%22106%' 
select * from app.sch_control_alarm where control_code like 'BASS1_%06002%' 

CREATE TABLE BASS1.MON_ALL_INTERFACE
 (
  type						CHAR(1)
  ,INTERFACE_CODE  CHAR(5)
  ,INTERFACE_NAME  VARCHAR(100)
  ,COARSE_TYPE				  VARCHAR(30)
  ,UPLOAD_TIME					  VARCHAR(20)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (INTERFACE_CODE) USING HASHING
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
  
where time_id= 201103
and return_flag=1 ) b 
                (select * from 
                (
                select user_id,chg_vip_time,row_number()over(partition by user_id order by time_id desc) row_id from BASS1.G_I_02005_MONTH
                where time_id=201104
                ) k
                where k.row_id =1) a
                left outer join 
                (
                select * from
                (
                select user_id,create_date,row_number()over(partition by user_id order by time_id desc) row_id 
                from BASS1.G_A_02004_DAY
                where time_id<=20110431
                ) k
                where k.row_id=1) b
                on a.user_id=b.user_id
                where bigint(chg_vip_time)<bigint(create_date)
                with ur
89460000740915	2011-03-21 
                
89460000740915	2011-03-20 23:59:59.000000

89160000265019      	20100901	1	89160000265019      	20100917	1

89101110031954      
89101110038097      
 


201102	754626
201101	733898
201012	713602
201011	676340

                 from
                (
                 select user_id,usertype_id from
                 (
                  select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id
                  from bass1.G_A_02008_DAY
                  where time_id<=20110331
                 ) k
                where k.row_id=1 
                ) a
                inner join (select distinct user_id from G_A_02004_DAY
                where usertype_id<>'3'
                and time_id<=20110331               
                ) c
                 on a.user_id=c.user_id
                
                left outer join (select user_id,region_flag
                from
                (
                select user_id,region_flag,row_number()over(partition by user_id order by time_id desc) row_id 
                from bass1.G_A_02052_MONTH
                ) k
                where k.row_id=1) b
                 on a.user_id=b.user_id
                  and b.REGION_FLAG in('1','2','3')
                where  a.usertype_id not in ('2010','2020','2030','9000')
                with ur
where  before_control_code in (
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = 'ÿ��8��ǰ'
)
and control_code like '%CHECK%'
and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
and control_code
in
('BASS1_INT_CHECK_02006_MONTH.tcl'
,'BASS1_INT_CHECK_0205202008_MONTH.tcl'
,'BASS1_INT_CHECK_B67_MONTH.tcl'
,'BASS1_INT_CHECK_D9E234F2_651TO56_MONTH.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_L5_MONTH.tcl'
,'BASS1_INT_CHECK_R031_MONTH.tcl'
,'BASS1_INT_CHECK_R034_MONTH.tcl'
,'BASS1_INT_CHECK_R036_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
,'BASS1_INT_CHECK_R030_MONTH.tcl'
,'BASS1_INT_CHECK_R032_MONTH.tcl'
,'BASS1_INT_CHECK_R037toR039_MONTH.tcl'
,'BASS1_INT_CHECK_R040_MONTH.tcl'
,'BASS1_INT_CHECK_0200803005_MONTH.tcl'
,'BASS1_INT_CHECK_R008_MONTH.tcl'
,'BASS1_INT_CHECK_R009_MONTH.tcl'
,'BASS1_INT_CHECK_SAMPLE_MONTH.tcl'
,'BASS1_INT_CHECK_VOICE_MONTH.tcl'
)

12559	2
12590	42846
12596	1400

12559	1
12590	120052
12596	1089

12596	1760

set flag = -2 
where control_code in 
(
select control_code from app.sch_control_runlog 
where control_code in (
select distinct control_code from   app.sch_control_before 
where  before_control_code in (
    select b.CONTROL_CODE from    
    BASS1.MON_ALL_INTERFACE a
    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
    and a.TYPE = 'm'
    and b.control_code like '%MONTH%'
    and upload_time = 'ÿ��10��ǰ'
    )
    and control_code like 'BASS1%EXP%'
)
and flag = 0
and date(endtime) < current date
)
and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')  
BASS1_G_S_05002_MONTH.tcl
BASS1_EXP_G_S_05002_MONTH

BASS1_EXP_G_S_05001_MONTH	2011-04-07 10:12:04.572475	[NULL]	[NULL]	1

db2 "load from /dev/null of del terminate into  bass1.T_GS05001M"  
db2 "load from /dev/null of del terminate into  bass1.T_GS05002M"  


from   bass1.T_GS05002M 
group by time_id 

02	805	0	0	100650555	671003700
03	91	0	0	28266454	188443030
04	563	0	0	15880785	105871900
08	21	0	0	19579200	32632000
16	103	0	0	632300940	770946200

select SP_BUS_CODE,count(0),sum(bigint(PAY_FEE)),sum(bigint(PAY_BALANCE_FEE)),sum(bigint(STLMNT_FEE)),sum(bigint(INFO_FEE))
02	767	0	0	101497245	676648300
03	101	0	0	10459780	69731880
04	921	0	0	7151325	47675500
08	21	0	0	19620000	32700000
16	99	0	0	611069783	744881850


select SP_BUS_CODE,count(0),sum(bigint(PAY_FEE)),sum(bigint(PAY_BALANCE_FEE)),sum(bigint(STLMNT_FEE)),sum(bigint(INFO_FEE))
02	798	0	0	117195616	781304110
03	110	0	0	11710603	78070700
04	1133	0	0	10629885	70865900
08	21	0	0	20071800	33453000
16	104	0	0	591129135	720477760




where 
 control_code 
    select b.CONTROL_CODE from    
    BASS1.MON_ALL_INTERFACE a
    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
    and a.TYPE = 'm'
    and b.control_code like '%MONTH%'
    and upload_time = 'ÿ��15��ǰ'
    )
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = 'ÿ��15��ǰ'
)

--update check
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select control_code from app.sch_control_runlog 
where control_code in (
select distinct control_code from   app.sch_control_before 
where  before_control_code in (
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = 'ÿ��15��ǰ'
)
and control_code like '%CHECK%'
and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
)
and flag = 0
and date(endtime) < current date
)


--update exp
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select control_code from app.sch_control_runlog 
where control_code in (
select distinct control_code from   app.sch_control_before 
where  before_control_code in (
    select b.CONTROL_CODE from    
    BASS1.MON_ALL_INTERFACE a
    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
    and a.TYPE = 'm'
    and b.control_code like '%MONTH%'
    and upload_time = 'ÿ��15��ǰ'
    )
    and control_code like 'BASS1%EXP%'
)
and flag = 0
and date(endtime) < current date
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1

 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
BASS1_EXP_G_S_04006_DAY	BASS1_INT_CHECK_F1_TO_DAY.tcl
BASS1_EXP_G_S_21001_DAY	BASS1_INT_CHECK_C567_DAY.tcl
BASS1_EXP_G_S_21001_DAY	BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl
BASS1_EXP_G_S_03005_MONTH	BASS1_INT_CHECK_G047TO50_MONTH.tcl
BASS1_EXP_G_S_03005_MONTH	BASS1_INT_CHECK_L34_MONTH.tcl
BASS1_EXP_G_S_04002_DAY	BASS1_INT_CHECK_L0_TO_DAY.tcl
BASS1_EXP_G_S_04014_DAY	BASS1_INT_CHECK_L1K9_TO_DAY.tcl
BASS1_EXP_G_A_01004_DAY	BASS1_INT_CHECK_L2_TO_DAY.tcl
BASS1_EXP_G_A_02004_DAY	BASS1_INT_CHECK_33TO40_DAY.tcl
BASS1_EXP_G_A_02004_DAY	INT_CHECK_DATARULE_DAY.tcl
BASS1_EXP_G_S_21004_DAY	BASS1_INT_CHECK_C567_DAY.tcl
BASS1_EXP_G_A_02008_DAY	INT_CHECK_DATARULE_DAY.tcl
BASS1_EXP_G_A_02008_DAY	BASS1_INT_CHECK_33TO40_DAY.tcl
BASS1_EXP_G_S_03012_MONTH	BASS1_INT_CHECK_E5_MONTH.tcl
BASS1_EXP_G_S_03012_MONTH	BASS1_INT_CHECK_G047TO50_MONTH.tcl
BASS1_EXP_G_S_03012_MONTH	BASS1_INT_CHECK_L34_MONTH.tcl
BASS1_EXP_G_S_03004_MONTH	BASS1_INT_CHECK_E5_MONTH.tcl
BASS1_EXP_G_S_22038_DAY	BASS1_INT_CHECK_C567_DAY.tcl
BASS1_EXP_G_S_21009_DAY	BASS1_INT_CHECK_C567_DAY.tcl
BASS1_EXP_G_S_02047_MONTH	BASS1_INT_CHECK_98Z6_MONTH.tcl
BASS1_EXP_G_S_04004_DAY	BASS1_INT_CHECK_A0L694B5_DAY.tcl
BASS1_EXP_G_S_04005_DAY	BASS1_INT_CHECK_L1K9_TO_DAY.tcl
BASS1_EXP_G_S_04005_DAY	BASS1_INT_CHECK_A12E6_DAY.tcl
BASS1_EXP_G_S_21008_MONTH	BASS1_INT_CHECK_8895_MONTH.tcl
BASS1_EXP_G_S_21007_DAY	BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl

insert into app.sch_control_before values ('BASS1_EXP_G_S_21001_DAY','BASS1_INT_CHECK_C567_DAY.tcl')                
insert into app.sch_control_before values ('BASS1_EXP_G_S_21001_DAY','BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl')     
insert into app.sch_control_before values ('BASS1_EXP_G_S_03005_MONTH','BASS1_INT_CHECK_G047TO50_MONTH.tcl')        
insert into app.sch_control_before values ('BASS1_EXP_G_S_03005_MONTH','BASS1_INT_CHECK_L34_MONTH.tcl')             
insert into app.sch_control_before values ('BASS1_EXP_G_S_04002_DAY','BASS1_INT_CHECK_L0_TO_DAY.tcl')               
insert into app.sch_control_before values ('BASS1_EXP_G_S_04014_DAY','BASS1_INT_CHECK_L1K9_TO_DAY.tcl')             
insert into app.sch_control_before values ('BASS1_EXP_G_A_01004_DAY','BASS1_INT_CHECK_L2_TO_DAY.tcl')               
insert into app.sch_control_before values ('BASS1_EXP_G_A_02004_DAY','BASS1_INT_CHECK_33TO40_DAY.tcl')              
insert into app.sch_control_before values ('BASS1_EXP_G_A_02004_DAY','INT_CHECK_DATARULE_DAY.tcl')                  
insert into app.sch_control_before values ('BASS1_EXP_G_S_21004_DAY','BASS1_INT_CHECK_C567_DAY.tcl')                
insert into app.sch_control_before values ('BASS1_EXP_G_A_02008_DAY','INT_CHECK_DATARULE_DAY.tcl')                  
insert into app.sch_control_before values ('BASS1_EXP_G_A_02008_DAY','BASS1_INT_CHECK_33TO40_DAY.tcl')              
insert into app.sch_control_before values ('BASS1_EXP_G_S_03012_MONTH','BASS1_INT_CHECK_E5_MONTH.tcl')              
insert into app.sch_control_before values ('BASS1_EXP_G_S_03012_MONTH','BASS1_INT_CHECK_G047TO50_MONTH.tcl')        
insert into app.sch_control_before values ('BASS1_EXP_G_S_03012_MONTH','BASS1_INT_CHECK_L34_MONTH.tcl')             
insert into app.sch_control_before values ('BASS1_EXP_G_S_03004_MONTH','BASS1_INT_CHECK_E5_MONTH.tcl')              
insert into app.sch_control_before values ('BASS1_EXP_G_S_22038_DAY','BASS1_INT_CHECK_C567_DAY.tcl')                
insert into app.sch_control_before values ('BASS1_EXP_G_S_21009_DAY','BASS1_INT_CHECK_C567_DAY.tcl')                
insert into app.sch_control_before values ('BASS1_EXP_G_S_02047_MONTH','BASS1_INT_CHECK_98Z6_MONTH.tcl')            
insert into app.sch_control_before values ('BASS1_EXP_G_S_04004_DAY','BASS1_INT_CHECK_A0L694B5_DAY.tcl')            
insert into app.sch_control_before values ('BASS1_EXP_G_S_04005_DAY','BASS1_INT_CHECK_L1K9_TO_DAY.tcl')             
insert into app.sch_control_before values ('BASS1_EXP_G_S_04005_DAY','BASS1_INT_CHECK_A12E6_DAY.tcl')               
insert into app.sch_control_before values ('BASS1_EXP_G_S_21008_MONTH','BASS1_INT_CHECK_8895_MONTH.tcl')            
insert into app.sch_control_before values ('BASS1_EXP_G_S_21007_DAY','BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl')     

 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
BASS1_EXP_G_A_01004_DAY
BASS1_EXP_G_A_02004_DAY
BASS1_EXP_G_A_02008_DAY
BASS1_EXP_G_I_03007_MONTH
BASS1_EXP_G_S_02047_MONTH
BASS1_EXP_G_S_03004_MONTH
BASS1_EXP_G_S_03005_MONTH
BASS1_EXP_G_S_03012_MONTH
BASS1_EXP_G_S_04002_DAY
BASS1_EXP_G_S_04004_DAY
BASS1_EXP_G_S_04005_DAY
BASS1_EXP_G_S_04006_DAY
BASS1_EXP_G_S_04014_DAY
BASS1_EXP_G_S_21001_DAY
BASS1_EXP_G_S_21004_DAY
BASS1_EXP_G_S_21007_DAY
BASS1_EXP_G_S_21008_MONTH
BASS1_EXP_G_S_21009_DAY
BASS1_EXP_G_S_22038_DAY

('BASS1_EXP_G_A_01004_DAY    '
,'BASS1_EXP_G_A_02004_DAY    '
,'BASS1_EXP_G_A_02008_DAY    '
,'BASS1_EXP_G_I_03007_MONTH  '
,'BASS1_EXP_G_S_02047_MONTH  '
,'BASS1_EXP_G_S_03004_MONTH  '
,'BASS1_EXP_G_S_03005_MONTH  '
,'BASS1_EXP_G_S_03012_MONTH  '
,'BASS1_EXP_G_S_04002_DAY    '
,'BASS1_EXP_G_S_04004_DAY    '
,'BASS1_EXP_G_S_04005_DAY    '
,'BASS1_EXP_G_S_04006_DAY    '
,'BASS1_EXP_G_S_04014_DAY    '
,'BASS1_EXP_G_S_21001_DAY    '
,'BASS1_EXP_G_S_21004_DAY    '
,'BASS1_EXP_G_S_21007_DAY    '
,'BASS1_EXP_G_S_21008_MONTH  '
,'BASS1_EXP_G_S_21009_DAY    '
,'BASS1_EXP_G_S_22038_DAY')
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)))
select distinct control_code 
where control_code                                          
in                                                          
('BASS1_EXP_G_A_01004_DAY    '                             
,'BASS1_EXP_G_A_02004_DAY    '                             
,'BASS1_EXP_G_A_02008_DAY    '                             
,'BASS1_EXP_G_I_03007_MONTH  '                             
,'BASS1_EXP_G_S_02047_MONTH  '                             
,'BASS1_EXP_G_S_03004_MONTH  '                             
,'BASS1_EXP_G_S_03005_MONTH  '                             
,'BASS1_EXP_G_S_03012_MONTH  '                             
,'BASS1_EXP_G_S_04002_DAY    '                             
,'BASS1_EXP_G_S_04004_DAY    '                             
,'BASS1_EXP_G_S_04005_DAY    '                             
,'BASS1_EXP_G_S_04006_DAY    '                             
,'BASS1_EXP_G_S_04014_DAY    '                             
,'BASS1_EXP_G_S_21001_DAY    '                             
,'BASS1_EXP_G_S_21004_DAY    '                             
,'BASS1_EXP_G_S_21007_DAY    '                             
,'BASS1_EXP_G_S_21008_MONTH  '                             
,'BASS1_EXP_G_S_21009_DAY    '                             
,'BASS1_EXP_G_S_22038_DAY')                                
and 
before_control_code not in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)

select distinct control_code from    app.sch_control_before
where control_code                                          
in                                                          
('BASS1_EXP_G_A_01004_DAY    '                             
,'BASS1_EXP_G_A_02004_DAY    '                             
,'BASS1_EXP_G_A_02008_DAY    '                             
,'BASS1_EXP_G_I_03007_MONTH  '                             
,'BASS1_EXP_G_S_02047_MONTH  '                             
,'BASS1_EXP_G_S_03004_MONTH  '                             
,'BASS1_EXP_G_S_03005_MONTH  '                             
,'BASS1_EXP_G_S_03012_MONTH  '                             
,'BASS1_EXP_G_S_04002_DAY    '                             
,'BASS1_EXP_G_S_04004_DAY    '                             
,'BASS1_EXP_G_S_04005_DAY    '                             
,'BASS1_EXP_G_S_04006_DAY    '                             
,'BASS1_EXP_G_S_04014_DAY    '                             
,'BASS1_EXP_G_S_21001_DAY    '                             
,'BASS1_EXP_G_S_21004_DAY    '                             
,'BASS1_EXP_G_S_21007_DAY    '                             
,'BASS1_EXP_G_S_21008_MONTH  '                             
,'BASS1_EXP_G_S_21009_DAY    '                             
,'BASS1_EXP_G_S_22038_DAY')       
except
select distinct control_code 
from    app.sch_control_before
where control_code                                          
in                                                          
('BASS1_EXP_G_A_01004_DAY    '                             
,'BASS1_EXP_G_A_02004_DAY    '                             
,'BASS1_EXP_G_A_02008_DAY    '                             
,'BASS1_EXP_G_I_03007_MONTH  '                             
,'BASS1_EXP_G_S_02047_MONTH  '                             
,'BASS1_EXP_G_S_03004_MONTH  '                             
,'BASS1_EXP_G_S_03005_MONTH  '                             
,'BASS1_EXP_G_S_03012_MONTH  '                             
,'BASS1_EXP_G_S_04002_DAY    '                             
,'BASS1_EXP_G_S_04004_DAY    '                             
,'BASS1_EXP_G_S_04005_DAY    '                             
,'BASS1_EXP_G_S_04006_DAY    '                             
,'BASS1_EXP_G_S_04014_DAY    '                             
,'BASS1_EXP_G_S_21001_DAY    '                             
,'BASS1_EXP_G_S_21004_DAY    '                             
,'BASS1_EXP_G_S_21007_DAY    '                             
,'BASS1_EXP_G_S_21008_MONTH  '                             
,'BASS1_EXP_G_S_21009_DAY    '                             
,'BASS1_EXP_G_S_22038_DAY')                                
and 
before_control_code not in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
select * from app.sch_control_before where control_code in 
(
'BASS1_EXP_G_A_01004_DAY'
,'BASS1_EXP_G_S_04005_DAY'
,'BASS1_EXP_G_S_04006_DAY'
,'BASS1_EXP_G_S_22038_DAY')
(
'BASS1_EXP_G_A_01004_DAY'
,'BASS1_EXP_G_S_04005_DAY'
,'BASS1_EXP_G_S_04006_DAY'
,'BASS1_EXP_G_S_22038_DAY')
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
(cola varchar(10),colb varchar(10))
in tbs_3h
06022
06023
22061
22062
22063
22064

AND INTERFACE_CODE in 									
('06021'
,'06022'
,'06023'
,'22061'
,'22062'
,'22063'
,'22064')
--,  count(distinct CHANNEL_TYPE ) 
from BASS1.G_I_06021_MONTH 
group by  time_id 
order by 1 
select CHANNEL_TYPE , count(0) 
--,  count(distinct CHANNEL_TYPE ) 
from BASS1.G_I_06021_MONTH 
group by  CHANNEL_TYPE 
order by 1 

--,  count(distinct SELF_CHANNEL_ID ) 
from BASS1.G_I_06021_MONTH 
group by  SELF_CHANNEL_ID 
order by 1 

select POSITION , count(0) 
--,  count(distinct POSITION ) 
from BASS1.G_I_06021_MONTH 
group by  POSITION 
order by 1
select CHANNEL_B_TYPE , count(0) 
--,  count(distinct CHANNEL_B_TYPE ) 
from BASS1.G_I_06021_MONTH 
group by  CHANNEL_B_TYPE 
order by 1 

--,  count(distinct CHANNEL_STATUS ) 
from BASS1.G_I_06021_MONTH 
group by  CHANNEL_STATUS 
order by 1 
1	6466
2	29
3	740

2����ͣӪҵ/Ԥע������Ӫ����ί�о�Ӫ����24Сʱ����Ӫҵ��Ϊ��ͣӪҵ������������ΪԤע����
3���ѹص�/ע������Ӫ����ί�о�Ӫ����24Сʱ����Ӫҵ��Ϊ�ѹص꣬����������Ϊע����

from BASS1.G_I_06021_MONTH 
group by   channel_type
order by 1 ,2

from BASS1.G_I_06021_MONTH 
group by   channel_type
order by 1 ,2
from BASS1.G_I_06021_MONTH 
group by   CHANNEL_B_TYPE
order by 1 ,2
from BASS1.G_I_06021_MONTH 
group by   CHANNEL_STAR
order by 1 ,2
646	578	2078	1784	1240

1	646
2	578
3	2078
4	1784
5	572
6	668
7	7

where channel_id not in 
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102 )
  and time_id =201102

(select distinct channel_id from bass1.g_i_06022_month where time_id =201102 )
where channel_id not in
(select distinct channel_id from bass1.g_i_06022_month where time_id =201102)
  and time_id =201102
  and channel_type <>'3'
  and channel_status='1'
where channel_id not in
(select distinct channel_id from bass1.g_i_06023_month where time_id =201102)
  and time_id =201102
  and channel_status='1'
where channel_id not in 
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102 )
  and time_id =201102
--,  count(distinct PROPERTY_SRC_TYPE ) 
from BASS1.G_I_06022_MONTH 
group by  OWNER_TYPE 
order by 1 


(select distinct channel_id from bass1.g_i_06021_month where time_id =201102 and channel_type = '3' )
  and time_id =201102
where channel_id not in
(select distinct channel_id from bass1.g_s_22061_month where time_id =201102)
  and time_id =201102
  and channel_type<>'3'
  and channel_status='1'



select count(*) from bass1.g_s_22062_month
where channel_id not in 
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102 )
  and time_id =201102
where channel_id not in
(select distinct channel_id from bass1.g_s_22062_month where time_id =201102)
  and time_id =201102
  and channel_status='1'
5554
where channel_id not in
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102 and channel_type<>'1')
  and time_id =201102
where channel_id in
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102 and channel_type='1')
  and time_id =201102

where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month where time_id =201102)
  and time_id =201102
  and channel_type in ('2','3')
  and channel_status='1'

 (
MOBILE_NUM  VARCHAR(11)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (MOBILE_NUM) USING HASHING
   
  ALTER TABLE BASS1.mon_user_mobile
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
						from \
						( \
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn \
						from APP.G_FILE_REPORT a \
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') \
						and err_code='00' \
						and substr(filename,18,5) \
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE \
								   where upload_time = 'ÿ��11��ǰ' \
								) \
						) t where rn = 1
   
   select  'xxxxx',count(0)
						from 
						( 
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn 
						from APP.G_FILE_REPORT a 
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') 
						and err_code='00' 
						and substr(filename,18,5) 
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE 
								   where upload_time = 'ÿ��11��ǰ' 
								) 
						) t where rn = 1                        
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month where time_id =201102)
  and time_id =201102
  and channel_type in ('2','3')
  and channel_status='1'
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month where time_id =201102)
  and time_id =201102
  and channel_type in ('2','3')
  and channel_status='1'
select * from bass2.stat_channel_reward_0002
where channel_name like '%����%'
where create_date between '20110201' and '20110231'
and test_flag='0'
where create_date between '20110201' and '20110231'
and test_flag='0'
(
select distinct channel_id from session.int_check_user_status
where create_date between '20110201' and '20110231'
and test_flag='0'
) aa
where aa.channel_id not in
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102)


--,  count(distinct CHANNEL_STATUS ) 
from BASS1.G_I_06021_MONTH 
where time_id = 201102
group by  CHANNEL_STATUS 
order by 1 "

where time_id =201102
group by channel_status
where time_id =201102
  and channel_status='1'
  and channel_b_type in ('1','2','3')
c.channel_type_id id3,c.type_name ��������
from 
(select * from bass2.ods_channel_type_20110209 a where parent_type_id=-1) a ,
(select * from bass2.ods_channel_type_20110209 
where parent_type_id in
  (select channel_type_id from bass2.ods_channel_type_20110209 a where parent_type_id=-1 )) b,
(select * from bass2.ods_channel_type_20110209 
where parent_type_id in
  (select channel_type_id from bass2.ods_channel_type_20110209 a 
    where parent_type_id in( select channel_type_id from bass2.ods_channel_type_20110209 a where parent_type_id =-1))) c                  
where  b.parent_type_id=a.channel_type_id
and  c.parent_type_id=b.channel_type_id
order by id1,id2,id3
(
select distinct channel_star from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_type<>'3'
) aa
where channel_star <>''
where time_id =201102
  and channel_status='1'
(
select distinct channel_star from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_type='3'
) aa
where channel_star=''
where time_id =201102
  and channel_status='1'
  and channel_type='1'
where time_id =201102
  and channel_status='1'
  and (longitude='0' or longitude='' or latitude='0' or latitude='')
     sum(bigint(new_users)),
    sum(bigint(hand_cnt)),
    sum(bigint(card_sale_cnt)),
    sum(bigint(accept_cnt)),
    --,sum(bigint(imp_accept_cnt))
    sum(bigint(term_sale_cnt)),
    --,sum(bigint(other_sale_cnt))
    sum(bigint(accept_bas_cnt))
    --,sum(bigint(query_bas_cnt))
from g_s_22062_month
where time_id =201102
group by accept_type
     sum(bigint(new_users)),
    sum(bigint(hand_cnt)),
    sum(bigint(card_sale_cnt)),
    sum(bigint(accept_cnt)),
    --,sum(bigint(imp_accept_cnt))
    sum(bigint(term_sale_cnt)),
    --,sum(bigint(other_sale_cnt))
    sum(bigint(accept_bas_cnt))
    --,sum(bigint(query_bas_cnt))
from g_s_22062_month
where time_id =201104

  from (
select accept_type,CHANNEL_STATUS,
     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --+sum(bigint(imp_accept_cnt))
    sum(bigint(term_sale_cnt))+
    --+sum(bigint(other_sale_cnt))
    sum(bigint(accept_bas_cnt)) cnt
    --+sum(bigint(query_bas_cnt)) 
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201102
  and b.time_id =201102
group by accept_type,CHANNEL_STATUS
) t group by CHANNEL_STATUS

  from (
select accept_type,CHANNEL_STATUS,
     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --+sum(bigint(imp_accept_cnt))
    sum(bigint(term_sale_cnt))+
    --+sum(bigint(other_sale_cnt))
    sum(bigint(accept_bas_cnt)) cnt
    --+sum(bigint(query_bas_cnt)) 
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201102
  and b.time_id =201102
group by accept_type,CHANNEL_STATUS
) t group by accept_type

     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --+sum(bigint(imp_accept_cnt))
    sum(bigint(term_sale_cnt))+
    --+sum(bigint(other_sale_cnt))
    sum(bigint(accept_bas_cnt)) cnt
    --+sum(bigint(query_bas_cnt)) 
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201102
  and b.time_id =201102
(
select sum(bigint(card_sale_cnt)) card_sale_cnt
from g_s_22062_month
where time_id =201102
) aa
where card_sale_cnt<=0
  from (
select accept_type,CHANNEL_STATUS,
     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --sum(bigint(imp_accept_cnt))+
    sum(bigint(term_sale_cnt))+
    --sum(bigint(other_sale_cnt))+
    sum(bigint(accept_bas_cnt)) +
    sum(bigint(query_bas_cnt)) cnt
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201102
  and b.time_id =201102
group by accept_type,CHANNEL_STATUS
) t 
			
--1723732
     sum(bigint(new_users))*1.0000/1723732*100
from g_s_22062_month
where time_id =201102

sum(bigint(imp_accept_cnt))
from g_s_22062_month
where time_id =201102

sum(bigint(CNT))
from g_s_22064_month
where time_id =201102

  from (
select accept_type,CHANNEL_STATUS,
     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --sum(bigint(imp_accept_cnt))+
    sum(bigint(term_sale_cnt))+
    --sum(bigint(other_sale_cnt))+
    sum(bigint(accept_bas_cnt)) +
    sum(bigint(query_bas_cnt)) cnt
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201102
  and b.time_id =201102
group by accept_type,CHANNEL_STATUS
) t 
order by 1,2
			

--,  count(distinct IMP_ACCEPTTYPE ) 
from g_s_22064_month 
group by  IMP_ACCEPTTYPE 
order by 1 

--,  count(distinct IMP_ACCEPTTYPE ) 
from g_s_22064_month 
group by  ACCEPT_TYPE 
order by 1 

from g_s_22064_month
where time_id =201102
  and imp_accepttype='23'
from g_s_22064_month
where time_id =201102
  and imp_accepttype='23'
where time_id =201102
  and busi_type in ('09','14','15')
  and sts='0'
  and VALID_DATE between '20110201' and '20110231'
  and EXPIRE_DATE>='20110201'
group by busi_type,apply_type

where time_id =201102
  and busi_type in ('14')
  and sts='0'
  and VALID_DATE between '20110201' and '20110231'
  and EXPIRE_DATE>='20110201'
group by busi_type,apply_type

where time_id =201102
  and busi_type in ('14')
  --and sts='0'
 and VALID_DATE like '201102%'
  --and EXPIRE_DATE>='20110201'
group by busi_type,apply_type,sts
where time_id =201102
  and busi_type in ('14')
sum(bigint(CNT))
from g_s_22064_month
where time_id =201102
sum(bigint(CNT))
from g_s_22064_month
where time_id =201102
         TIME_ID
        ,sum(bigint(ZZHI_CNT          ))
        +sum(bigint(TERM_CNT          ))
        +sum(bigint(CARD_CNT          ))
        +sum(bigint(PAYMENT_CNT       ))
        +sum(bigint(OTHER_CNT         ))
        /**,sum(bigint(E_PAY_AMOUNT      ))
        ,sum(bigint(O_TERM_AMOUNT     ))
        ,sum(bigint(ZHI_CUST_CNT      ))
        ,sum(bigint(E_CUST_CNT        ))
        ,sum(bigint(TX_CUST_CNT       ))**/
from g_s_22065_month
where TIME_ID = 201102
group by   TIME_ID
         TIME_ID
        ,sum(bigint(ZZHI_CNT          ))
        +sum(bigint(TERM_CNT          ))
        +sum(bigint(CARD_CNT          ))
        +sum(bigint(PAYMENT_CNT       ))
        +sum(bigint(OTHER_CNT         ))
from g_s_22065_month
where TIME_ID = 201102
group by   TIME_ID
select   accept_type,sum(cnt)
  from (
select accept_type,CHANNEL_STATUS,
     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --+sum(bigint(imp_accept_cnt))
    sum(bigint(term_sale_cnt))+
    --+sum(bigint(other_sale_cnt))
    sum(bigint(accept_bas_cnt))+
    sum(bigint(query_bas_cnt)) cnt
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201102
  and b.time_id =201102
group by accept_type,CHANNEL_STATUS
) t 
order by 1,2

declare global temporary table session.int_check_user_status
    (
   user_id        CHARACTER(15),
   product_no     CHARACTER(15),
   test_flag      CHARACTER(1),
   sim_code       CHARACTER(15),
   usertype_id    CHARACTER(4),
   create_date    CHARACTER(15),
   brand_id       CHARACTER(4),
   time_id        int,
   CHANNEL_ID     varchar(25)
    )                            
partitioning key           
 (
   user_id    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp


select * from   bass1.g_a_02004_day

--ץȡ�û��������
insert into session.int_check_user_status (
     user_id    
    ,product_no 
    ,test_flag  
    ,sim_code   
    ,usertype_id  
    ,create_date
    ,brand_id
    ,time_id
    ,CHANNEL_ID    )
select e.user_id
    ,e.product_no  
    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
    ,e.sim_code
    ,f.usertype_id  
    ,e.create_date  
    ,e.brand_id
    ,f.time_id  
    ,e.CHANNEL_ID     
from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id,CHANNEL_ID
                ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id<=20110231 ) e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=20110231 ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1
commit


select count(*) from 
(
select distinct channel_id from session.int_check_user_status
where create_date between '20110201' and '20110231'
and test_flag='0'
) aa
where aa.channel_id not in
(select distinct channel_id from bass1.g_i_06021_month where time_id =201102)
  
where create_date between '20110201' and '20110231'
and test_flag='0'

where create_date between '20110201' and '20110231'
and test_flag='0'
where TIME_ID = 201102
				       bass2.dw_product_ord_offer_dm_201102 b,
				       bass2.dim_prod_up_product_item d,
				       bass2.dim_pub_channel e,
				       bass2.dim_sys_org_role_type f,
				       bass2.dim_cfg_static_data g,
				       bass2.dw_channel_info_201102 h
				 where a.customer_order_id = b.customer_order_id
				   and b.offer_id = d.product_item_id
				   and a.org_id = e.channel_id
				   and a.channel_type = g.code_value
				   and g.code_type = '911000'
				   and e.channeltype_id = f.org_role_type_id
				   and a.org_id = h.channel_id
				   and h.channel_type_class in (90105,90102)
				   and a.channel_type in ('Q','L','I','e','D','B','6','4')
where time_id =201102
  and channel_status='1'
  and channel_type='4'

select count(channel_id) from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_b_type in ('4')

where time_id =201102
  and channel_status='1'
  and channel_b_type in ('5','6')
--6551

select count(channel_id) from bass1.g_i_06021_month
where time_id =201102
  and channel_status='1'
  and channel_type='3'
--6312

where time_id =201102
  and channel_status='1'
  and channel_b_type in ('1','2','3')

133
where time_id =201102
  and channel_status='1'
  and channel_type in ('1','2')
    ,case when opt_code in ('4464','4465','4864','4865') then '2' else '1' end
    ,sum(case when CERTIFICATE_TYPE='6' then 1 else 0 end )
    ,sum(case when CERTIFICATE_TYPE='6' then amount else 0 end )
from BASS2.dw_acct_payment_dm_201102
group by staff_org_id
        ,case when opt_code in ('4464','4465','4864','4865') then '2' else '1' end


          (  	CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT    )
			select channel_id
						,'1'
						,count(1)
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
			from bass2.dw_product_201104
			where month_new_mark = 1
			group by Channel_ID
where month_new_mark = 1
and test_flag='0'
where month_new_mark = 1
and test_flag='0'
and test_flag='0'
where month_new_mark = 1
where create_date between '20110201' and '20110231'
and test_flag='0'
and test_flag='0'
where month_new_mark = 1

	              (
	               enterprise_id       varchar(20),
                   numbers             int
	              )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (enterprise_id) USING HASHING

ALTER TABLE BASS1.g_a_01004_tmp1
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
	              (
	               enterprise_id       varchar(20),
                   manager_id          varchar(20)
	              )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (enterprise_id) USING HASHING

ALTER TABLE BASS1.g_a_01004_tmp2
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
	              (
	               enterprise_id       varchar(20)	              
	               )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (enterprise_id) USING HASHING

ALTER TABLE BASS1.g_a_01004_tmp10
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
 
ȥ��һ��:
 select count(0) from
   (select time_id,enterprise_id,cust_statu_typ_id from G_A_01004_DAY where time_id <= 20110411 ) a,
   (select enterprise_id,max(time_id) as time_id  from G_A_01004_DAY 
                                                 where time_id<=20110411  
                                              group by enterprise_id)b
where a.time_id=b.time_id and a.enterprise_id=b.enterprise_id and a.cust_statu_typ_id = '20'

����Ч��һ����

 select count(0) 
from 
(
select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
from bass1.G_A_01004_DAY 
where time_id <= 20110411 
) t where t.rn = 1 and  cust_statu_typ_id = '20'

from 
(
select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
from bass1.G_A_01004_DAY 
where time_id <= 20110411 
) t where t.rn = 1 and  cust_statu_typ_id = '20'
from 
(
select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
from bass1.G_A_01004_DAY 
where time_id <= 20110411 
) t where t.rn = 1 and  cust_statu_typ_id = '20'
from 
(
select time_id,enterprise_id,cust_statu_typ_id ,row_number()over(partition by enterprise_id order by time_id desc) rn 
from bass1.G_A_01004_DAY 
where time_id <= ${timestamp}   
) t where t.rn = 1 and  cust_statu_typ_id = '20'
                 (select time_id,enterprise_id,cust_statu_typ_id from G_A_01004_DAY where time_id <= 20110411) a,
                 (select enterprise_id,max(time_id) as time_id  from G_A_01004_DAY 
                                                                 where time_id<=20110411
															                                group by enterprise_id)b
                where a.time_id=b.time_id and a.enterprise_id=b.enterprise_id and a.cust_statu_typ_id = '20' 
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (CUST_ID
   ) USING HASHING     
  	     ENTERPRISE_ID
        ,GROUP_NAME
        ,PASSWORD
        ,GROUP_LEVEL
        ,GROUP_TYPE
        ,REGION_SPECIA
        ,GROUP_STATUS
        ,LEVEL_DEF_MODE
        ,REC_STATUS
        ,VOCATION
        ,VOCATION2
        ,VOCATION3
        ,GROUP_COUNTRY
        ,GROUP_PROVINCE
        ,GROUP_CITY
        ,REGION_TYPE
        ,REGION_DETAIL
        ,GROUP_ADDRESS
        ,GROUP_POSTCODE
        ,POST_PROVINCE
        ,POST_CITY
        ,POST_ADDRESS
        ,POST_POSTCODE
        ,PHONE_ID
        ,FAX_ID
        ,IDEN_ID
        ,IDEN_NBR
        ,OWNER_NAME
        ,TAX_ID
        ,NET_ADDRESS
        ,PAY_TYPE
        ,EMAIL
        ,CREATE_DATE
        ,DONE_DATE
        ,VALID_DATE
        ,EXPIRE_DATE
        ,OP_ID
        ,ORG_ID
        ,SO_NBR
        ,CUST_ID
        ,NOTES
        ,VPMN_ID
        ,EXT_FIELD1
        ,EXT_FIELD2
        ,EXT_FIELD3
        ,EXT_FIELD4
        ,EXT_FIELD5
        ,EXT_FIELD6
        ,EXT_FIELD7
        ,EXT_FIELD8
        ,EXT_FIELD9
        ,EXT_FIELD10
				from 
				(
				select a.*,row_number()over(partition by enterprise_id order by done_date desc) rn 
				from bass2.dwd_enterprise_msg_his_20110411 a
				) t 
				where t.rn = 1 
					and  t.rec_status = 0 
					and t.enterprise_id in (select enterprise_id from bass1.g_a_01004_tmp10) 
from bass1.g_a_01004_day 
group by  time_id 
order by 1 desc
           
20110410	11703
select time_id , count(0) 
--,  count(distinct time_id ) 
from bass1.g_a_01004_day_20110412bak 
group by  time_id 
order by 1 
20110410	11703

                                from BASS2.DW_ENTERPRISE_ACCOUNT_20110411 where REC_STATUS=1
				(
				select a.*,row_number()over(partition by enterprise_id order by done_date desc) rn 
				from bass2.dwd_enterprise_msg_his_20110411 a
				) t                                 
					and  t.rec_status = 0                    
				(
				select a.*,row_number()over(partition by enterprise_id order by done_date desc) rn 
				from bass2.dwd_enterprise_msg_his_20110411 a
				) t                                 
					and  t.rec_status = 0 
(select * from bass2.dwd_enterprise_msg_his_20110411)  a,
(select enterprise_id,max(done_date) as done_date from bass2.dwd_enterprise_msg_his_20110411  group by enterprise_id) b
where a.done_date = b.done_date and a.enterprise_id = b.enterprise_id and rec_status = 0 and  a.enterprise_id in (select * from bass1.g_a_01004_tmp10) 
)a 
CREATE TABLE BASS1.g_a_01004_tmp3
 (TIME_ID            INTEGER,
  ENTERPRISE_ID      CHARACTER(20),
  ENT_DEF_MODE       CHARACTER(1),
  PRT_GRP_CUST_ID    CHARACTER(20),
  ENTERPRISE_NAME    CHARACTER(60),
  OWNER_NAME         CHARACTER(20),
  NET_ADDRESS        CHARACTER(250),
  FAX_NO             CHARACTER(20),
  ENT_SCALE_ID       CHARACTER(1),
  MEMBER_NUMS        CHARACTER(8),
  ENT_REGION_TYPE    CHARACTER(2),
  ENT_INDUSTRY_ID    CHARACTER(2),
  GRP_AREA_SPEC_ID   CHARACTER(2),
  ENT_MANAGER_ID     CHARACTER(20),
  CMCC_ID            CHARACTER(5),
  CREATE_DATE        CHARACTER(8),
  LINKMAN_NAME       CHARACTER(20),
  TELEPHONE_NUM      CHARACTER(20),
  MOBILE_NUM         CHARACTER(15),
  LINKMAN_TITLE      CHARACTER(20),
  LINKMAN_FAX        CHARACTER(20),
  LINKMAN_MAIL       CHARACTER(50),
  LINKMAN_ADDR       CHARACTER(70),
  POST_CODE          CHARACTER(6),
  CUST_STATU_TYP_ID  CHARACTER(2),
  UNITE_PAY_FLAG     CHARACTER(1),
  IND_RES_SCHEMA     CHARACTER(100),
  CRT_CHNL_ID        CHARACTER(25),
  ENTER_TYPE_ID      CHARACTER(2)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,
    ENTERPRISE_ID
   ) USING HASHING

ALTER TABLE BASS1.g_a_01004_tmp3
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

                                      
CREATE TABLE BASS1.g_a_01004_tmp12
 (TIME_ID            INTEGER,
  ENTERPRISE_ID      CHARACTER(20),
  ENT_DEF_MODE       CHARACTER(1),
  PRT_GRP_CUST_ID    CHARACTER(20),
  ENTERPRISE_NAME    CHARACTER(60),
  OWNER_NAME         CHARACTER(20),
  NET_ADDRESS        CHARACTER(250),
  FAX_NO             CHARACTER(20),
  ENT_SCALE_ID       CHARACTER(1),
  MEMBER_NUMS        CHARACTER(8),
  ENT_REGION_TYPE    CHARACTER(2),
  ENT_INDUSTRY_ID    CHARACTER(2),
  GRP_AREA_SPEC_ID   CHARACTER(2),
  ENT_MANAGER_ID     CHARACTER(20),
  CMCC_ID            CHARACTER(5),
  CREATE_DATE        CHARACTER(8),
  LINKMAN_NAME       CHARACTER(20),
  TELEPHONE_NUM      CHARACTER(20),
  MOBILE_NUM         CHARACTER(15),
  LINKMAN_TITLE      CHARACTER(20),
  LINKMAN_FAX        CHARACTER(20),
  LINKMAN_MAIL       CHARACTER(50),
  LINKMAN_ADDR       CHARACTER(70),
  POST_CODE          CHARACTER(6),
  CUST_STATU_TYP_ID  CHARACTER(2),
  UNITE_PAY_FLAG     CHARACTER(1),
  IND_RES_SCHEMA     CHARACTER(100),
  CRT_CHNL_ID        CHARACTER(25),
  ENTER_TYPE_ID      CHARACTER(2)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,
    ENTERPRISE_ID
   ) USING HASHING

ALTER TABLE BASS1.g_a_01004_tmp12
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

  	     ENTERPRISE_ID
        ,GROUP_NAME
        ,PASSWORD
        ,GROUP_LEVEL
        ,GROUP_TYPE
        ,REGION_SPECIA
        ,GROUP_STATUS
        ,LEVEL_DEF_MODE
        ,REC_STATUS
        ,VOCATION
        ,VOCATION2
        ,VOCATION3
        ,GROUP_COUNTRY
        ,GROUP_PROVINCE
        ,GROUP_CITY
        ,REGION_TYPE
        ,REGION_DETAIL
        ,GROUP_ADDRESS
        ,GROUP_POSTCODE
        ,POST_PROVINCE
        ,POST_CITY
        ,POST_ADDRESS
        ,POST_POSTCODE
        ,PHONE_ID
        ,FAX_ID
        ,IDEN_ID
        ,IDEN_NBR
        ,OWNER_NAME
        ,TAX_ID
        ,NET_ADDRESS
        ,PAY_TYPE
        ,EMAIL
        ,CREATE_DATE
        ,DONE_DATE
        ,VALID_DATE
        ,EXPIRE_DATE
        ,OP_ID
        ,ORG_ID
        ,SO_NBR
        ,CUST_ID
        ,NOTES
        ,VPMN_ID
        ,EXT_FIELD1
        ,EXT_FIELD2
        ,EXT_FIELD3
        ,EXT_FIELD4
        ,EXT_FIELD5
        ,EXT_FIELD6
        ,EXT_FIELD7
        ,EXT_FIELD8
        ,EXT_FIELD9
        ,EXT_FIELD10
				from bass2.dwd_enterprise_msg_his_20110411 a,
        (select enterprise_id,max(done_date) as done_date from bass2.dwd_enterprise_msg_his_20110411  group by enterprise_id) b
        where a.done_date = b.done_date 
        			and a.enterprise_id = b.enterprise_id and rec_status = 0 
        			and  a.enterprise_id in (select t.enterprise_id from bass1.g_a_01004_tmp10 t) 
				with ur                                       
select 
  	     a.ENTERPRISE_ID
        ,a.GROUP_NAME
        ,a.PASSWORD
        ,a.GROUP_LEVEL
        ,a.GROUP_TYPE
        ,a.REGION_SPECIA
        ,a.GROUP_STATUS
        ,a.LEVEL_DEF_MODE
        ,a.REC_STATUS
        ,a.VOCATION
        ,a.VOCATION2
        ,a.VOCATION3
        ,a.GROUP_COUNTRY
        ,a.GROUP_PROVINCE
        ,a.GROUP_CITY
        ,a.REGION_TYPE
        ,a.REGION_DETAIL
        ,a.GROUP_ADDRESS
        ,a.GROUP_POSTCODE
        ,a.POST_PROVINCE
        ,a.POST_CITY
        ,a.POST_ADDRESS
        ,a.POST_POSTCODE
        ,a.PHONE_ID
        ,a.FAX_ID
        ,a.IDEN_ID
        ,a.IDEN_NBR
        ,a.OWNER_NAME
        ,a.TAX_ID
        ,a.NET_ADDRESS
        ,a.PAY_TYPE
        ,a.EMAIL
        ,a.CREATE_DATE
        ,a.DONE_DATE
        ,a.VALID_DATE
        ,a.EXPIRE_DATE
        ,a.OP_ID
        ,a.ORG_ID
        ,a.SO_NBR
        ,a.CUST_ID
        ,a.NOTES
        ,a.VPMN_ID
        ,a.EXT_FIELD1
        ,a.EXT_FIELD2
        ,a.EXT_FIELD3
        ,a.EXT_FIELD4
        ,a.EXT_FIELD5
        ,a.EXT_FIELD6
        ,a.EXT_FIELD7
        ,a.EXT_FIELD8
        ,a.EXT_FIELD9
        ,a.EXT_FIELD10
				from bass2.dwd_enterprise_msg_his_20110411 a,
        (select enterprise_id,max(done_date) as done_date from bass2.dwd_enterprise_msg_his_20110411  group by enterprise_id) b
        where a.done_date = b.done_date 
        			and a.enterprise_id = b.enterprise_id and rec_status = 0 
        			and  a.enterprise_id in (select t.enterprise_id from bass1.g_a_01004_tmp10 t) 
				with ur                  
CREATE TABLE BASS1.g_a_01004_tmp4
 (TIME_ID            INTEGER,
  ENTERPRISE_ID      CHARACTER(20),
  ENT_DEF_MODE       CHARACTER(1),
  PRT_GRP_CUST_ID    CHARACTER(20),
  ENTERPRISE_NAME    CHARACTER(60),
  OWNER_NAME         CHARACTER(20),
  NET_ADDRESS        CHARACTER(250),
  FAX_NO             CHARACTER(20),
  ENT_SCALE_ID       CHARACTER(1),
  MEMBER_NUMS        CHARACTER(8),
  ENT_REGION_TYPE    CHARACTER(2),
  ENT_INDUSTRY_ID    CHARACTER(2),
  GRP_AREA_SPEC_ID   CHARACTER(2),
  ENT_MANAGER_ID     CHARACTER(20),
  CMCC_ID            CHARACTER(5),
  CREATE_DATE        CHARACTER(8),
  LINKMAN_NAME       CHARACTER(20),
  TELEPHONE_NUM      CHARACTER(20),
  MOBILE_NUM         CHARACTER(15),
  LINKMAN_TITLE      CHARACTER(20),
  LINKMAN_FAX        CHARACTER(20),
  LINKMAN_MAIL       CHARACTER(50),
  LINKMAN_ADDR       CHARACTER(70),
  POST_CODE          CHARACTER(6),
  CUST_STATU_TYP_ID  CHARACTER(2),
  UNITE_PAY_FLAG     CHARACTER(1),
  IND_RES_SCHEMA     CHARACTER(100),
  CRT_CHNL_ID        CHARACTER(25),
  ENTER_TYPE_ID      CHARACTER(2)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,
    ENTERPRISE_ID
   ) USING HASHING

ALTER TABLE BASS1.g_a_01004_tmp4
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE                
          (  	CHANNEL_ID      		BIGINT   ,
              ACCEPT_TYPE         CHAR(1) ,
              NEW_USERS          bigint		,
						  HAND_CNT           bigint		,
						  HAND_FEE           bigint		,
						  CARD_SALE_CNT      bigint		,
						  CARD_SALE_FEE      bigint		,
						  ACCEPT_CNT         bigint		,
						  IMP_ACCEPT_CNT     bigint		,
						  TERM_SALE_CNT      bigint		,
						  OTHER_SALE_CNT     bigint		,
						  ACCEPT_BAS_CNT     bigint		,
						  QUERY_BAS_CNT      bigint		,
						  OFF_ACCEPT_CNT     bigint
           )
      PARTITIONING KEY (CHANNEL_ID,ACCEPT_TYPE) USING HASHING
      WITH REPLACE ON COMMIT PRESERVE ROWS NOT LOGGED IN TBS_USER_TEMP
					DATA CAPTURE NONE
					IN TBS_APP_BASS1
					INDEX IN TBS_INDEX
					PARTITIONING KEY (CHANNEL_ID,ACCEPT_TYPE) USING HASHING

where create_date between '20110201' and '20110231'
and test_flag='0'
and CHANNEL_STATE=1
--DW_CHANNEL_INFO�У���ODS_DIM_SYS_ORGANIZE�Ҳ�����,������Ӧ��organize_id��Ϣ
select a.channel_id,a.organize_id from BASS2.ODS_CHANNEL_INFO_20110411 a where not exists (select 1 from BASS2.ODS_DIM_SYS_ORGANIZE_20110411 b where a.organize_id = b.organize_id and b.STATE=1)
and CHANNEL_STATE=1

--��dw_product�У���DW_CHANNEL_INFOû�е�,(���¿���������ʶ����06021��,У��ʧ��)
select distinct a.channel_id from BASS2.dw_product_201103 a where not exists (select 1 from BASS2.ODS_CHANNEL_INFO_20110411 b where a.channel_id = b.channel_id)
and userstatus_id in (1,2,3,6,8) and test_mark=0
        
        
--��ODS_DIM_SYS_ORGANIZE�У���DW_CHANNEL_INFOû�е� ,������Ӧ��channel_id��Ϣ
select a.organize_id from BASS2.ODS_DIM_SYS_ORGANIZE_20110411 a where not exists (select 1 from BASS2.ODS_CHANNEL_INFO_20110411 b where a.organize_id = b.organize_id and b.CHANNEL_STATE=1)
and a.state=1

--��dw_product�У���ODS_DIM_SYS_ORGANIZEû�е�
select distinct a.channel_id from BASS2.dw_product_201103 a where not exists (select 1 from bass2.ODS_DIM_SYS_ORGANIZE_20110411 b where a.channel_id = b.organize_id)
and userstatus_id in (1,2,3,6,8) and test_mark=0

            
CREATE TABLE BASS1.dim_21003_ip_type
 (
 	ip_type_id varchar(4)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ip_type_id) USING HASHING
   
ALTER TABLE BASS1.dim_21003_ip_type
LOCKSIZE ROW
APPEND OFF
NOT VOLATILE
     
select count(0)
from (
select
                                        1
					,PRODUCT_NO
					,case when ip_type_id not in ('1000', '2000', '2100', '2101', '2102', '2199', '2200', '2201', 
                                        '2202', '2203', '2204', '2299', '3000', '3100', '3101', '3102', 
                                        '3103', '3104', '3105', '3106', '3107', '3108', '3109', '3110', 
                                        '3111', '3112', '3113', '3114', '3199', '3200', '4000', '4100', 
                                        '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', 
                                        '4109', '4110', '4111', '4199', '4200', '6000', '6100', '6101', 
                                        '6102', '6103', '6104', '6105', '6106', '6107', '6199', '6200', 
                                        '9000' ) 
                then '9000' else ip_type_id end
				FROM
					bass1.int_210012916_201103 a 
          WHERE op_time=20110331
				 GROUP BY
					PRODUCT_NO
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,ip_type_id
					,adversary_id
					,roam_type_id
					,call_type_id
) t             

select count(0)
from (
select
                                        1
					,PRODUCT_NO
				  ,case when b.ip_type_id is not null then b.ip_type_id else '9000' end 
					/**,case when ip_type_id not in ('1000', '2000', '2100', '2101', '2102', '2199', '2200', '2201', 
                                        '2202', '2203', '2204', '2299', '3000', '3100', '3101', '3102', 
                                        '3103', '3104', '3105', '3106', '3107', '3108', '3109', '3110', 
                                        '3111', '3112', '3113', '3114', '3199', '3200', '4000', '4100', 
                                        '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', 
                                        '4109', '4110', '4111', '4199', '4200', '6000', '6100', '6101', 
                                        '6102', '6103', '6104', '6105', '6106', '6107', '6199', '6200', 
                                        '9000' ) 
                then '9000' else ip_type_id end **/
				FROM
					bass1.int_210012916_201103 a 
					left join bass1.dim_21003_ip_type b on a.ip_type_id = b.ip_type_id
          WHERE a.op_time=20110331
				 GROUP BY
					PRODUCT_NO
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,case when b.ip_type_id is not null then b.ip_type_id else '9000' end 
					,toll_type_id
					,adversary_id
					,roam_type_id
					,call_type_id
) t 					
1000	7269836
2102	57771
2202	1
4101	2
4102	5
4103	1
4104	2
6101	1
6104	28


select ip_type_id,count(0)
from (
select
                                        1
					,PRODUCT_NO
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
				  ,case when b.ip_type_id is not null then b.ip_type_id else '9000' end  ip_type_id
					/**,case when ip_type_id not in ('1000', '2000', '2100', '2101', '2102', '2199', '2200', '2201', 
                                        '2202', '2203', '2204', '2299', '3000', '3100', '3101', '3102', 
                                        '3103', '3104', '3105', '3106', '3107', '3108', '3109', '3110', 
                                        '3111', '3112', '3113', '3114', '3199', '3200', '4000', '4100', 
                                        '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', 
                                        '4109', '4110', '4111', '4199', '4200', '6000', '6100', '6101', 
                                        '6102', '6103', '6104', '6105', '6106', '6107', '6199', '6200', 
                                        '9000' ) 
                then '9000' else ip_type_id end **/
					,adversary_id
					,roam_type_id
					,call_type_id
					,char(bigint(sum(call_counts		)))
					,char(bigint(sum(base_bill_duration	)))
					,char(bigint(sum(toll_bill_duration	)))
					,char(bigint(sum(call_duration		)))
					,char(bigint(sum(base_call_fee		)))
					,char(bigint(sum(toll_call_fee		)))
					,char(bigint(sum(callfw_toll_fee	)))
					,char(bigint(sum(call_fee		)))
					,char(bigint(sum(favoured_basecall_fee	)))
					,char(bigint(sum(favoured_tollcall_fee	)))
					,char(bigint(sum(favoured_callfw_tollfee)))
					,char(bigint(sum(favoured_call_fee	)))
					,char(bigint(sum(free_duration		)))
					,char(bigint(sum(favour_duration	)))
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
				FROM
					bass1.int_210012916_201103 a 
					left join bass1.dim_21003_ip_type b on a.ip_type_id = b.ip_type_id
          WHERE a.op_time=20110331
				 GROUP BY
					PRODUCT_NO
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,case when b.ip_type_id is not null then b.ip_type_id else '9000' end 
					,adversary_id
					,roam_type_id
					,call_type_id
) t 					
group by ip_type_id

1000	3074539
2102	38036
2202	1
4101	2
4102	5
4103	1
4104	2
6101	1
6104	24

select ip_type_id,count(0)
from (
select
                                        1
					,PRODUCT_NO
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,case when ip_type_id not in ('1000', '2000', '2100', '2101', '2102', '2199', '2200', '2201', 
                                        '2202', '2203', '2204', '2299', '3000', '3100', '3101', '3102', 
                                        '3103', '3104', '3105', '3106', '3107', '3108', '3109', '3110', 
                                        '3111', '3112', '3113', '3114', '3199', '3200', '4000', '4100', 
                                        '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', 
                                        '4109', '4110', '4111', '4199', '4200', '6000', '6100', '6101', 
                                        '6102', '6103', '6104', '6105', '6106', '6107', '6199', '6200', 
                                        '9000' ) 
                then '9000' else ip_type_id end ip_type_id
					,adversary_id
					,roam_type_id
					,call_type_id
					,char(bigint(sum(call_counts		)))
					,char(bigint(sum(base_bill_duration	)))
					,char(bigint(sum(toll_bill_duration	)))
					,char(bigint(sum(call_duration		)))
					,char(bigint(sum(base_call_fee		)))
					,char(bigint(sum(toll_call_fee		)))
					,char(bigint(sum(callfw_toll_fee	)))
					,char(bigint(sum(call_fee		)))
					,char(bigint(sum(favoured_basecall_fee	)))
					,char(bigint(sum(favoured_tollcall_fee	)))
					,char(bigint(sum(favoured_callfw_tollfee)))
					,char(bigint(sum(favoured_call_fee	)))
					,char(bigint(sum(free_duration		)))
					,char(bigint(sum(favour_duration	)))
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
				FROM
					bass1.int_210012916_201103 a 
          WHERE op_time=20110331
				 GROUP BY
					PRODUCT_NO
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,ip_type_id
					,adversary_id
					,roam_type_id
					,call_type_id
) t 					
group by ip_type_id

1000	3074539
2102	38036
2202	1
4101	2
4102	5
4103	1
4104	2
6101	1
6104	24

select A.*,char(a.RUNTIME/60)||'min',char(a.RUNTIME/60/60)||'hr' from   app.sch_control_runlog A
where control_code like 'BASS1%DAY%' and control_code  not like '%EXP%'
and a.RUNTIME/60 >= 5
ORDER BY RUNTIME DESC 

		  TIME_ID INTEGER ,                                
		  PRODUCT_NO CHAR(15) NOT NULL ,                   
		  BRAND_ID CHAR(1) NOT NULL ,                      
		  SVC_TYPE_ID CHAR(3) NOT NULL ,                   
		  TOLL_TYPE_ID CHAR(3) NOT NULL ,                  
		  IP_TYPE_ID CHAR(4) NOT NULL ,                    
		  ADVERSARY_ID CHAR(6) NOT NULL ,                  
		  ROAM_TYPE_ID CHAR(3) NOT NULL ,                  
		  CALL_TYPE_ID CHAR(2) NOT NULL ,                  
		  CALL_COUNTS CHAR(14) NOT NULL ,                  
		  BASE_BILL_DURATION CHAR(14) NOT NULL ,           
		  TOLL_BILL_DURATION CHAR(14) NOT NULL ,           
		  CALL_DURATION CHAR(14) NOT NULL ,                
		  BASE_CALL_FEE CHAR(14) NOT NULL ,                
		  TOLL_CALL_FEE CHAR(14) NOT NULL ,                
		  CALLFW_TOLL_FEE CHAR(14) NOT NULL ,              
		  CALL_FEE CHAR(14) NOT NULL ,                     
		  FAVOURED_BASECALL_FEE CHAR(14) NOT NULL ,        
		  FAVOURED_TOLLCALL_FEE CHAR(14) NOT NULL ,        
		  FAVOURED_CALLFW_TOLLFEE CHAR(14) NOT NULL ,      
		  FAVOURED_CALL_FEE CHAR(14) NOT NULL ,            
		  FREE_DURATION CHAR(14) NOT NULL ,                
		  FAVOUR_DURATION CHAR(14) NOT NULL ,              
		  MNS_TYPE CHAR(1) ,                               
		  OPP_PROPERTY CHAR(1) )                           
		 DISTRIBUTE BY HASH(TIME_ID,                       
		 PRODUCT_NO)                                       
		   IN TBS_APP_BASS1 INDEX IN TBS_INDEX          
                                                         
                                                         
   ALTER TABLE BASS1.G_S_21003_TO_DAY
where time_id between 20110401 and 20110412
where time_id / 100 = 201103
20110412	3148528
20110411	3143936
20110410	3070838
20110409	3090109
20110408	3165495
20110407	3113247
20110406	3099055
20110405	2993327
20110404	2978821
20110403	3058565
20110402	3096712
20110401	3203944

           1
					,PRODUCT_NO
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,case when b.ip_type_id is not null then b.ip_type_id else '9000' end  ip_type_id
				/**,case when ip_type_id not in ('1000', '2000', '2100', '2101', '2102', '2199', '2200', '2201', 
                                        '2202', '2203', '2204', '2299', '3000', '3100', '3101', '3102', 
                                        '3103', '3104', '3105', '3106', '3107', '3108', '3109', '3110', 
                                        '3111', '3112', '3113', '3114', '3199', '3200', '4000', '4100', 
                                        '4101', '4102', '4103', '4104', '4105', '4106', '4107', '4108', 
                                        '4109', '4110', '4111', '4199', '4200', '6000', '6100', '6101', 
                                        '6102', '6103', '6104', '6105', '6106', '6107', '6199', '6200', 
                                        '9000' ) 
                then '9000' else ip_type_id end **/
					,adversary_id
					,roam_type_id
					,call_type_id
					,char(bigint(sum(call_counts		)))
					,char(bigint(sum(base_bill_duration	)))
					,char(bigint(sum(toll_bill_duration	)))
					,char(bigint(sum(call_duration		)))
					,char(bigint(sum(base_call_fee		)))
					,char(bigint(sum(toll_call_fee		)))
					,char(bigint(sum(callfw_toll_fee	)))
					,char(bigint(sum(call_fee		)))
					,char(bigint(sum(favoured_basecall_fee	)))
					,char(bigint(sum(favoured_tollcall_fee	)))
					,char(bigint(sum(favoured_callfw_tollfee)))
					,char(bigint(sum(favoured_call_fee	)))
					,char(bigint(sum(free_duration		)))
					,char(bigint(sum(favour_duration	)))
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
				FROM
					bass1.int_210012916_201104 a
					left join bass1.dim_21003_ip_type b on a.ip_type_id = b.ip_type_id					
          WHERE op_time=20110412
				 GROUP BY
					PRODUCT_NO
					,value(CHAR(MNS_TYPE),'0')
          ,value(CHAR(OPP_PROPERTY),'0')
					,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',char(brand_id)),'2')
					,svc_type_id
					,toll_type_id
					,case when b.ip_type_id is not null then b.ip_type_id else '9000' end 
					,adversary_id
					,roam_type_id
					,call_type_id
					with ur
20110301	3018794
20110302	2925358
20110303	2981854
20110304	2777079
20110305	3089573
20110306	2778975
20110307	2838726
20110308	2895024
20110309	2885696
20110310	2950512
20110311	3091927
20110312	3054205
20110313	3042400
20110314	3021563
20110315	3006895
20110316	3054129
20110317	3306599
20110318	3125961
20110319	3085239
20110320	2982375
20110321	3037803
20110322	3046467
20110323	3067281
20110324	3064333
20110325	3101065
20110326	3035791
20110327	3018674
20110328	3072291
20110329	3059620
20110330	3089545
20110331	3112611
20110401	3203944
20110402	3096712
20110403	3058565
20110404	2978821
20110405	2993327
20110406	3099055
20110407	3113247
20110408	3165495
20110409	3090109
20110410	3070838
20110411	3143936
20110412	3148528

 �ֻ����䣨ADC�����������룺87258 
�ֻ����䣨ADC��ʹ�ü��ſͻ���������0 
329300
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
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'
�ĳ�from bass2.Dw_cm_busi_radius_dm_201103 b,
��
CREATE FUNCTION BASS1.FN_GET_ALL_DIM_160(GID VARCHAR(20), DID VARCHAR(20)) RETURNS     VARCHAR(10) LANGUAGE SQL DETERMINISTIC NO EXTERNAL ACTION READS SQL DATA NULL CALL INHERIT SPECIAL REGISTERS BEGIN ATOMIC RETURN 


               	   where item_id in (80000185)

where PRODUCT_INSTANCE_ID
select * from   bass2.dw_product_201103
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'

201102	17396
select count(0)     
               	   from bass2.dw_acct_shoulditem_201103
               	   where item_id in (80000618,80000619)
               	   group by cust_id,acct_id,item_id) a
		         		when b.enterprise_id is not null then b.enterprise_id
		         		when c.enterprise_id is not null then c.enterprise_id
		         		when d.enterprise_id is not null then d.enterprise_id
                           else '' end as enterprise_id,
                      a.acct_id,
                      a.item_id,
                      case when c.level_def_mode=1 then 1 when c.enterprise_id=d.enterprise_id and c.level_def_mode=1 then 1 else 0 end as special_mark,
                      '966'    ,
               	    sum(case when b.acct_id is not null then a.fact_fee else 0 end) as unipay_fee,
               	    sum(case when b.acct_id is null then a.fact_fee else 0 end) as non_unipay_fee
               from (select cust_id,acct_id,item_id,sum(fact_fee) as fact_fee
               	   from bass2.dw_acct_shoulditem_201103
               	   where item_id in (80000618,80000619)
               	   group by cust_id,acct_id,item_id) a left join bass2.dw_enterprise_account_his_201103 b on a.acct_id=b.acct_id
               		                            left join bass2.dw_enterprise_msg_201103 c on a.cust_id=c.cust_id
               		                            left join bass2.dw_enterprise_member_his_201103 d on a.cust_id=d.cust_id
               group by case
               			when b.enterprise_id is not null then b.enterprise_id
               			when c.enterprise_id is not null then c.enterprise_id
               			when d.enterprise_id is not null then d.enterprise_id
                             else '' end,
                        a.acct_id,
                        a.item_id,
                        case when c.level_def_mode=1 then 1 when c.enterprise_id=d.enterprise_id and c.level_def_mode=1 then 1 else 0 end
                  from
                  (
                   select
                     a.enterprise_id as enterprise_id
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002') as ent_busi_id
                     ,case
												when upper(b.config_value)='MAS' then '1'
												when upper(b.config_value)='ADC' then '2'
												else '3'
											end as manage_mod
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0109',char(a.service_id)),'090') as account_item
                     ,sum(a.unipay_fee)*100 as income
                   from  bass2.dw_enterprise_unipay_201103 a
                   left join (select * from bass2.dim_service_config where config_id=1000027)  b on a.service_id = b.service_id
				           left join (select * from bass1.all_dim_lkp_160 where bass1_tbid='BASS_STD1_0108') c on a.service_id = c.xzbas_value
                   where a.test_mark = 0
                     and a.service_id not in ('936','966') and a.enterprise_id is not null
                   group by
                     a.enterprise_id
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002')
                     ,case
												when upper(b.config_value)='MAS' then '1'
												when upper(b.config_value)='ADC' then '2'
												else '3'
											end
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0109',char(a.service_id)),'090')
                  )t   where   ent_busi_id = '1220'        
count(0)
                  from
                  (
                   select
                     a.enterprise_id as enterprise_id
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002') as ent_busi_id
                     ,case
												when upper(b.config_value)='MAS' then '1'
												when upper(b.config_value)='ADC' then '2'
												else '3'
											end as manage_mod
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0109',char(a.service_id)),'090') as account_item
                     ,sum(a.unipay_fee)*100 as income
                   from  bass2.dw_enterprise_unipay_201103 a
                   left join (select * from bass2.dim_service_config where config_id=1000027)  b on a.service_id = b.service_id
				           left join (select * from bass1.all_dim_lkp_160 where bass1_tbid='BASS_STD1_0108') c on a.service_id = c.xzbas_value
                   where a.test_mark = 0
                     and a.service_id not in ('936','966','926') and a.enterprise_id is not null
                   group by
                     a.enterprise_id
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002')
                     ,case
												when upper(b.config_value)='MAS' then '1'
												when upper(b.config_value)='ADC' then '2'
												else '3'
											end
                     ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0109',char(a.service_id)),'090')
                  )t       
except                  
select 
 enterprise_id,
                    ent_busi_id,
                    manage_mod,
                    account_item from   g_s_03017_month    where time_id = 201103    and  ent_busi_id = '1220' 
                    
		  TIME_ID INTEGER NOT NULL ,                                          
		  ENTERPRISE_ID CHAR(20) NOT NULL ,                                   
		  ENT_BUSI_ID CHAR(4) NOT NULL ,                                      
		  MANAGE_MOD CHAR(1) NOT NULL ,                                       
		  ACCOUNT_ITEM CHAR(3) NOT NULL ,                                     
		  INCOME CHAR(12) NOT NULL )                                          
		 DISTRIBUTE BY HASH(TIME_ID,                                          
		 ENTERPRISE_ID,                                                       
		 ENT_BUSI_ID)                                                         
		   IN TBS_APP_BASS1 INDEX IN TBS_INDEX NOT LOGGED INITIALLY        
                                                                            

 		sum(bigint(flowup)+bigint(flowdown))*1.00/1024/1024/1024 wlan_flow
 		,count(distinct product_no) user_cnt
        from   bass1.G_S_04003_DAY
		where time_id between  20110301 and 20110331
CREATE TABLE BASS1.G_I_77780_DAY_DOWN20110414
 (TIME_ID                char(1)         ,
  ENTERPRISE_ID          CHARACTER(20),
  ID                     CHARACTER(9),
  ENTERPRISE_NAME        CHARACTER(60),
  ORG_TYPE               CHARACTER(5),
  ADDR_CODE              CHARACTER(6),
  CITY                   CHARACTER(20),
  REGION                 CHARACTER(20),
  COUNTY                 CHARACTER(20),
  DOOR_NO                CHARACTER(60),
  AREA_CODE              CHARACTER(5),
  PHONE_NO1              CHARACTER(11),
  PHONE_NO2              CHARACTER(10),
  POST_CODE              CHARACTER(6),
  INDUSTRY_TYPE          CHARACTER(4),
  EMPLOYEE_CNT           CHARACTER(8),
  INDUSTRY_UNIT_CNT      CHARACTER(5),
  ECONOMIC_TYPE          CHARACTER(3),
  OPEN_YEAR              CHARACTER(4),
  OPEN_MONTH             CHARACTER(2),
  SHAREHOLDER            CHARACTER(1),
  GROUP_TYPE             CHARACTER(1),
  MANAGE_STYLE           CHARACTER(1),
  OPERATE_REVENUE_CLASS  CHARACTER(2),
  CAPITAL_CLASS          CHARACTER(2),
  INDUSTRY_CLASS_CODE    CHARACTER(2)   ,
  CUST_STATUS            CHARACTER(2),
  CUST_INFO_SRC_ID       CHARACTER(1),
  UPLOAD_TYPE_ID         CHARACTER(1)    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    ID
   ) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY_DOWN20110414
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

select count(0) from   G_I_77780_DAY_DOWN20110414

                       ,coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002')     as ent_busi_id
                       ,case
													when upper(c.config_value)='MAS' then '1'
													when upper(c.config_value)='ADC' then '2'
													else '3'
											  end             as manage_mod
                       ,'1'             as acct_type
               	       ,sum(bigint(non_unipay_fee))  as income
                  from bass2.dw_enterprise_unipay_201103 a,
                       (select acct_id,min(user_id) as user_id from  bass2.dw_product_201103 where userstatus_id<>0 and usertype_id in (1,2,9) group by acct_id) b,
                       (select * from bass2.dim_service_config where config_id=1000027)  c,
				               (select * from bass1.all_dim_lkp_160 where bass1_tbid='BASS_STD1_0108') d
                 where a.acct_id = b.acct_id 
                   and a.service_id =c.service_id
                   and a.service_id = d.xzbas_value
                   and a.test_mark = 0 
                   and a.service_id <> '926'
                 group by b.user_id,
                 coalesce(bass1.fn_get_all_dim_160('BASS_STD1_0108',char(a.service_id)),'4002'),
                 case
									when upper(c.config_value)='MAS' then '1'
									when upper(c.config_value)='ADC' then '2'
									else '3'
							   end
CREATE TABLE BASS1   .G_S_03018_MONTH_B20110414  (
		  TIME_ID INTEGER NOT NULL , 
		  USER_ID CHAR(20) NOT NULL , 
		  ENT_BUSI_ID CHAR(4) NOT NULL , 
		  MANAGE_MOD CHAR(1) NOT NULL , 
		  ACCT_TYPE CHAR(1) NOT NULL , 
		  INCOME CHAR(12) NOT NULL )   
		 DISTRIBUTE BY HASH(TIME_ID,  
		 USER_ID)   
		   IN TBS_APP_BASS1 INDEX IN TBS_INDEX NOT LOGGED INITIALLY  
           
SELECT *

CREATE TABLE BASS1   .G_I_06021_MONTH_B20110415  (
		  TIME_ID INTEGER NOT NULL , 
		  CHANNEL_ID CHAR(25) NOT NULL , 
		  CHANNEL_TYPE CHAR(1) NOT NULL , 
		  SELF_CHANNEL_ID CHAR(25) , 
		  CMCC_ID CHAR(5) NOT NULL , 
		  COUNTRY_NAME CHAR(30) NOT NULL , 
		  THORPE_NAME CHAR(50) NOT NULL , 
		  CHANNEL_NAME CHAR(100) NOT NULL , 
		  CHANNEL_ADDR CHAR(100) NOT NULL , 
		  POSITION CHAR(1) NOT NULL , 
		  REGION_INFO CHAR(1) NOT NULL , 
		  CHANNEL_B_TYPE CHAR(1) NOT NULL , 
		  IS_EXCLUDE CHAR(1) NOT NULL , 
		  IS_PHONE_SHOP CHAR(1) NOT NULL , 
		  CHANNEL_STAR CHAR(1) , 
		  CHANNEL_STATUS CHAR(1) NOT NULL , 
		  BUSINESS_BEGIN CHAR(4) NOT NULL , 
		  BUSINESS_END CHAR(4) NOT NULL , 
		  VALID_DATE CHAR(8) NOT NULL , 
		  EXPIRE_DATE CHAR(8) NOT NULL , 
		  TIMES CHAR(4) , 
		  LONGITUDE CHAR(10) , 
		  LATITUDE CHAR(10) , 
		  FITMENT_PRICE CHAR(10) , 
		  EQUIP_PRICE CHAR(10) , 
		  PRICES CHAR(10) , 
		  CHARGE CHAR(10) )   
		 DISTRIBUTE BY HASH(TIME_ID,  
		 CHANNEL_ID)   
		   IN TBS_APP_BASS1 INDEX IN TBS_INDEX  
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end,2) , char(10)) latitude2  from bass2.dual
SELECT cast(round(case when 28.00000+rand(1)*4 < 28.425222 then 28.425222 
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end,2) as char(10)) latitude2   from G_I_06021_MONTH_B20110415 fetch first 10 rows only
create view t_v_06021
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end,5) latitude   from G_I_06021_MONTH_B20110415
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end),9,5) latitude   from G_I_06021_MONTH_B20110415
			when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        else 80.00000+rand(1)*8  end,5) latitude   from G_I_06021_MONTH_B20110415
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end) as decimal(7,5))) latitude   from G_I_06021_MONTH_B20110415
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end) as decimal(7,5)))
	from bass2.dw_channel_info_201103 a
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)
			when 28.00000+rand(1)*4 > 32.398516 then 32.398516 
        else 28.00000+rand(1)*4  end) as decimal(7,5)))
								char(cast((case when 80.00000+rand(1)*8 < 80.337524 then 80.337524
								when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        				else 80.00000+rand(1)*8  end) as decimal(7,5)))
          else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5))) end  )) longitude
	from bass2.dw_channel_info_201103 a
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)
SELECT
	   201103
		,trim(char(a.channel_id))
		,case when a.channel_type_class=90105 then '1'
          else '3'
     end channel_type
		,'' self_channel_id
		,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',SUBSTR(A.REGION_CODE,2,3)),'13101')
		,value(b.county_name,'δ֪')
		,value(c.thorpe_name,'δ֪')
		,value(a.channel_name,'δ֪')
		,value(a.channel_address,'δ֪')
		,case when a.geography_type in (1,2,3) then '1'
			 		when a.geography_type in (4) then '2'
			 		when a.geography_type in (5) then '3'
			 		else '4'
		 end  position
		,case when a.geography_property=1 then '4'
			 		when a.geography_property=2 then '2'
			 		when a.geography_property=3 then '1'
			 		when a.geography_property=4 then '3'
			 		when a.geography_property=5 then '6'
			 		when a.geography_property=6 then '5'
			 		when a.geography_property=7 then '7'
			 		when a.geography_property=8 then '3'
			 		when a.geography_property=9 then '3'
			 		else '3'
		 end region_info
		,case when a.channel_type_class=90105 and a.channel_type in (90153,90155,90157,90158,90196,90940,90942,90943) then '1'
			 		when a.channel_type_class=90105 and a.channel_type in (90154,90941) then '2'
			 		when a.channel_type_class=90105 and a.channel_type in (90156) then '3'
			 		when a.channel_type_class=90102 and a.channel_type in (90881) then '5'
			 		when a.channel_type_class=90102 and a.channel_type in (90885) then '6'
			 		else '6'
		 end channel_b_type
		,case when a.is_exclude=1 then '1' else '0' end
		,'0' is_phone_shop
		,case when a.channel_type_class=90105 then ''
			    else value(trim(char(channel_level)),'6')
		 end channel_star
		,case when a.user_state=1 and a.state=0 then '1'
			 		when a.user_state=2 and a.state=0 then '2'
			 		when a.user_state=3 and a.state=0 then '3'
			 		else '3'
		 end channel_status
		,'0930'
		,'1830'
		,'00010101' valid_date
		,'00010101' expire_date
		,'' times
		,case when a.longitude is null or a.longitude*1.00/100 < 80  or a.longitude*1.00/100 > 99 then 
								char(cast((case when 80.00000+rand(1)*8 < 80.337524 then 80.337524
								when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        				else 80.00000+rand(1)*8  end) as decimal(7,5)))
          else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5))) end  longitude
		,case when a.latitude is null or a.latitude*1.00/100 < 26 or a.latitude*1.00/100 > 36 then 
							char(cast((case when 26.00000+rand(1)*10 < 26.425222 then 26.425222 
								when 26.00000+rand(1)*10 > 36.398516 then 36.398516 
					        else 26.00000+rand(1)*10  end) as decimal(7,5)))
					else char(cast(a.latitude*1.00/100 as decimal(7,5))) end  latitude
		--,value(trim(char(a.latitude   )),'0')
		,'' fitment_price
		,'' equip_price
		,'' prices
		,'' charge
	from bass2.dw_channel_info_201103 a
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)
SELECT
	   201103 time_id
		,trim(char(a.channel_id)) channel_id
		,case when a.channel_type_class=90105 then '1'
          else '3'
     end channel_type
		,'' self_channel_id
		,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',SUBSTR(A.REGION_CODE,2,3)),'13101')
		,value(b.county_name,'δ֪')
		,value(c.thorpe_name,'δ֪')
		,value(a.channel_name,'δ֪')
		,value(a.channel_address,'δ֪')
		,case when a.geography_type in (1,2,3) then '1'
			 		when a.geography_type in (4) then '2'
			 		when a.geography_type in (5) then '3'
			 		else '4'
		 end  position
		,case when a.geography_property=1 then '4'
			 		when a.geography_property=2 then '2'
			 		when a.geography_property=3 then '1'
			 		when a.geography_property=4 then '3'
			 		when a.geography_property=5 then '6'
			 		when a.geography_property=6 then '5'
			 		when a.geography_property=7 then '7'
			 		when a.geography_property=8 then '3'
			 		when a.geography_property=9 then '3'
			 		else '3'
		 end region_info
		,case when a.channel_type_class=90105 and a.channel_type in (90153,90155,90157,90158,90196,90940,90942,90943) then '1'
			 		when a.channel_type_class=90105 and a.channel_type in (90154,90941) then '2'
			 		when a.channel_type_class=90105 and a.channel_type in (90156) then '3'
			 		when a.channel_type_class=90102 and a.channel_type in (90881) then '5'
			 		when a.channel_type_class=90102 and a.channel_type in (90885) then '6'
			 		else '6'
		 end channel_b_type
		,case when a.is_exclude=1 then '1' else '0' end
		,'0' is_phone_shop
		,case when a.channel_type_class=90105 then ''
			    else value(trim(char(channel_level)),'6')
		 end channel_star
		,case when a.user_state=1 and a.state=0 then '1'
			 		when a.user_state=2 and a.state=0 then '2'
			 		when a.user_state=3 and a.state=0 then '3'
			 		else '3'
		 end channel_status
		,'0930'
		,'1830'
		,'00010101' valid_date
		,'00010101' expire_date
		,'' times
		,case when a.longitude is null or a.longitude*1.00/100 < 80  or a.longitude*1.00/100 > 99 then 
								char(cast((case when 80.00000+rand(1)*8 < 80.337524 then 80.337524
								when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        				else 80.00000+rand(1)*8  end) as decimal(7,5)))
          else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5))) end  longitude
		,case when a.latitude is null or a.latitude*1.00/100 < 26 or a.latitude*1.00/100 > 36 then 
							char(cast((case when 26.00000+rand(1)*10 < 26.425222 then 26.425222 
								when 26.00000+rand(1)*10 > 36.398516 then 36.398516 
					        else 26.00000+rand(1)*10  end) as decimal(7,5)))
					else char(cast(a.latitude*1.00/100 as decimal(7,5))) end  latitude
		--,value(trim(char(a.latitude   )),'0')
		,'' fitment_price
		,'' equip_price
		,'' prices
		,'' charge
	from bass2.dw_channel_info_201103 a
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)
) t
where time_id =201102
  and channel_status='1'
  and channel_type='1'
  and (longitude='0' or longitude='' or latitude='0' or latitude='')
  
        ,STATMONTH
        ,CHANNEL_ID
        ,ACCEPT_TYPE
        ,NEW_USERS
        ,HAND_CNT
        ,HAND_FEE
        ,CARD_SALE_CNT
        ,ACCEPT_CNT
        ,IMP_ACCEPT_CNT
        ,TERM_SALE_CNT
        ,OTHER_SALE_CNT
        ,ACCEPT_BAS_CNT
        ,QUERY_BAS_CNT
         max(TIME_ID					)
        ,max(STATMONTH        )
        ,max(CHANNEL_ID       )
        ,max(ACCEPT_TYPE      )
        ,max(NEW_USERS        )
        ,max(HAND_CNT         )
        ,max(HAND_FEE         )
        ,max(CARD_SALE_CNT    )
        ,max(ACCEPT_CNT       )
        ,max(IMP_ACCEPT_CNT   )
        ,max(TERM_SALE_CNT    )
        ,max(OTHER_SALE_CNT   )
        ,max(ACCEPT_BAS_CNT   )
        ,max(QUERY_BAS_CNT    )
from  BASS1.G_S_22062_MONTH a
               
         min(TIME_ID					)
        ,min(STATMONTH        )
        ,min(CHANNEL_ID       )
        ,min(ACCEPT_TYPE      )
        ,min(NEW_USERS        )
        ,min(HAND_CNT         )
        ,min(HAND_FEE         )
        ,min(CARD_SALE_CNT    )
        ,min(ACCEPT_CNT       )
        ,min(IMP_ACCEPT_CNT   )
        ,min(TERM_SALE_CNT    )
        ,min(OTHER_SALE_CNT   )
        ,min(ACCEPT_BAS_CNT   )
        ,min(QUERY_BAS_CNT    )
from  BASS1.G_S_22062_MONTH a
               

CREATE TABLE BASS1   .G_S_22062_MONTH_TEST  (
		  TIME_ID INTEGER NOT NULL , 
		  STATMONTH CHAR(6) NOT NULL , 
		  CHANNEL_ID CHAR(25) NOT NULL , 
		  ACCEPT_TYPE CHAR(1) , 
		  NEW_USERS CHAR(8) , 
		  HAND_CNT CHAR(10) , 
		  HAND_FEE CHAR(10) , 
		  CARD_SALE_CNT CHAR(8) , 
		  ACCEPT_CNT CHAR(10) , 
		  IMP_ACCEPT_CNT CHAR(10) , 
		  TERM_SALE_CNT CHAR(10) , 
		  OTHER_SALE_CNT CHAR(10) , 
		  ACCEPT_BAS_CNT CHAR(10) , 
		  QUERY_BAS_CNT CHAR(10) )   
		 DISTRIBUTE BY HASH(TIME_ID,  
		 STATMONTH,  
		 CHANNEL_ID)   
		   IN TBS_APP_BASS1 INDEX IN TBS_INDEX  


insert into   bass1.G_S_22062_MONTH_TEST
select 
			   201103 TIME_ID
			 	,'201103' STATMONTH
        ,a.CHANNEL_ID
        ,'1' ACCEPT_TYPE
        ,'0' NEW_USERS
        ,'0' HAND_CNT
        ,'0' HAND_FEE
        ,'0' CARD_SALE_CNT
        ,'0' ACCEPT_CNT
        ,'0' IMP_ACCEPT_CNT
        ,'0' TERM_SALE_CNT
        ,'0' OTHER_SALE_CNT
        ,'0' ACCEPT_BAS_CNT
        ,'0' QUERY_BAS_CNT                       
from  bass1.g_i_06021_month a 
where a.channel_id not in
(select distinct b.channel_id from bass1.g_s_22062_month b where b.time_id = 201103)
  and a.time_id =201103
  and a.channel_status='1'
  
  
select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.G_S_22062_MONTH_TEST where time_id =201103)
  and time_id =201103
  and channel_status='1'
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month where time_id =201103)
  and time_id =201103
  and channel_type in ('2','3')
  and channel_status='1'
  

CREATE TABLE BASS1   .G_S_22063_MONTH_TEST  (
		  TIME_ID INTEGER NOT NULL , 
		  STATMONTH CHAR(6) NOT NULL , 
		  CHANNEL_ID CHAR(25) NOT NULL , 
		  FH_REWARD CHAR(10) , 
		  BASIC_REWARD CHAR(10) , 
		  INCR_REWARD CHAR(10) , 
		  INSPIRE_REWARD CHAR(10) , 
		  TERM_REWARD CHAR(10) , 
		  RENT_CHARGE CHAR(8) )   
		 DISTRIBUTE BY HASH(TIME_ID,  
		 STATMONTH,  
		 CHANNEL_ID)   
		   IN TBS_APP_BASS1 INDEX IN TBS_INDEX  

insert into bass1.g_s_22063_month_test
select 
         TIME_ID
        ,STATMONTH
        ,CHANNEL_ID
        ,'0' FH_REWARD
        ,'100' BASIC_REWARD
        ,'0' INCR_REWARD
        ,'0' INSPIRE_REWARD
        ,'0' TERM_REWARD
        ,'0' RENT_CHARGE
from   bass1.g_i_06021_month a
where a.channel_id not in
(select distinct b.channel_id from bass1. g_s_22063_month b where b.time_id =201103)
  and a.time_id =201103
  and a.channel_type in ('2','3')
  and a.channel_status='1'
        

insert into bass1.g_s_22063_month_test
select 
			   201103 TIME_ID
			 	,'201103' STATMONTH
        ,CHANNEL_ID
        ,'0' FH_REWARD
        ,'100' BASIC_REWARD
        ,'0' INCR_REWARD
        ,'0' INSPIRE_REWARD
        ,'0' TERM_REWARD
        ,'0' RENT_CHARGE
from   bass1.g_i_06021_month a
where a.channel_id not in
(select distinct b.channel_id from bass1. g_s_22063_month b where b.time_id =201103)
  and a.time_id =201103
  and a.channel_type in ('2','3')
  and a.channel_status='1'
        
select count(0) from     bass1.g_s_22063_month_test
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month_test where time_id =201103)
  and time_id =201103
  and channel_type in ('2','3')
  and channel_status='1'
                  
BASS1_G_S_22062_MONTH.tcl	BASS2_Dw_acct_payment_dm.tcl


	   201103
		,trim(char(a.channel_id))
		,case when a.channel_type_class=90105 then '1'
          else '3'
     end channel_type
		,'' self_channel_id
		,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',SUBSTR(A.REGION_CODE,2,3)),'13101')
		,value(b.county_name,'δ֪')
		,value(c.thorpe_name,'δ֪')
		,value(a.channel_name,'δ֪')
		,value(a.channel_address,'δ֪')
		,case when a.geography_type in (1,2,3) then '1'
			 		when a.geography_type in (4) then '2'
			 		when a.geography_type in (5) then '3'
			 		else '4'
		 end  position
		,case when a.geography_property=1 then '4'
			 		when a.geography_property=2 then '2'
			 		when a.geography_property=3 then '1'
			 		when a.geography_property=4 then '3'
			 		when a.geography_property=5 then '6'
			 		when a.geography_property=6 then '5'
			 		when a.geography_property=7 then '7'
			 		when a.geography_property=8 then '3'
			 		when a.geography_property=9 then '3'
			 		else '3'
		 end region_info
		,case when a.channel_type_class=90105 and a.channel_type in (90153,90155,90157,90158,90196,90940,90942,90943) then '1'
			 		when a.channel_type_class=90105 and a.channel_type in (90154,90941) then '2'
			 		when a.channel_type_class=90105 and a.channel_type in (90156) then '3'
			 		when a.channel_type_class=90102 and a.channel_type in (90881) then '5'
			 		when a.channel_type_class=90102 and a.channel_type in (90885) then '6'
			 		else '6'
		 end channel_b_type
		,case when a.is_exclude=1 then '1' else '0' end
		,'0' is_phone_shop
		,case when a.channel_type_class=90105 then ''
			    else value(trim(char(channel_level)),'6')
		 end channel_star
		,case when a.user_state=1 and a.state=0 then '1'
			 		when a.user_state=2 and a.state=0 then '2'
			 		when a.user_state=3 and a.state=0 then '3'
			 		else '3'
		 end channel_status
		,'0930'
		,'1830'
		,'00010101' valid_date
		,'00010101' expire_date
		,'' times
		,case when a.channel_type_class=90105 then 
				case when a.longitude is null or a.longitude*1.00/100 < 80  or a.longitude*1.00/100 > 99 then 
								char(cast((case when 80.00000+rand(1)*8 < 80.337524 then 80.337524
								when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        				else 80.00000+rand(1)*8  end) as decimal(7,5)))
          else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5))) end  
      else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5)))   
     end longitude
		,case when a.channel_type_class=90105 then 
				case when a.latitude is null or a.latitude*1.00/100 < 26 or a.latitude*1.00/100 > 36 then 
							char(cast((case when 26.00000+rand(1)*10 < 26.425222 then 26.425222 
								when 26.00000+rand(1)*10 > 36.398516 then 36.398516 
					        else 26.00000+rand(1)*10  end) as decimal(7,5)))
					else char(cast(a.latitude*1.00/100 as decimal(7,5))) end  
			else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5)))   		
			end latitude
		,'' fitment_price
		,'' equip_price
		,'' prices
		,'' charge
	from bass2.dw_channel_info_201103 a
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)                             

CREATE TABLE BASS1.G_I_77780_DAY_DOWN20110415
 (TIME_ID                INTEGER         ,
  ENTERPRISE_ID          CHARACTER(20),
  ID                     CHARACTER(9),
  ENTERPRISE_NAME        CHARACTER(60),
  ORG_TYPE               CHARACTER(5),
  ADDR_CODE              CHARACTER(6),
  CITY                   CHARACTER(20),
  REGION                 CHARACTER(20),
  COUNTY                 CHARACTER(20),
  DOOR_NO                CHARACTER(60),
  AREA_CODE              CHARACTER(5),
  PHONE_NO1              CHARACTER(11),
  PHONE_NO2              CHARACTER(10),
  POST_CODE              CHARACTER(6),
  INDUSTRY_TYPE          CHARACTER(4),
  EMPLOYEE_CNT           CHARACTER(8),
  INDUSTRY_UNIT_CNT      CHARACTER(5),
  ECONOMIC_TYPE          CHARACTER(3),
  OPEN_YEAR              CHARACTER(4),
  OPEN_MONTH             CHARACTER(2),
  SHAREHOLDER            CHARACTER(1),
  GROUP_TYPE             CHARACTER(1),
  MANAGE_STYLE           CHARACTER(1),
  OPERATE_REVENUE_CLASS  CHARACTER(2),
  CAPITAL_CLASS          CHARACTER(2),
  INDUSTRY_CLASS_CODE    CHARACTER(2)    ,
  CUST_STATUS            CHARACTER(2),
  CUST_INFO_SRC_ID       CHARACTER(1),
  UPLOAD_TYPE_ID         CHARACTER(1)   
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    ID
   ) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY_DOWN20110415
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

drop table BASS1.G_I_77780_DAY_DOWN20110415
CREATE TABLE BASS1.G_I_77780_DAY_DOWN20110415
 (TIME_ID                char(1)         ,
  ENTERPRISE_ID          CHARACTER(20),
  ID                     CHARACTER(9),
  ENTERPRISE_NAME        CHARACTER(60),
  ORG_TYPE               CHARACTER(5),
  ADDR_CODE              CHARACTER(6),
  CITY                   CHARACTER(20),
  REGION                 CHARACTER(20),
  COUNTY                 CHARACTER(20),
  DOOR_NO                CHARACTER(60),
  AREA_CODE              CHARACTER(5),
  PHONE_NO1              CHARACTER(11),
  PHONE_NO2              CHARACTER(10),
  POST_CODE              CHARACTER(6),
  INDUSTRY_TYPE          CHARACTER(4),
  EMPLOYEE_CNT           CHARACTER(8),
  INDUSTRY_UNIT_CNT      CHARACTER(5),
  ECONOMIC_TYPE          CHARACTER(3),
  OPEN_YEAR              CHARACTER(4),
  OPEN_MONTH             CHARACTER(2),
  SHAREHOLDER            CHARACTER(1),
  GROUP_TYPE             CHARACTER(1),
  MANAGE_STYLE           CHARACTER(1),
  OPERATE_REVENUE_CLASS  CHARACTER(2),
  CAPITAL_CLASS          CHARACTER(2),
  INDUSTRY_CLASS_CODE    CHARACTER(2)    ,
  CUST_STATUS            CHARACTER(2),
  CUST_INFO_SRC_ID       CHARACTER(1),
  UPLOAD_TYPE_ID         CHARACTER(1)   
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    ID
   ) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY_DOWN20110415
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE

01002
01004
02004
02008
02011
02053
06031
06032

             from APP.SCH_CONTROL_TASK
             where upper(replace(replace(replace(CMD_LINE,'YESTERDAY()',''),'LASTMONTH()',''),' ',''))=upper(replace(:in_CMDLine,' ',''))
(
 '01006'
,'01007'
,'02013'
,'02055'
,'02056'
,'02057'
,'02058'
,'02060'
,'02061'
,'02064'
,'06001'
,'04009'
,'04010'
,'04012'
,'21004'
,'21005'
,'21009'
,'21016')

2346797


trim(char(RULE_ID))|| space(5-length(trim(char(RULE_ID)))
||trim(char(FLAG))|| space(2-length(trim(char(FLAG)))
||trim(char(RET_VAL))|| space(100-length(trim(char(RET_VAL)))
from bass1.g_i_06031_day
trim(char(RULE_ID))|| space(5-length(trim(char(RULE_ID)))
||trim(char(FLAG))|| space(2-length(trim(char(FLAG)))
||trim(char(RET_VAL))|| space(100-length(trim(char(RET_VAL)))
from bass1.g_i_06031_day where time_id = 20110415
trim(char(MSISDN))||space(9-length(trim(char(MSISDN))))
||trim(char(CMCC_ID))||space(5-length(trim(char(CMCC_ID))))
from bass1.g_i_06031_day where time_id = 20110415

trim(char(CMCC_ID))|| space(5-length(trim(char(CMCC_ID))))
||trim(char(CMCC_NAME))|| space(100-length(trim(char(CMCC_NAME))))
||trim(char(CMCC_DESC))|| space(20-length(trim(char(CMCC_DESC))))
from bass1.g_i_06032_day where time_id = 20110415
trim(char(CMCC_ID)) a, space(5-length(trim(char(CMCC_ID)))) b
,trim(char(CMCC_NAME)) c,space(100-length(trim(char(CMCC_NAME)))) c
,trim(char(CMCC_DESC)) e, space(20-length(trim(char(CMCC_DESC)))) f
from bass1.g_i_06032_day where time_id = 20110415
trim(char(CMCC_ID))|| space(5-length(trim(char(CMCC_ID))))
||trim(char(CMCC_NAME))|| space(100-length(trim(char(CMCC_NAME))))
--||trim(char(CMCC_DESC))|| space(20-length(trim(char(CMCC_DESC))))
from bass1.g_i_06032_day where time_id = 20110415
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)

DR19','DR21','DR22','DR31','DR32')
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)

 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
WHERE CONTROL_CODE IN ('BASS1_EXP_G_A_01004_DAY','BASS1_EXP_G_S_04006_DAY','BASS1_EXP_G_S_22038_DAY')

WHERE CONTROL_CODE IN ('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')

select * from app.sch_control_task
WHERE CONTROL_CODE IN ('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')


 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)
select * from app.sch_control_before where control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)

 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
)

SELECT * FROM APP.SCH_CONTROL_BEFORE_20110417 
WHERE CONTROL_CODE IN ('BASS1_EXP_G_A_01004_DAY','BASS1_EXP_G_S_04006_DAY','BASS1_EXP_G_S_22038_DAY')

select count(0) from app.SCH_CONTROL_BEFORE WHERE CONTROL_CODE LIKE 'BASS1_%'
964
UPDATE APP.sch_control_before 
SET before_control_code = 'BASS1_G_A_01004_DAY.tcl'
WHERE CONTROL_CODE = 'BASS1_EXP_G_A_01004_DAY'

UPDATE APP.sch_control_before 
SET before_control_code = 'BASS1_G_S_04006_DAY.tcl'
WHERE CONTROL_CODE = 'BASS1_EXP_G_S_04006_DAY'

UPDATE APP.sch_control_before 
SET before_control_code = 'BASS1_G_S_22038_DAY.tcl'
WHERE CONTROL_CODE = 'BASS1_EXP_G_S_22038_DAY'
delete FROM APP.sch_control_before 
WHERE CONTROL_CODE IN ('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')

set CC_FLAG = 2
WHERE CONTROL_CODE IN ('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')

INSERT INTO  APP.sch_control_before 
SELECT * FROM APP.SCH_CONTROL_BEFORE_20110417 
WHERE CONTROL_CODE IN ('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')

SELECT * FROM APP.sch_control_before 
WHERE CONTROL_CODE IN ('BASS1_EXP_G_A_01004_DAY','BASS1_EXP_G_S_04006_DAY','BASS1_EXP_G_S_22038_DAY')

964
select * from app.sch_control_task where control_code in (
 'BASS1_INT_CHECK_33TO40_DAY.tcl'
,'BASS1_INT_CHECK_36_MONTH.tcl'
,'BASS1_INT_CHECK_59_62H789I01_TO_DAY.tcl'
,'BASS1_INT_CHECK_63EO_TO_DAY.tcl'
,'BASS1_INT_CHECK_8895_MONTH.tcl'
,'BASS1_INT_CHECK_98Z6_MONTH.tcl'
,'BASS1_INT_CHECK_A0L694B5_DAY.tcl'
,'BASS1_INT_CHECK_A12E6_DAY.tcl'
,'BASS1_INT_CHECK_C567_DAY.tcl'
,'INT_CHECK_DATARULE_DAY.tcl'
,'BASS1_INT_CHECK_E5_MONTH.tcl'
,'BASS1_INT_CHECK_F0_TO_DAY.tcl'
,'BASS1_INT_CHECK_F1_TO_DAY.tcl'
,'BASS1_INT_CHECK_F7_MONTH.tcl'
,'BASS1_INT_CHECK_G047TO50_MONTH.tcl'
,'BASS1_INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl'
,'BASS1_INT_CHECK_L0_TO_DAY.tcl'
,'BASS1_INT_CHECK_L1K9_TO_DAY.tcl'
,'BASS1_INT_CHECK_L2_TO_DAY.tcl'
,'BASS1_INT_CHECK_L34_MONTH.tcl'
) 
and control_code not in 
('BASS1_INT_CHECK_F1_TO_DAY.tcl','BASS1_INT_CHECK_L2_TO_DAY.tcl','BASS1_INT_CHECK_C567_DAY.tcl')

--BASS1_EXP_G_A_02061_DAY


SELECT * FROM bass1.MON_ALL_INTERFACE  t
where t


select * from   app.sch_control_before
where control_code in 
('BASS1_EXP_G_S_21004_DAY','BASS1_EXP_G_S_21001_DAY','BASS1_EXP_G_S_21009_DAY')

BASS1_EXP_G_S_21001_DAY	BASS1_INT_CHECK_C567_DAY.tcl
BASS1_EXP_G_S_21004_DAY	BASS1_INT_CHECK_C567_DAY.tcl
BASS1_EXP_G_S_21009_DAY	BASS1_INT_CHECK_C567_DAY.tcl


delete from app.sch_control_before
where control_code = 'BASS1_EXP_G_S_21001_DAY'
and before_control_code = 'BASS1_INT_CHECK_C567_DAY.tcl'

delete from app.sch_control_before
where control_code = 'BASS1_EXP_G_S_21004_DAY'
and before_control_code = 'BASS1_INT_CHECK_C567_DAY.tcl'


delete from app.sch_control_before
where control_code = 'BASS1_EXP_G_S_21009_DAY'
and before_control_code = 'BASS1_INT_CHECK_C567_DAY.tcl'

select * from   app.sch_control_runlog 
where  flag = 1


select * from   app.sch_control_runlog 
where control_code like '%BASS1_EXP%'
and flag = 1


select count(0) from    G_S_21004_DAY where time_id = 20110418
select count(0) from    G_S_21001_DAY where time_id = 20110418
select count(0) from    G_S_21009_DAY where time_id = 20110418


select time_id,count(0)
from G_S_21009_DAY
where time_id > 20110101
group by time_id


0	44
10	15
12	25
13	2
14	911

0	1
9	5
10	88
11	24
12	14
13	7
14	3321

--Ŀ���
CREATE TABLE BASS1.t_grp_id_old_new_map
 (
	 area_id            		INTEGER              ----��������        
	,OLD_ENTERPRISE_ID      		CHAR(20)             ----old���ſͻ���ʶ    
	,NEW_ENTERPRISE_ID      		CHAR(20)            ----new���ſͻ���ʶ    
	,ENTERPRISE_NAME    		CHAR(60)       								----���ſͻ�����*    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (OLD_ENTERPRISE_ID,NEW_ENTERPRISE_ID) USING HASHING;

ALTER TABLE BASS1.t_grp_id_old_new_map
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
where  length(trim(NEW_ENTERPRISE_ID)) = 14
and NEW_ENTERPRISE_ID like '8%'
and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_ID

where not( length(trim(NEW_ENTERPRISE_ID)) = 14
and NEW_ENTERPRISE_ID like '8%'
and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_ID
)
 CREATE TABLE BASS1.grp_id_old_new_map_20110330 like BASS1.t_grp_id_old_new_map
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (OLD_ENTERPRISE_ID,NEW_ENTERPRISE_ID) USING HASHING;

ALTER TABLE BASS1.grp_id_old_new_map_20110330
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

delete from  BASS1.grp_id_old_new_map_20110330
insert into   BASS1.grp_id_old_new_map_20110330
select 
         AREA_ID
        ,OLD_ENTERPRISE_ID
        ,NEW_ENTERPRISE_ID
        ,ENTERPRISE_NAME
from (        
select a.*,row_number()over(partition by OLD_ENTERPRISE_ID order by ENTERPRISE_NAME desc ) rn  from   BASS1.t_grp_id_old_new_map a
where  length(trim(NEW_ENTERPRISE_ID)) = 14
and NEW_ENTERPRISE_ID like '8%'
and OLD_ENTERPRISE_ID <> NEW_ENTERPRISE_ID
) t where rn = 1



('89102999530006'
,'89102999674130'
,'89102999677848'
,'89101560000227'
,'89103001129200'
,'89103001297331'
,'89102999719736'
,'89101560001881'
,'89103001217795'
,'89103000988259'
,'89102999676402'
,'89108911009013'
,'89103001217855'
,'89102999958291'
,'89103001340362'
,'89102999677953'
,'89101560000042'
,'89102999719613'
,'89103000115958'
,'89103001212607'
,'89101560000470'
,'89102999038707'
,'89102999677751'
,'89101560001694'
,'89101560000964'
,'89102999719542'
('89102999530006'
,'89102999674130'
,'89102999677848'
,'89101560000227'
,'89103001129200'
,'89103001297331'
,'89102999719736'
,'89101560001881'
,'89103001217795'
,'89103000988259'
,'89102999676402'
,'89108911009013'
,'89103001217855'
,'89102999958291'
,'89103001340362'
,'89102999677953'
,'89101560000042'
,'89102999719613'
,'89103000115958'
,'89103001212607'
,'89101560000470'
,'89102999038707'
,'89102999677751'
,'89101560001694'
,'89101560000964'
,'89102999719542'
select '89102999530006' enterprise_id from bass2.dual union all
select '89102999674130' enterprise_id from bass2.dual union all
select '89102999677848' enterprise_id from bass2.dual union all
select '89101560000227' enterprise_id from bass2.dual union all
select '89103001129200' enterprise_id from bass2.dual union all
select '89103001297331' enterprise_id from bass2.dual union all
select '89102999719736' enterprise_id from bass2.dual union all
select '89101560001881' enterprise_id from bass2.dual union all
select '89103001217795' enterprise_id from bass2.dual union all
select '89103000988259' enterprise_id from bass2.dual union all
select '89102999676402' enterprise_id from bass2.dual union all
select '89108911009013' enterprise_id from bass2.dual union all
select '89103001217855' enterprise_id from bass2.dual union all
select '89102999958291' enterprise_id from bass2.dual union all
select '89103001340362' enterprise_id from bass2.dual union all
select '89102999677953' enterprise_id from bass2.dual union all
select '89101560000042' enterprise_id from bass2.dual union all
select '89102999719613' enterprise_id from bass2.dual union all
select '89103000115958' enterprise_id from bass2.dual union all
select '89103001212607' enterprise_id from bass2.dual union all
select '89101560000470' enterprise_id from bass2.dual union all
select '89102999038707' enterprise_id from bass2.dual union all
select '89102999677751' enterprise_id from bass2.dual union all
select '89101560001694' enterprise_id from bass2.dual union all
select '89101560000964' enterprise_id from bass2.dual union all
select '89102999719542' enterprise_id from bass2.dual 
) t
select '89102999530006' enterprise_id from bass2.dual union all
select '89102999674130' enterprise_id from bass2.dual union all
select '89102999677848' enterprise_id from bass2.dual union all
select '89101560000227' enterprise_id from bass2.dual union all
select '89103001129200' enterprise_id from bass2.dual union all
select '89103001297331' enterprise_id from bass2.dual union all
select '89102999719736' enterprise_id from bass2.dual union all
select '89101560001881' enterprise_id from bass2.dual union all
select '89103001217795' enterprise_id from bass2.dual union all
select '89103000988259' enterprise_id from bass2.dual union all
select '89102999676402' enterprise_id from bass2.dual union all
select '89108911009013' enterprise_id from bass2.dual union all
select '89103001217855' enterprise_id from bass2.dual union all
select '89102999958291' enterprise_id from bass2.dual union all
select '89103001340362' enterprise_id from bass2.dual union all
select '89102999677953' enterprise_id from bass2.dual union all
select '89101560000042' enterprise_id from bass2.dual union all
select '89102999719613' enterprise_id from bass2.dual union all
select '89103000115958' enterprise_id from bass2.dual union all
select '89103001212607' enterprise_id from bass2.dual union all
select '89101560000470' enterprise_id from bass2.dual union all
select '89102999038707' enterprise_id from bass2.dual union all
select '89102999677751' enterprise_id from bass2.dual union all
select '89101560001694' enterprise_id from bass2.dual union all
select '89101560000964' enterprise_id from bass2.dual union all
select '89102999719542' enterprise_id from bass2.dual 
) t
select t.*,case when b.ENTERPRISE_ID is not null then 1 else 0 end ifok from table(
select '1','89102999530006' enterprise_id from bass2.dual union all
select '2','89102999674130' enterprise_id from bass2.dual union all
select '3','89102999677848' enterprise_id from bass2.dual union all
select '4','89101560000227' enterprise_id from bass2.dual union all
select '5','89103001129200' enterprise_id from bass2.dual union all
select '6','89103001297331' enterprise_id from bass2.dual union all
select '7','89102999719736' enterprise_id from bass2.dual union all
select '8','89101560001881' enterprise_id from bass2.dual union all
select '9','89103001217795' enterprise_id from bass2.dual union all
select '10','89103000988259' enterprise_id from bass2.dual union all
select '11','89102999676402' enterprise_id from bass2.dual union all
select '12','89108911009013' enterprise_id from bass2.dual union all
select '13','89103001217855' enterprise_id from bass2.dual union all
select '14','89102999958291' enterprise_id from bass2.dual union all
select '15','89103001340362' enterprise_id from bass2.dual union all
select '16','89102999677953' enterprise_id from bass2.dual union all
select '17','89101560000042' enterprise_id from bass2.dual union all
select '18','89102999719613' enterprise_id from bass2.dual union all
select '19','89103000115958' enterprise_id from bass2.dual union all
select '20','89103001212607' enterprise_id from bass2.dual union all
select '21','89101560000470' enterprise_id from bass2.dual union all
select '22','89102999038707' enterprise_id from bass2.dual union all
select '23','89102999677751' enterprise_id from bass2.dual union all
select '24','89101560001694' enterprise_id from bass2.dual union all
select '25','89101560000964' enterprise_id from bass2.dual union all
select '26','89102999719542' enterprise_id from bass2.dual 
) t
left join  G_I_77780_DAY_DOWN20110415 b 
on  t.enterprise_id = b.enterprise_id
order by 1
select t.*,case when b.ENTERPRISE_ID is not null then 1 else 0 end ifok from table(
select 1,'89102999530006' enterprise_id from bass2.dual union all
select 2,'89102999674130' enterprise_id from bass2.dual union all
select 3,'89102999677848' enterprise_id from bass2.dual union all
select 4,'89101560000227' enterprise_id from bass2.dual union all
select 5,'89103001129200' enterprise_id from bass2.dual union all
select 6,'89103001297331' enterprise_id from bass2.dual union all
select 7,'89102999719736' enterprise_id from bass2.dual union all
select 8,'89101560001881' enterprise_id from bass2.dual union all
select 9,'89103001217795' enterprise_id from bass2.dual union all
select 10,'89103000988259' enterprise_id from bass2.dual union all
select 11,'89102999676402' enterprise_id from bass2.dual union all
select 12,'89108911009013' enterprise_id from bass2.dual union all
select 13,'89103001217855' enterprise_id from bass2.dual union all
select 14,'89102999958291' enterprise_id from bass2.dual union all
select 15,'89103001340362' enterprise_id from bass2.dual union all
select 16,'89102999677953' enterprise_id from bass2.dual union all
select 17,'89101560000042' enterprise_id from bass2.dual union all
select 18,'89102999719613' enterprise_id from bass2.dual union all
select 19,'89103000115958' enterprise_id from bass2.dual union all
select 20,'89103001212607' enterprise_id from bass2.dual union all
select 21,'89101560000470' enterprise_id from bass2.dual union all
select 22,'89102999038707' enterprise_id from bass2.dual union all
select 23,'89102999677751' enterprise_id from bass2.dual union all
select 24,'89101560001694' enterprise_id from bass2.dual union all
select 25,'89101560000964' enterprise_id from bass2.dual union all
select 26,'89102999719542' enterprise_id from bass2.dual 
) t
left join  G_I_77780_DAY_DOWN20110415 b 
on  t.enterprise_id = b.enterprise_id
order by 1
select t.*,case when b.ENTERPRISE_ID is not null then 1 else 0 end ifok
select 1,'89102999530006' enterprise_id from bass2.dual union all
select 2,'89102999674130' enterprise_id from bass2.dual union all
select 3,'89102999677848' enterprise_id from bass2.dual union all
select 4,'89101560000227' enterprise_id from bass2.dual union all
select 5,'89103001129200' enterprise_id from bass2.dual union all
select 6,'89103001297331' enterprise_id from bass2.dual union all
select 7,'89102999719736' enterprise_id from bass2.dual union all
select 8,'89101560001881' enterprise_id from bass2.dual union all
select 9,'89103001217795' enterprise_id from bass2.dual union all
select 10,'89103000988259' enterprise_id from bass2.dual union all
select 11,'89102999676402' enterprise_id from bass2.dual union all
select 12,'89108911009013' enterprise_id from bass2.dual union all
select 13,'89103001217855' enterprise_id from bass2.dual union all
select 14,'89102999958291' enterprise_id from bass2.dual union all
select 15,'89103001340362' enterprise_id from bass2.dual union all
select 16,'89102999677953' enterprise_id from bass2.dual union all
select 17,'89101560000042' enterprise_id from bass2.dual union all
select 18,'89102999719613' enterprise_id from bass2.dual union all
select 19,'89103000115958' enterprise_id from bass2.dual union all
select 20,'89103001212607' enterprise_id from bass2.dual union all
select 21,'89101560000470' enterprise_id from bass2.dual union all
select 22,'89102999038707' enterprise_id from bass2.dual union all
select 23,'89102999677751' enterprise_id from bass2.dual union all
select 24,'89101560001694' enterprise_id from bass2.dual union all
select 25,'89101560000964' enterprise_id from bass2.dual union all
select 26,'89102999719542' enterprise_id from bass2.dual 
) t
left join  G_I_77780_DAY b 
on  t.enterprise_id = b.enterprise_id
order by 1


select 1		sn ,'89100000000781' ent_id from bass2.dual  union all
select 2		sn ,'89102999678267' ent_id from bass2.dual  union all
select 3		sn ,'89100000000705' ent_id from bass2.dual  union all
select 4		sn ,'89102999683759' ent_id from bass2.dual  union all
select 6		sn ,'89103000946057' ent_id from bass2.dual  union all
select 7		sn ,'89103000064017' ent_id from bass2.dual  union all
select 8		sn ,'89100000001994' ent_id from bass2.dual  union all
select 9		sn ,'89102999435065' ent_id from bass2.dual  union all
select 10	sn ,'89103000621702' ent_id from bass2.dual    union all
select 11	sn ,'89100000000706' ent_id from bass2.dual    union all
select 12	sn ,'89100000001201' ent_id from bass2.dual    union all
select 13	sn ,'89100000001059' ent_id from bass2.dual    union all
select 14	sn ,'89100000000745' ent_id from bass2.dual    union all
select 15	sn ,'89103001622602' ent_id from bass2.dual    union all
select 16	sn ,'89103001458963' ent_id from bass2.dual    union all
select 17	sn ,'89103000617612' ent_id from bass2.dual    union all
select 18	sn ,'89103000552694' ent_id from bass2.dual    union all
select 19	sn ,'89102999506974' ent_id from bass2.dual    union all
select 20	sn ,'89103001245590' ent_id from bass2.dual    union all
select 21	sn ,'89103001406741' ent_id from bass2.dual    union all
select 22	sn ,'89103000146038' ent_id from bass2.dual    union all
select 23	sn ,'89100000000985' ent_id from bass2.dual    union all
select 24	sn ,'89103001401858' ent_id from bass2.dual    union all
select 25	sn ,'89103001479683' ent_id from bass2.dual    union all
select 26	sn ,'89103001526257' ent_id from bass2.dual    union all
select 27	sn ,'89102999492029' ent_id from bass2.dual    union all
select 28	sn ,'89103000178395' ent_id from bass2.dual    union all
select 29	sn ,'89100000003591' ent_id from bass2.dual    union all
select 30	sn ,'89100000001180' ent_id from bass2.dual    union all
select 31	sn ,'89100000003760' ent_id from bass2.dual    union all
select 32	sn ,'89100000003870' ent_id from bass2.dual    union all
select 33	sn ,'89102999541900' ent_id from bass2.dual    union all
select 34	sn ,'89103000138225' ent_id from bass2.dual    union all
select 35	sn ,'89103001127423' ent_id from bass2.dual    union all
select 36	sn ,'89102999386292' ent_id from bass2.dual    union all
select 37	sn ,'89103001243985' ent_id from bass2.dual    union all
select 38	sn ,'89101560001495' ent_id from bass2.dual    union all
select 39	sn ,'89103000994233' ent_id from bass2.dual    union all
select 40	sn ,'89103000987125' ent_id from bass2.dual    union all
select 41	sn ,'89101560000477' ent_id from bass2.dual    union all
select 42	sn ,'89103001378490' ent_id from bass2.dual    union all
select 43	sn ,'89100000001227' ent_id from bass2.dual    union all
select 44	sn ,'89102999790622' ent_id from bass2.dual    union all
select 45	sn ,'89100000001017' ent_id from bass2.dual    union all
select 46	sn ,'89101560000743' ent_id from bass2.dual    union all
select 47	sn ,'89102999567199' ent_id from bass2.dual    union all
select 48	sn ,'89102999994032' ent_id from bass2.dual    union all
select 49	sn ,'89103001186111' ent_id from bass2.dual    union all
select 50	sn ,'89101560000568' ent_id from bass2.dual    union all
select 51	sn ,'89103000980688' ent_id from bass2.dual    union all
select 52	sn ,'89103000137752' ent_id from bass2.dual    union all
select 53	sn ,'89101560001721' ent_id from bass2.dual    union all
select 54	sn ,'89103001312386' ent_id from bass2.dual    union all
select 55	sn ,'89103001185005' ent_id from bass2.dual    union all
select 56	sn ,'89103000082555' ent_id from bass2.dual    union all
select 57	sn ,'89103000150831' ent_id from bass2.dual    union all
select 58	sn ,'89103000760867' ent_id from bass2.dual    union all
select 59	sn ,'89100000003531' ent_id from bass2.dual    union all
select 60	sn ,'89100000003719' ent_id from bass2.dual    union all
select 61	sn ,'89103000730481' ent_id from bass2.dual    union all
select 62	sn ,'89103000024493' ent_id from bass2.dual    union all
select 63	sn ,'89102999961453' ent_id from bass2.dual    union all
select 64	sn ,'89102999704905' ent_id from bass2.dual    union all
select 65	sn ,'89100000001202' ent_id from bass2.dual    union all
select 66	sn ,'89100000000795' ent_id from bass2.dual    union all
select 67	sn ,'89103001561167' ent_id from bass2.dual    union all
select 68	sn ,'89100000001209' ent_id from bass2.dual    union all
select 69	sn ,'89100000001107' ent_id from bass2.dual    union all
select 70	sn ,'89103001511691' ent_id from bass2.dual    union all
select 71	sn ,'89102999540922' ent_id from bass2.dual    union all
select 72	sn ,'89100000003865' ent_id from bass2.dual    union all
select 73	sn ,'89103001217307' ent_id from bass2.dual    union all
select 74	sn ,'89103001123440' ent_id from bass2.dual    union all
select 75	sn ,'89103001247755' ent_id from bass2.dual    union all
select 76	sn ,'89103001018443' ent_id from bass2.dual    union all
select 77	sn ,'89102999484031' ent_id from bass2.dual    union all
select 78	sn ,'89103001410733' ent_id from bass2.dual    union all
select 79	sn ,'89102999040885' ent_id from bass2.dual    union all
select 80	sn ,'89103001560707' ent_id from bass2.dual    union all
select 81	sn ,'89103001555680' ent_id from bass2.dual    union all
select 82	sn ,'89103001410717' ent_id from bass2.dual    union all
select 83	sn ,'89103001478666' ent_id from bass2.dual    union all
select 84	sn ,'89100000000880' ent_id from bass2.dual    union all
select 85	sn ,'89103000604297' ent_id from bass2.dual    union all
select 86	sn ,'89103001215411' ent_id from bass2.dual    union all
select 87	sn ,'89101560001288' ent_id from bass2.dual    
) t 
(select 1		,'89101560000071' ent_id from bass2.dual union all
select 2		,'89103001266375' ent_id from bass2.dual union all 
select 3		,'89103001112300' ent_id from bass2.dual union all 
select 4		,'89103000225049' ent_id from bass2.dual union all 
select 5		,'89103001221427' ent_id from bass2.dual union all
select 6		,'89103000195207' ent_id from bass2.dual union all
select 7		,'89103001211641' ent_id from bass2.dual union all
select 8		,'89103001241135' ent_id from bass2.dual union all
select 9		,'89103000608698' ent_id from bass2.dual union all
select 10	,'89103001221379'   ent_id from bass2.dual union all
select 11	,'89103001221422'   ent_id from bass2.dual union all
select 12	,'89103000545529'   ent_id from bass2.dual union all
select 13	,'89102999353706'   ent_id from bass2.dual union all
select 14	,'89103000924607'   ent_id from bass2.dual union all
select 15	,'89101560001876'   ent_id from bass2.dual union all
select 16	,'89103000138204'   ent_id from bass2.dual union all
select 17	,'89103000936986'   ent_id from bass2.dual union all
select 18	,'89102999478101'   ent_id from bass2.dual union all
select 19	,'89101560000501'   ent_id from bass2.dual union all
select 20	,'89103001113778'   ent_id from bass2.dual union all
select 21	,'89103000990159'   ent_id from bass2.dual union all
select 22	,'89101560001384'   ent_id from bass2.dual union all
select 23	,'89102999987787'   ent_id from bass2.dual union all
select 24	,'89101560001460'   ent_id from bass2.dual union all
select 25	,'89102999386322'   ent_id from bass2.dual union all
select 26	,'89103001396413'   ent_id from bass2.dual union all
select 27	,'89103001061802'   ent_id from bass2.dual union all
select 28	,'89103001051735'   ent_id from bass2.dual union all
select 29	,'89103001245853'   ent_id from bass2.dual 
) t
(select 1		,'89101560000071' ent_id from bass2.dual union all
select 2		,'89103001266375' ent_id from bass2.dual union all 
select 3		,'89103001112300' ent_id from bass2.dual union all 
select 4		,'89103000225049' ent_id from bass2.dual union all 
select 5		,'89103001221427' ent_id from bass2.dual union all
select 6		,'89103000195207' ent_id from bass2.dual union all
select 7		,'89103001211641' ent_id from bass2.dual union all
select 8		,'89103001241135' ent_id from bass2.dual union all
select 9		,'89103000608698' ent_id from bass2.dual union all
select 10	,'89103001221379'   ent_id from bass2.dual union all
select 11	,'89103001221422'   ent_id from bass2.dual union all
select 12	,'89103000545529'   ent_id from bass2.dual union all
select 13	,'89102999353706'   ent_id from bass2.dual union all
select 14	,'89103000924607'   ent_id from bass2.dual union all
select 15	,'89101560001876'   ent_id from bass2.dual union all
select 16	,'89103000138204'   ent_id from bass2.dual union all
select 17	,'89103000936986'   ent_id from bass2.dual union all
select 18	,'89102999478101'   ent_id from bass2.dual union all
select 19	,'89101560000501'   ent_id from bass2.dual union all
select 20	,'89103001113778'   ent_id from bass2.dual union all
select 21	,'89103000990159'   ent_id from bass2.dual union all
select 22	,'89101560001384'   ent_id from bass2.dual union all
select 23	,'89102999987787'   ent_id from bass2.dual union all
select 24	,'89101560001460'   ent_id from bass2.dual union all
select 25	,'89102999386322'   ent_id from bass2.dual union all
select 26	,'89103001396413'   ent_id from bass2.dual union all
select 27	,'89103001061802'   ent_id from bass2.dual union all
select 28	,'89103001051735'   ent_id from bass2.dual union all
select 29	,'89103001245853'   ent_id from bass2.dual 
) t


select sum(income)*1.00/100
from (
select sum(bigint(income)) income from   g_s_03017_month
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'
union all 
select sum(bigint(income)) income from   g_s_03018_month
where time_id = 201103
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
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=1
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'
where time_id = 201103
and ent_busi_id = '1220'
select sum(income)*1.00/100
from (
select sum(bigint(income)) income from   g_s_03017_month
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'
union all 
select sum(bigint(income)) income from   g_s_03018_month
where time_id = 201103
and manage_mod = '2'
and ent_busi_id = '1220'
) t

select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,15),b.control_code from    
) t where unit_code = '02022'

                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1

select count(*) from bass2.dw_product_200907 a,
(
select user_id,region_flag
from
(
select time_id,user_id,region_flag
        ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.G_A_02052_MONTH
where time_id<=200907
) a
where row_id=1
  and region_flag='3'
) b,
bass2.trans_user_id_20100625 c
where b.user_id=c.new_USER_ID
  and a.user_id = c.USER_ID
  and a.usertype_id in (1,2,9) 
  and a.userstatus_id in (1,2,3,6,8)
  and a.test_mark<>1

select '200902' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200902 where LOCNTYPE_ID = 3 union all
select '200903' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200903 where LOCNTYPE_ID = 3 union all
select '200904' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200904 where LOCNTYPE_ID = 3 union all
select '200905' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200905 where LOCNTYPE_ID = 3 union all
select '200906' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200906 where LOCNTYPE_ID = 3 union all
select '200907' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200907 where LOCNTYPE_ID = 3 union all
select '200908' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200908 where LOCNTYPE_ID = 3 union all
select '200909' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200909 where LOCNTYPE_ID = 3 union all
select '200910' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200910 where LOCNTYPE_ID = 3 union all
select '200911' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200911 where LOCNTYPE_ID = 3 union all
select '200912' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_200912 where LOCNTYPE_ID = 3 union all
select '201001' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201001 where LOCNTYPE_ID = 3 union all
select '201002' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201002 where LOCNTYPE_ID = 3 union all
select '201003' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201003 where LOCNTYPE_ID = 3 union all
select '201004' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201004 where LOCNTYPE_ID = 3 union all
select '201005' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201005 where LOCNTYPE_ID = 3 union all
select '201006' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201006 where LOCNTYPE_ID = 3 union all
select '201007' m ,count(0) from  bass2.STAT_ZD_VILLAGE_USERS_201007 where LOCNTYPE_ID = 3 
           
 (
	 TIME_ID            	INTEGER             ----��¼�к�        
	,OP_TIME            	CHAR(8)             ----���� ����       
	,QRY_CNT            	CHAR(12)            ----��ѯ�� ��λ���� 
	,CANCEL_CNT         	CHAR(12)            ----�˶��� ��λ���� 
	,CANCEL_FAIL_CNT    	CHAR(12)            ----�˶�ʧ���� ��λ����
	,COMPLAINT_CNT      	CHAR(12)            ----Ͷ���� ��λ���� 
	,ALL_CANCEL_CNT     	CHAR(12)            ----�����˶�ҵ������ ��λ����
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22080_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,13) from    
) t where unit_code = '22012'
select * from table(
select substr(control_code , 11,5) unit_code,substr(b.CONTROL_CODE,7,15),b.control_code from    
) t where unit_code = '21006'


 G_S_21003_MONTH

201103	680378057



('891910006907'
,'891910006537'
,'891910005097'
,'891910005160'
,'891910006292'
,'891910005743'
,'891910006739'
,'891910006669')

 (
	 TIME_ID            		INTEGER        NOT NULL       ----��������        
	,ENTERPRISE_ID      		CHAR(20)       					       ----���ſͻ���ʶ    
	,ID                 		CHAR(9)        								----ID*              
	,ENTERPRISE_NAME    		CHAR(60)       								----���ſͻ�����*    
	,ORG_TYPE           		CHAR(5)             					----�������� 							BASS_STD_0001       
	,ADDR_CODE          		CHAR(6)             					----��ַ����*        
	,CITY               		CHAR(20)            					----���е���*        
	,REGION             		CHAR(20)            					----����*            
	,COUNTY             		CHAR(20)            					----����*            
	,DOOR_NO            		CHAR(60)            					----����*            
	,AREA_CODE          		CHAR(5)             					----����*            
	,PHONE_NO1          		CHAR(11)            					----�绰1*           
	,PHONE_NO2          		CHAR(10)            					----�绰2*           
	,POST_CODE          		CHAR(6)             					----��������*        
	,INDUSTRY_TYPE      		CHAR(4)             					----��ҵ���� 							BASS_STD1_0113       
	,EMPLOYEE_CNT       		CHAR(8)             					----ְԱ            
	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----��ҵ��λ��*      
	,ECONOMIC_TYPE      		CHAR(3)             					----�������� 							BASS_STD_0002       
	,OPEN_YEAR          		CHAR(4)             					----��ҵ1           
	,OPEN_MONTH         		CHAR(2)             					----��ҵ2           
	,SHAREHOLDER        		CHAR(1)             					----�ع�  								BASS_STD_0005          
	,GROUP_TYPE         		CHAR(1)             					----�������� 							BASS_STD_0003       
	,MANAGE_STYLE       		CHAR(1)             					----��Ӫ��ʽ      				BASS_STD_0004  
	,OPERATE_REVENUE_CLASS	CHAR(2)           						----Ӫҵ������� 					BASS_STD_0006   
	,CAPITAL_CLASS      		CHAR(2)           						----�ʲ�����     					BASS_STD_0007   
	,INDUSTRY_CLASS_CODE		CHAR(2)    NOT NULL         	----��ҵ������� 					BASS_STD1_0043   
	,CUST_STATUS        		CHAR(2)             					----���ſͻ�״̬    
	,CUST_INFO_SRC_ID   		CHAR(1)             					----���ſͻ�������Դ
	,UPLOAD_TYPE_ID     		CHAR(1)    NOT NULL         	----�ϴ������ʶ    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY_b20110421
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
insert into  BASS1.G_I_77780_DAY_b20110421
select * from BASS1.G_I_77780_DAY

drop table  BASS1.G_I_77780_DAY_MID_a ;
CREATE TABLE BASS1.G_I_77780_DAY_MID_a (
	 TIME_ID            		INTEGER               ----��������        
	,ENTERPRISE_ID      		CHAR(20)              ----���ſͻ���ʶ    
	,ID                 		CHAR(9)        								----ID*              
	,ENTERPRISE_NAME    		CHAR(60)       								----���ſͻ�����*    
	,ORG_TYPE           		CHAR(5)             					----�������� 							BASS_STD_0001       
		--ADDR_CODE���淶6 CHAR(6) 
	,ADDR_CODE          		VARCHAR(10)             					----��ַ����*        
	,CITY               		CHAR(20)            					----���е���*        
	---REGION ���淶 9           		CHAR(20) 
	,REGION             		VARCHAR(100)            					----����*            
	---COUNTY ���淶 10            		CHAR(20) 	
	,COUNTY             		VARCHAR(100)            					----����*            
	,DOOR_NO            		CHAR(60)            					----����*            
	,AREA_CODE          		CHAR(5)             					----����*            
	--PHONE_NO1 ���淶  12       		CHAR(11)
	,PHONE_NO1          		varCHAR(111)            					----�绰1*           
	--,PHONE_NO2  ���淶  13        		CHAR(10) 
	,PHONE_NO2          		VARCHAR(110)            					----�绰2*           
	--POST_CODE ���淶  14         		CHAR(6)
	,POST_CODE          		varCHAR(16)             					----��������*        
--	,INDUSTRY_TYPE  ���淶  15    		CHAR(4) 
	,INDUSTRY_TYPE      		VARCHAR(14)             					----��ҵ���� 							BASS_STD1_0113       
	,EMPLOYEE_CNT       		CHAR(8)             					----ְԱ            
	,INDUSTRY_UNIT_CNT  		CHAR(5)             					----��ҵ��λ��*      
	--ECONOMIC_TYPE  ���淶  18    		CHAR(3)
	,ECONOMIC_TYPE      		CHAR(13)             					----�������� 							BASS_STD_0002       
	--OPEN_YEAR  ���淶  19        		CHAR(4)
	,OPEN_YEAR          		varCHAR(14)             					----��ҵ1           
	--OPEN_MONTH ���淶  20        		CHAR(2) 
	,OPEN_MONTH         		varCHAR(22)             					----��ҵ2           
	--SHAREHOLDER���淶CHAR(1) 
	,SHAREHOLDER        		VARCHAR(4)             					----�ع�  								BASS_STD_0005          
	--GROUP_TYPE���淶CHAR(1) 
	,GROUP_TYPE         		VARCHAR(2)             					----�������� 							BASS_STD_0003       
	--MANAGE_STYLE���淶CHAR(1) 
	,MANAGE_STYLE       		VARCHAR(4)             					----��Ӫ��ʽ      				BASS_STD_0004  
	,OPERATE_REVENUE_CLASS	CHAR(2)           						----Ӫҵ������� 					BASS_STD_0006   
	,CAPITAL_CLASS      		CHAR(2)           						----�ʲ�����     					BASS_STD_0007   
	,INDUSTRY_CLASS_CODE		CHAR(2)            	----��ҵ������� 					BASS_STD1_0043   
	,CUST_STATUS        		CHAR(2)             					----���ſͻ�״̬    
	,CUST_INFO_SRC_ID   		CHAR(1)             					----���ſͻ�������Դ
	,UPLOAD_TYPE_ID     		CHAR(1)            	----�ϴ������ʶ    
)
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,ENTERPRISE_ID) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY_MID_a
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
  
 (
	 interface_code       CHAR(5)            
	,dim_table_id      		CHAR(20)       NOT NULL      
	,code                 CHAR(9)        NOT NULL				       
	,code_name    		    CHAR(60)       NOT NULL											
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (dim_table_id,code) USING HASHING;

ALTER TABLE BASS1.dim_bass1_std_map
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY_MID2
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;


insert into bass1.G_I_77780_DAY_MID2
select 
         TIME_ID
        ,ENTERPRISE_ID
        ,ID
        ,ENTERPRISE_NAME
        ,ORG_TYPE
      ,case when ADDR_CODE is null or length(ADDR_CODE) <> 6 then '540102' else ADDR_CODE end ADDR_CODE
        ,CITY
        ,substr(REGION,1,20) REGION
        ,substr(COUNTY,1,20) COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,replace(PHONE_NO1,'-','') PHONE_NO1
        ,substr(replace(PHONE_NO2,'-',''),1,10) PHONE_NO2
        ,case when POST_CODE is null or length(POST_CODE) <> 6 then '850000' else POST_CODE end POST_CODE
        ,case when INDUSTRY_TYPE is null or length(INDUSTRY_TYPE) <> 4 then '9421' else INDUSTRY_TYPE end INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,case when ECONOMIC_TYPE is null or length(trim(ECONOMIC_TYPE)) <> 3 then '190' else trim(ECONOMIC_TYPE) end ECONOMIC_TYPE
        ,case when OPEN_YEAR is null then '2002' when  length(OPEN_YEAR) <> 4  then substr(OPEN_YEAR,1,4) else OPEN_YEAR end OPEN_YEAR
        ,case when length(OPEN_MONTH) =1 then '0'||trim(OPEN_MONTH) when length(OPEN_MONTH) <> 2 then '01' else OPEN_MONTH end OPEN_MONTH
        ,case when SHAREHOLDER is null then '9' 
        	when SHAREHOLDER not in ('1','2','3','4','5') then '9' else SHAREHOLDER end SHAREHOLDER
        ,case when GROUP_TYPE not in ('1','2') then '2' else GROUP_TYPE end GROUP_TYPE
        ,case when MANAGE_STYLE is null then '9' 
        	when length(MANAGE_STYLE) = 2 and  substr(MANAGE_STYLE,2,1) in ('1','2','3') then substr(MANAGE_STYLE,2,1)
        	when length(MANAGE_STYLE) = 1 and  MANAGE_STYLE in ('1','2','3') then MANAGE_STYLE 
      		else '9' end MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
      ,case when INDUSTRY_CLASS_CODE is null then '99'  else INDUSTRY_CLASS_CODE end INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
      ,case when UPLOAD_TYPE_ID is null then '3' else UPLOAD_TYPE_ID end UPLOAD_TYPE_ID
from bass1.G_I_77780_DAY_MID_a a

create table bass1.G_I_77780_DAY_MID3 like bass1.G_I_77780_DAY
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,ID) USING HASHING;
   

				 join  BASS1.grp_id_old_new_map_20110330  b on a.ENTERPRISE_ID = b.old_enterprise_id

delete from bass1.G_I_77780_DAY_MID3;
insert into bass1.G_I_77780_DAY_MID3
select 
         TIME_ID
        ,NEW_ENTERPRISE_ID
        ,ID
        ,NEW_ENTERPRISE_NAME
        ,ORG_TYPE
        ,ADDR_CODE
        ,CITY
        ,REGION
        ,COUNTY
        ,DOOR_NO
        ,AREA_CODE
        ,PHONE_NO1
        ,PHONE_NO2
        ,POST_CODE
        ,INDUSTRY_TYPE
        ,EMPLOYEE_CNT
        ,INDUSTRY_UNIT_CNT
        ,ECONOMIC_TYPE
        ,OPEN_YEAR
        ,OPEN_MONTH
        ,SHAREHOLDER
        ,GROUP_TYPE
        ,MANAGE_STYLE
        ,OPERATE_REVENUE_CLASS
        ,CAPITAL_CLASS
        ,INDUSTRY_CLASS_CODE
        ,CUST_STATUS
        ,CUST_INFO_SRC_ID
        ,UPLOAD_TYPE_ID
from   (select a.* 
		,VALUE(b.new_enterprise_id,a.ENTERPRISE_ID) NEW_ENTERPRISE_ID
		,VALUE(b.ENTERPRISE_NAME,a.ENTERPRISE_NAME) NEW_ENTERPRISE_NAME
		, row_number()over(PARTITION by ENTERPRISE_ID,id order by  UPLOAD_TYPE_ID asc ) rn 
				from  bass1.G_I_77780_DAY_MID2 a
				left join  BASS1.grp_id_old_new_map_20110330  b on a.ENTERPRISE_ID = b.old_enterprise_id
			 ) t where t.rn = 1 

from  bass1.G_I_77780_DAY_MID3

 
89102999676402      XZ0309285	2	1

from  bass1.G_I_77780_DAY_MID3

from 
(select distinct ORG_TYPE ORG_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ORG_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0001')
select count(0)
from 
(select distinct ORG_TYPE ORG_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ORG_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0001')



drop table BASS1.t_bass1_std_0113
CREATE TABLE BASS1.t_bass1_std_0113
 (
	 code    char(4)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (code) USING HASHING;


 select count(0)
from 
(select distinct trim(INDUSTRY_TYPE) INDUSTRY_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_TYPE  not in 
(select code from BASS1.t_bass1_std_0113 )

from 
(select distinct ECONOMIC_TYPE ECONOMIC_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ECONOMIC_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD_0002')

where interface_code = '77780' and dim_table_id ='BASS_STD1_0002'
from 
(select distinct ECONOMIC_TYPE ECONOMIC_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ECONOMIC_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0002')

from 
(select distinct ECONOMIC_TYPE ECONOMIC_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ECONOMIC_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0002')

update G_I_77780_DAY_MID3 
set ECONOMIC_TYPE = '171'
where ECONOMIC_TYPE in 
(
'170'
)

where ECONOMIC_TYPE in 
('100'
,'101'
,'150'
,'170'
)


from 
(select distinct ECONOMIC_TYPE ECONOMIC_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where ECONOMIC_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0002')

select count(0)
from 
(select distinct SHAREHOLDER SHAREHOLDER from   bass1.G_I_77780_DAY_MID3) a 
where SHAREHOLDER  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0005')

select count(0)
from 
(select distinct GROUP_TYPE GROUP_TYPE from   bass1.G_I_77780_DAY_MID3) a 
where GROUP_TYPE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0003')


select count(0)
from 
(select distinct MANAGE_STYLE MANAGE_STYLE from   bass1.G_I_77780_DAY_MID3) a 
where MANAGE_STYLE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0004')

select count(0)
from 
(select distinct OPERATE_REVENUE_CLASS OPERATE_REVENUE_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where OPERATE_REVENUE_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0006')


from 
(select distinct OPERATE_REVENUE_CLASS OPERATE_REVENUE_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where OPERATE_REVENUE_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0006')


update G_I_77780_DAY_MID3 
set OPERATE_REVENUE_CLASS = '0'||OPERATE_REVENUE_CLASS
where  OPERATE_REVENUE_CLASS
from 
(select distinct OPERATE_REVENUE_CLASS OPERATE_REVENUE_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where OPERATE_REVENUE_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0006')
)

from 
(select distinct OPERATE_REVENUE_CLASS OPERATE_REVENUE_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where OPERATE_REVENUE_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0006')
)
select *
from 
(select distinct CAPITAL_CLASS CAPITAL_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where CAPITAL_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')


select *
from 
(select distinct CAPITAL_CLASS CAPITAL_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where CAPITAL_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')
)
update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '0'||CAPITAL_CLASS
where  CAPITAL_CLASS
select *
from 
(select distinct CAPITAL_CLASS CAPITAL_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where CAPITAL_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')
)
from 
(select distinct CAPITAL_CLASS CAPITAL_CLASS from   bass1.G_I_77780_DAY_MID3) a 
where CAPITAL_CLASS  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')


update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '0'||trim(CAPITAL_CLASS)
where length(trim(CAPITAL_CLASS)) = 1



update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '05'
where CAPITAL_CLASS is null 



update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '01'
where CAPITAL_CLASS = '00'

select count(0)
from 
(select distinct INDUSTRY_CLASS_CODE INDUSTRY_CLASS_CODE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_CLASS_CODE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')


select *
from 
(select distinct INDUSTRY_CLASS_CODE INDUSTRY_CLASS_CODE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_CLASS_CODE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')



update G_I_77780_DAY_MID3 
set INDUSTRY_CLASS_CODE = '0'||TRIM(INDUSTRY_CLASS_CODE)
where length(trim(INDUSTRY_CLASS_CODE)) = 1

select INDUSTRY_CLASS_CODE,count(0)
group by INDUSTRY_CLASS_CODE

update G_I_77780_DAY_MID3 
set INDUSTRY_CLASS_CODE = '08'
where 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')


select count(0)
from 
(select distinct INDUSTRY_CLASS_CODE INDUSTRY_CLASS_CODE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_CLASS_CODE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')
select *
from 
(select distinct INDUSTRY_CLASS_CODE INDUSTRY_CLASS_CODE from   bass1.G_I_77780_DAY_MID3) a 
where INDUSTRY_CLASS_CODE  not in 
(select code from BASS1.dim_bass1_std_map 
where interface_code = '77780' and dim_table_id ='BASS_STD1_0007')


update G_I_77780_DAY_MID3 
set CUST_STATUS = '11'
where  CUST_STATUS = '10'



update G_I_77780_DAY_MID3 
set CUST_INFO_SRC_ID = '1'
where  CUST_INFO_SRC_ID IS NULL 

select * from bass1.g_i_77780_day_mid3 where TIME_ID                   is null 
select * from bass1.g_i_77780_day_mid3 where ENTERPRISE_ID             is null 
select * from bass1.g_i_77780_day_mid3 where ID                        is null 
select * from bass1.g_i_77780_day_mid3 where ORG_TYPE                  is null 
select * from bass1.g_i_77780_day_mid3 where ADDR_CODE                 is null 
select * from bass1.g_i_77780_day_mid3 where CITY                      is null 
select * from bass1.g_i_77780_day_mid3 where REGION                    is null 
select * from bass1.g_i_77780_day_mid3 where COUNTY                    is null 
select * from bass1.g_i_77780_day_mid3 where DOOR_NO                   is null 
select * from bass1.g_i_77780_day_mid3 where AREA_CODE                 is null 
select * from bass1.g_i_77780_day_mid3 where PHONE_NO1                 is null 
select * from bass1.g_i_77780_day_mid3 where PHONE_NO2                 is null 
select * from bass1.g_i_77780_day_mid3 where POST_CODE                 is null 
select * from bass1.g_i_77780_day_mid3 where INDUSTRY_TYPE             is null 
select * from bass1.g_i_77780_day_mid3 where EMPLOYEE_CNT              is null 
select * from bass1.g_i_77780_day_mid3 where INDUSTRY_UNIT_CNT         is null 
select * from bass1.g_i_77780_day_mid3 where ECONOMIC_TYPE             is null 
select * from bass1.g_i_77780_day_mid3 where OPEN_YEAR                 is null 
select * from bass1.g_i_77780_day_mid3 where OPEN_MONTH                is null 
select * from bass1.g_i_77780_day_mid3 where SHAREHOLDER               is null 
select * from bass1.g_i_77780_day_mid3 where GROUP_TYPE                is null 
select * from bass1.g_i_77780_day_mid3 where MANAGE_STYLE              is null 
select * from bass1.g_i_77780_day_mid3 where OPERATE_REVENUE_CLASS     is null 
select * from bass1.g_i_77780_day_mid3 where CAPITAL_CLASS             is null 
select * from bass1.g_i_77780_day_mid3 where INDUSTRY_CLASS_CODE       is null 
select * from bass1.g_i_77780_day_mid3 where CUST_STATUS               is null 
select * from bass1.g_i_77780_day_mid3 where CUST_INFO_SRC_ID          is null 
select * from bass1.g_i_77780_day_mid3 where UPLOAD_TYPE_ID            is null 
where return_flag=1
where return_flag=1
update BASS1.G_I_77780_DAY
set CUST_STATUS = ' '
where CUST_STATUS is null 
;

update BASS1.G_I_77780_DAY
set CUST_INFO_SRC_ID = ' '
where CUST_INFO_SRC_ID is null 
;

update BASS1.G_I_77780_DAY
set UPLOAD_TYPE_ID = ' '
where UPLOAD_TYPE_ID is null 
;

update BASS1.G_I_77780_DAY
set GROUP_TYPE = ' '
where GROUP_TYPE is null 
;


update BASS1.G_I_77780_DAY
set MANAGE_STYLE = ' '
where MANAGE_STYLE is null 
;

update BASS1.G_I_77780_DAY
set OPERATE_REVENUE_CLASS = ' '
where OPERATE_REVENUE_CLASS is null 
;

update BASS1.G_I_77780_DAY
set CAPITAL_CLASS = ' '
where CAPITAL_CLASS is null 
;

update BASS1.G_I_77780_DAY
set INDUSTRY_CLASS_CODE = ' '
where INDUSTRY_CLASS_CODE is null 
;

select * from bass1.g_i_77780_day_mid3 where ENTERPRISE_ID             is null            ;
select * from bass1.g_i_77780_day_mid3 where ID                        is null            ;
--update bass1.g_i_77780_day set id = ' ' where ID                        is null         ;
select * from bass1.g_i_77780_day_mid3 where ORG_TYPE                  is null            ;
select * from bass1.g_i_77780_day_mid3 where ADDR_CODE                 is null            ;
select * from bass1.g_i_77780_day_mid3 where CITY                      is null            ;
select * from bass1.g_i_77780_day_mid3 where REGION                    is null            ;
select * from bass1.g_i_77780_day_mid3 where COUNTY                    is null            ;
--update  bass1.g_i_77780_day set COUNTY = ' ' where COUNTY    is null                    ;
select * from bass1.g_i_77780_day_mid3 where DOOR_NO                   is null            ;
--update  bass1.g_i_77780_day set DOOR_NO = ' ' where DOOR_NO  is null                    ;
select * from bass1.g_i_77780_day_mid3 where AREA_CODE                 is null            ;
select * from bass1.g_i_77780_day_mid3 where PHONE_NO1                 is null            ;
--update  bass1.g_i_77780_day set PHONE_NO1 = ' ' where PHONE_NO1  is null                ;
select * from bass1.g_i_77780_day_mid3 where PHONE_NO2                 is null            ;
--update  bass1.g_i_77780_day set PHONE_NO2 = ' ' where PHONE_NO2  is null                ;
select * from bass1.g_i_77780_day_mid3 where POST_CODE                 is null            ;
--update  bass1.g_i_77780_day set POST_CODE = ' ' where POST_CODE   is null               ;
select * from bass1.g_i_77780_day_mid3 where INDUSTRY_TYPE             is null            ;
--update  bass1.g_i_77780_day set INDUSTRY_TYPE = ' ' where INDUSTRY_TYPE  is null        ;
select * from bass1.g_i_77780_day_mid3 where EMPLOYEE_CNT              is null            ;
--                                                                                        ;
select * from bass1.g_i_77780_day_mid3 where INDUSTRY_UNIT_CNT         is null            ;
select * from bass1.g_i_77780_day_mid3 where ECONOMIC_TYPE             is null            ;
select * from bass1.g_i_77780_day_mid3 where OPEN_YEAR                 is null            ;
select * from bass1.g_i_77780_day_mid3 where OPEN_MONTH                is null            ;
select * from bass1.g_i_77780_day_mid3 where SHAREHOLDER               is null            ;
select * from bass1.g_i_77780_day_mid3 where GROUP_TYPE                is null            ;
select * from bass1.g_i_77780_day_mid3 where MANAGE_STYLE              is null            ;
select * from bass1.g_i_77780_day_mid3 where OPERATE_REVENUE_CLASS     is null            ;
select * from bass1.g_i_77780_day_mid3 where CAPITAL_CLASS             is null            ;
select * from bass1.g_i_77780_day_mid3 where INDUSTRY_CLASS_CODE       is null            ;
select * from bass1.g_i_77780_day_mid3 where CUST_STATUS               is null            ;
select * from bass1.g_i_77780_day_mid3 where CUST_INFO_SRC_ID          is null            ;
select * from bass1.g_i_77780_day_mid3 where UPLOAD_TYPE_ID            is null            ;

select * from bass1.g_i_77780_day where TIME_ID                   is null            ;
select * from bass1.g_i_77780_day where ENTERPRISE_ID             is null            ;
select * from bass1.g_i_77780_day where ID                        is null            ;
--update bass1.g_i_77780_day set id = ' ' where ID                        is null         ;
select * from bass1.g_i_77780_day where ORG_TYPE                  is null            ;
select * from bass1.g_i_77780_day where ADDR_CODE                 is null            ;
select * from bass1.g_i_77780_day where CITY                      is null            ;
select * from bass1.g_i_77780_day where REGION                    is null            ;
select * from bass1.g_i_77780_day where COUNTY                    is null            ;
--update  bass1.g_i_77780_day set COUNTY = ' ' where COUNTY    is null                    ;
select * from bass1.g_i_77780_day where DOOR_NO                   is null            ;
--update  bass1.g_i_77780_day set DOOR_NO = ' ' where DOOR_NO  is null                    ;
select * from bass1.g_i_77780_day where AREA_CODE                 is null            ;
select * from bass1.g_i_77780_day where PHONE_NO1                 is null            ;
--update  bass1.g_i_77780_day set PHONE_NO1 = ' ' where PHONE_NO1  is null                ;
select * from bass1.g_i_77780_day where PHONE_NO2                 is null            ;
--update  bass1.g_i_77780_day set PHONE_NO2 = ' ' where PHONE_NO2  is null                ;
select * from bass1.g_i_77780_day where POST_CODE                 is null            ;
--update  bass1.g_i_77780_day set POST_CODE = ' ' where POST_CODE   is null               ;
select * from bass1.g_i_77780_day where INDUSTRY_TYPE             is null            ;
--update  bass1.g_i_77780_day set INDUSTRY_TYPE = ' ' where INDUSTRY_TYPE  is null        ;
select * from bass1.g_i_77780_day where EMPLOYEE_CNT              is null            ;
--                                                                                        ;
select * from bass1.g_i_77780_day where INDUSTRY_UNIT_CNT         is null            ;
select * from bass1.g_i_77780_day where ECONOMIC_TYPE             is null            ;
select * from bass1.g_i_77780_day where OPEN_YEAR                 is null            ;
select * from bass1.g_i_77780_day where OPEN_MONTH                is null            ;
select * from bass1.g_i_77780_day where SHAREHOLDER               is null            ;
select * from bass1.g_i_77780_day where GROUP_TYPE                is null            ;
select * from bass1.g_i_77780_day where MANAGE_STYLE              is null            ;
select * from bass1.g_i_77780_day where OPERATE_REVENUE_CLASS     is null            ;
select * from bass1.g_i_77780_day where CAPITAL_CLASS             is null            ;
select * from bass1.g_i_77780_day where INDUSTRY_CLASS_CODE       is null            ;
select * from bass1.g_i_77780_day where CUST_STATUS               is null            ;
select * from bass1.g_i_77780_day where CUST_INFO_SRC_ID          is null            ;
select * from bass1.g_i_77780_day where UPLOAD_TYPE_ID            is null            ;
update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '2040'
where INDUSTRY_TYPE = '2050'
;


update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '3010'
where INDUSTRY_TYPE = '3000'
;
 select count(0),count(distinct value(ENTERPRISE_ID,'a')||value(id,'a')) 
from   BASS1.G_I_77780_DAY



update BASS1.G_I_77780_DAY
set CUST_STATUS = ' '
where CUST_STATUS is null 
;

update BASS1.G_I_77780_DAY
set CUST_INFO_SRC_ID = ' '
where CUST_INFO_SRC_ID is null 
;

update BASS1.G_I_77780_DAY
set UPLOAD_TYPE_ID = ' '
where UPLOAD_TYPE_ID is null 
;

update BASS1.G_I_77780_DAY
set GROUP_TYPE = ' '
where GROUP_TYPE is null 
;


update BASS1.G_I_77780_DAY
set MANAGE_STYLE = ' '
where MANAGE_STYLE is null 
;

update BASS1.G_I_77780_DAY
set OPERATE_REVENUE_CLASS = ' '
where OPERATE_REVENUE_CLASS is null 
;

update BASS1.G_I_77780_DAY
set CAPITAL_CLASS = ' '
where CAPITAL_CLASS is null 
;

update BASS1.G_I_77780_DAY
set INDUSTRY_CLASS_CODE = ' '
where INDUSTRY_CLASS_CODE is null 
;


update BASS1.G_I_77780_DAY
set INDUSTRY_TYPE = '7210'
where INDUSTRY_TYPE = '7214'
;

update G_I_77780_DAY_MID3 
set CUST_STATUS = '36'
where  CUST_STATUS IS NULL 





update G_I_77780_DAY_MID3 
set CUST_INFO_SRC_ID = '1'
where  CUST_INFO_SRC_ID IS NULL 


update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '0'||trim(CAPITAL_CLASS)
where length(trim(CAPITAL_CLASS)) = 1




update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '05'
where CAPITAL_CLASS is null 



update G_I_77780_DAY_MID3 
set CAPITAL_CLASS = '01'
where CAPITAL_CLASS = '00'

set ORG_TYPE = '51'
where ORG_TYPE = '5'


update G_I_77780_DAY_MID3 
set ECONOMIC_TYPE = '110'
where ECONOMIC_TYPE in 
('100'
,'101'
,'150'
,'170'
)

update G_I_77780_DAY_MID3 
set OPERATE_REVENUE_CLASS = '10'
where OPERATE_REVENUE_CLASS is null 

                from bass1.G_A_02052_MONTH
                from bass1.G_A_02052_MONTH
select   region_flag,sum(c.BASE_BILL_DURATION) BASE_BILL_DURATION
from         session.region_flag a
inner join session.int_check_user_status b on a.user_id = b.user_id
inner join       session.BASE_BILL_DURATION c on b.PRODUCT_NO = c.PRODUCT_NO
group by region_flag

declare global temporary table session.int_check_user_status
    (
   user_id        CHARACTER(15),
   product_no     CHARACTER(15),
   test_flag      CHARACTER(1),
   sim_code       CHARACTER(15),
   usertype_id    CHARACTER(4),
   create_date    CHARACTER(15),
   brand_id       CHARACTER(4),
   time_id        int
    )                            
partitioning key           
 (
   user_id    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp


--ץȡ�û��������
insert into t_int_check_user_status(
     user_id    
    ,product_no 
    ,test_flag  
    ,sim_code   
    ,usertype_id  
    ,create_date
    ,brand_id
    ,time_id )
select e.user_id
    ,e.product_no  
    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
    ,e.sim_code
    ,f.usertype_id  
    ,e.create_date  
    ,e.brand_id
    ,f.time_id       
from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id
                ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id<=20110331 ) e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=20110331 ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1
and f.usertype_id NOT IN ('2010','2020','2030','9000')

create table t_int_check_user_status like     session.int_check_user_status
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING


            

declare global temporary table session.BASE_BILL_DURATION
    (
   product_no     CHARACTER(15) ,
    BASE_BILL_DURATION bigint
    )                            
partitioning key           
 (
   product_no    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp



declare global temporary table session.BASE_BILL_DURATION
    (
   product_no     CHARACTER(15) ,
    BASE_BILL_DURATION bigint
    )                            
partitioning key           
 (
   product_no    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp


  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (product_no
   ) USING HASHING
           
create table t_int_check_user_status like     session.int_check_user_status
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (product_no
   ) USING HASHING

insert into t_BASE_BILL_DURATION
select PRODUCT_NO,sum(bigint(BASE_BILL_DURATION)) BASE_BILL_DURATION
from G_S_21003_MONTH
where time_id = 201103
group by PRODUCT_NO
create table t_BASE_BILL_DURATION like     session.BASE_BILL_DURATION
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (product_no
   ) USING HASHING

declare global temporary table session.region_flag
    (
   user_id     CHARACTER(20) ,
    REGION_FLAG  CHARACTER(1) 
    )                            
partitioning key           
 (
   user_id    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp

create table t_region_flag like     session.region_flag
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING

insert into t_region_flag
select user_id,region_flag
from 
(
                select user_id,region_flag,row_number()over(partition by user_id order by time_id desc) row_id 
                from bass1.G_A_02052_MONTH
                ) t where row_id = 1
                

select   region_flag,sum(c.BASE_BILL_DURATION) BASE_BILL_DURATION
from         t_region_flag a
inner join t_int_check_user_status b on a.user_id = b.user_id
inner join       t_int_check_user_status c on b.PRODUCT_NO = c.PRODUCT_NO
 where usertype_id NOT IN ('2010','2020','2030','9000')
group by region_flag

select   region_flag,sum(c.BASE_BILL_DURATION) BASE_BILL_DURATION
from         t_region_flag a
inner join t_int_check_user_status b on a.user_id = b.user_id
inner join      t_BASE_BILL_DURATION c on b.PRODUCT_NO = c.PRODUCT_NO
group by region_flag

from         t_region_flag a
inner join t_int_check_user_status b on a.user_id = b.user_id
inner join      t_BASE_BILL_DURATION c on b.PRODUCT_NO = c.PRODUCT_NO
bass1.g_rule_check where rule_code = 'C1'
and time_id between  20110301 and 20110307
from 
(
select OFFER_ID,count(0) cnt
from bass2.DW_PRODUCT_INS_OFF_INS_PROD_201103
where valid_date < '2011-03-01' and expire_date >  '2011-03-01'
group by OFFER_ID
) a ,
SELECT PRODUCT_ITEM_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
        WHERE ITEM_TYPE='OFFER_PLAN'
  AND DEL_FLAG='1'
  AND SUPPLIER_ID IS NOT NULL
) b 
where a.OFFER_ID = b.PRODUCT_ITEM_ID


drop table BASS1.G_I_77780_DAY_DOWN20110422
CREATE TABLE BASS1.G_I_77780_DAY_DOWN20110422
 (TIME_ID                char(1)         ,
  ENTERPRISE_ID          CHARACTER(20),
  ID                     CHARACTER(9),
  ENTERPRISE_NAME        CHARACTER(60),
  ORG_TYPE               CHARACTER(5),
  ADDR_CODE              CHARACTER(6),
  CITY                   CHARACTER(20),
  REGION                 CHARACTER(20),
  COUNTY                 CHARACTER(20),
  DOOR_NO                CHARACTER(60),
  AREA_CODE              CHARACTER(5),
  PHONE_NO1              CHARACTER(11),
  PHONE_NO2              CHARACTER(10),
  POST_CODE              CHARACTER(6),
  INDUSTRY_TYPE          CHARACTER(4),
  EMPLOYEE_CNT           CHARACTER(8),
  INDUSTRY_UNIT_CNT      CHARACTER(5),
  ECONOMIC_TYPE          CHARACTER(3),
  OPEN_YEAR              CHARACTER(4),
  OPEN_MONTH             CHARACTER(2),
  SHAREHOLDER            CHARACTER(1),
  GROUP_TYPE             CHARACTER(1),
  MANAGE_STYLE           CHARACTER(1),
  OPERATE_REVENUE_CLASS  CHARACTER(2),
  CAPITAL_CLASS          CHARACTER(2),
  INDUSTRY_CLASS_CODE    CHARACTER(2)    ,
  CUST_STATUS            CHARACTER(2),
  CUST_INFO_SRC_ID       CHARACTER(1),
  UPLOAD_TYPE_ID         CHARACTER(1)   
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    ID
   ) USING HASHING

ALTER TABLE BASS1.G_I_77780_DAY_DOWN20110422
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
--,  count(distinct POINT_FEEDBACK_ID ) 
from G_S_02007_month 
group by  POINT_FEEDBACK_ID 
order by 1 
	         group by user_id,ord_code 
FLOWDOWN                       SYSIBM    CHARACTER                10     0 No    
(
select BASE_PROD_ID , count(0) 
--,  count(distinct BASE_PROD_ID ) 
from g_i_02018_month 
group by  BASE_PROD_ID 
) a ,bass2.dim_prod_up_product_item  b 
select BASE_PROD_TYPE , count(0) 
--,  count(distinct BASE_PROD_TYPE ) 
from g_i_02018_month 
group by  BASE_PROD_TYPE 
order by 1 


                  ,sum(bigint(data_flow_down1+data_flow_down2))*1.0/1024/1024/1024 as down_flows
                  ,sum(bigint(data_flow_down1+data_flow_down2))*1.0/1024/1024/1024 as down_flows
            where drtype_id not in (8307)
              and bigint(product_no) not between 14734500000 
              and 14734999999 and apn_ni <> 'JF.XZ.IP.MOBILE.LAN.CHINAMOBILE'
              and service_code not in ('1010000001','1010000002','2000000000')
select BASE_PROD_TYPE , count(0) 
--,  count(distinct BASE_PROD_TYPE ) 
from bass1.g_i_02018_month 
group by  BASE_PROD_TYPE 
order by 1 

--,  count(distinct base_prod_id ) 
from bass1.g_i_02018_month 
group by  base_prod_id 
order by 1 

select \n\r from bass2.dual
VALUES 'Hello everyone' || CHR(10) || CHR(13) || 'i''m wave'  


CREATE TABLE BASS1.dim_gprs_pkg_flow
 (
	prod_id integer,
	flow decimal(12,2)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (prod_id
   ) USING HASHING;

ALTER TABLE BASS1.dim_gprs_pkg_flow
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
insert into bass1.dim_gprs_pkg_flow values (90004013,10);
insert into bass1.dim_gprs_pkg_flow values (90001311,10);
insert into bass1.dim_gprs_pkg_flow values (90004011,10);
insert into bass1.dim_gprs_pkg_flow values (90009610,10);
insert into bass1.dim_gprs_pkg_flow values (99001694,10);
insert into bass1.dim_gprs_pkg_flow values (90009401,10);
insert into bass1.dim_gprs_pkg_flow values (90004012,10);
insert into bass1.dim_gprs_pkg_flow values (90004303,10);
insert into bass1.dim_gprs_pkg_flow values (90009334,20);
insert into bass1.dim_gprs_pkg_flow values (99001642,20);
insert into bass1.dim_gprs_pkg_flow values (90004304,20);
insert into bass1.dim_gprs_pkg_flow values (99001612,20);
insert into bass1.dim_gprs_pkg_flow values (99001613,20);
insert into bass1.dim_gprs_pkg_flow values (99001643,20);
insert into bass1.dim_gprs_pkg_flow values (90600006,20);
insert into bass1.dim_gprs_pkg_flow values (90001214,20);
insert into bass1.dim_gprs_pkg_flow values (90400006,20);
insert into bass1.dim_gprs_pkg_flow values (90300006,20);
insert into bass1.dim_gprs_pkg_flow values (90001324,30);
insert into bass1.dim_gprs_pkg_flow values (90001330,30);
insert into bass1.dim_gprs_pkg_flow values (90001329,30);
insert into bass1.dim_gprs_pkg_flow values (90001323,30);
insert into bass1.dim_gprs_pkg_flow values (90001326,30);
insert into bass1.dim_gprs_pkg_flow values (90001327,30);
insert into bass1.dim_gprs_pkg_flow values (90001321,30);
insert into bass1.dim_gprs_pkg_flow values (90001313,30);
insert into bass1.dim_gprs_pkg_flow values (90009392,30);
insert into bass1.dim_gprs_pkg_flow values (90004024,30);
insert into bass1.dim_gprs_pkg_flow values (90001212,30);
insert into bass1.dim_gprs_pkg_flow values (90004040,30);
insert into bass1.dim_gprs_pkg_flow values (90004015,30);
insert into bass1.dim_gprs_pkg_flow values (90008109,30);
insert into bass1.dim_gprs_pkg_flow values (90001213,30);
insert into bass1.dim_gprs_pkg_flow values (90009614,30);
insert into bass1.dim_gprs_pkg_flow values (90001320,30);
insert into bass1.dim_gprs_pkg_flow values (90009722,30);
insert into bass1.dim_gprs_pkg_flow values (99001136,30);
insert into bass1.dim_gprs_pkg_flow values (90001312,30);
insert into bass1.dim_gprs_pkg_flow values (90001322,30);
insert into bass1.dim_gprs_pkg_flow values (90001325,30);
insert into bass1.dim_gprs_pkg_flow values (99001670,30);
insert into bass1.dim_gprs_pkg_flow values (90001319,30);
insert into bass1.dim_gprs_pkg_flow values (90001328,30);
insert into bass1.dim_gprs_pkg_flow values (90004302,30);
insert into bass1.dim_gprs_pkg_flow values (90009424,30);
insert into bass1.dim_gprs_pkg_flow values (90003312,50);
insert into bass1.dim_gprs_pkg_flow values (90003306,50);
insert into bass1.dim_gprs_pkg_flow values (90001315,50);
insert into bass1.dim_gprs_pkg_flow values (90003311,50);
insert into bass1.dim_gprs_pkg_flow values (90003305,50);
insert into bass1.dim_gprs_pkg_flow values (90003308,50);
insert into bass1.dim_gprs_pkg_flow values (99001614,50);
insert into bass1.dim_gprs_pkg_flow values (90003315,50);
insert into bass1.dim_gprs_pkg_flow values (90003307,50);
insert into bass1.dim_gprs_pkg_flow values (90001314,50);
insert into bass1.dim_gprs_pkg_flow values (90003316,50);
insert into bass1.dim_gprs_pkg_flow values (99001660,60);
insert into bass1.dim_gprs_pkg_flow values (99001644,60);
insert into bass1.dim_gprs_pkg_flow values (90009292,70);
insert into bass1.dim_gprs_pkg_flow values (90009721,70);
insert into bass1.dim_gprs_pkg_flow values (90004305,70);
insert into bass1.dim_gprs_pkg_flow values (90004023,70);
insert into bass1.dim_gprs_pkg_flow values (99001002,77);
insert into bass1.dim_gprs_pkg_flow values (99001335,77);
insert into bass1.dim_gprs_pkg_flow values (99001504,77);
insert into bass1.dim_gprs_pkg_flow values (99001316,77);
insert into bass1.dim_gprs_pkg_flow values (99001502,77);
insert into bass1.dim_gprs_pkg_flow values (99001320,77);
insert into bass1.dim_gprs_pkg_flow values (99001009,77);
insert into bass1.dim_gprs_pkg_flow values (99001311,77);
insert into bass1.dim_gprs_pkg_flow values (99001308,77);
insert into bass1.dim_gprs_pkg_flow values (99001001,77);
insert into bass1.dim_gprs_pkg_flow values (99001317,77);
insert into bass1.dim_gprs_pkg_flow values (99001663,85);
insert into bass1.dim_gprs_pkg_flow values (99001676,100);
insert into bass1.dim_gprs_pkg_flow values (90003213,100);
insert into bass1.dim_gprs_pkg_flow values (99001661,100.50);
insert into bass1.dim_gprs_pkg_flow values (99001645,100.50);
insert into bass1.dim_gprs_pkg_flow values (90009133,140);
insert into bass1.dim_gprs_pkg_flow values (99001685,150);
insert into bass1.dim_gprs_pkg_flow values (90001316,150);
insert into bass1.dim_gprs_pkg_flow values (90008110,150);
insert into bass1.dim_gprs_pkg_flow values (99001634,150);
insert into bass1.dim_gprs_pkg_flow values (90009393,150);
insert into bass1.dim_gprs_pkg_flow values (99001137,150);
insert into bass1.dim_gprs_pkg_flow values (90009720,150);
insert into bass1.dim_gprs_pkg_flow values (90009723,150);
insert into bass1.dim_gprs_pkg_flow values (90009615,150);
insert into bass1.dim_gprs_pkg_flow values (99001671,150);
insert into bass1.dim_gprs_pkg_flow values (90009431,150);
insert into bass1.dim_gprs_pkg_flow values (92001002,150);
insert into bass1.dim_gprs_pkg_flow values (90004025,150);
insert into bass1.dim_gprs_pkg_flow values (90003019,154);
insert into bass1.dim_gprs_pkg_flow values (99001677,200);
insert into bass1.dim_gprs_pkg_flow values (90009328,210);
insert into bass1.dim_gprs_pkg_flow values (90009329,210);
insert into bass1.dim_gprs_pkg_flow values (90001317,300);
insert into bass1.dim_gprs_pkg_flow values (90003314,400);
insert into bass1.dim_gprs_pkg_flow values (90003310,400);
insert into bass1.dim_gprs_pkg_flow values (90003303,400);
insert into bass1.dim_gprs_pkg_flow values (90003301,400);
insert into bass1.dim_gprs_pkg_flow values (90003304,400);
insert into bass1.dim_gprs_pkg_flow values (90003302,400);
insert into bass1.dim_gprs_pkg_flow values (90003313,400);
insert into bass1.dim_gprs_pkg_flow values (90003309,400);
insert into bass1.dim_gprs_pkg_flow values (73700001,500);
insert into bass1.dim_gprs_pkg_flow values (90009616,500);
insert into bass1.dim_gprs_pkg_flow values (90009724,500);
insert into bass1.dim_gprs_pkg_flow values (99001672,500);
insert into bass1.dim_gprs_pkg_flow values (99001690,500);
insert into bass1.dim_gprs_pkg_flow values (99001687,500);
insert into bass1.dim_gprs_pkg_flow values (90004014,500);
insert into bass1.dim_gprs_pkg_flow values (99001689,500);
insert into bass1.dim_gprs_pkg_flow values (90004026,500);
insert into bass1.dim_gprs_pkg_flow values (90009437,500);
insert into bass1.dim_gprs_pkg_flow values (90004052,500);
insert into bass1.dim_gprs_pkg_flow values (90009490,500);
insert into bass1.dim_gprs_pkg_flow values (99001686,500);
insert into bass1.dim_gprs_pkg_flow values (90001318,500);
insert into bass1.dim_gprs_pkg_flow values (99001606,501);
insert into bass1.dim_gprs_pkg_flow values (99001662,501);
insert into bass1.dim_gprs_pkg_flow values (99001646,525);
insert into bass1.dim_gprs_pkg_flow values (90004047,600);
insert into bass1.dim_gprs_pkg_flow values (90004017,800);
insert into bass1.dim_gprs_pkg_flow values (90004018,800);
insert into bass1.dim_gprs_pkg_flow values (90009293,800);
insert into bass1.dim_gprs_pkg_flow values (90004020,800);
insert into bass1.dim_gprs_pkg_flow values (90004019,800);
insert into bass1.dim_gprs_pkg_flow values (90009114,980);
insert into bass1.dim_gprs_pkg_flow values (90009617,1024);
insert into bass1.dim_gprs_pkg_flow values (99001673,1024);
insert into bass1.dim_gprs_pkg_flow values (99001692,1024);
insert into bass1.dim_gprs_pkg_flow values (99001691,1024);
insert into bass1.dim_gprs_pkg_flow values (99001664,1026);
insert into bass1.dim_gprs_pkg_flow values (99001605,1026);
insert into bass1.dim_gprs_pkg_flow values (90004053,2048);
insert into bass1.dim_gprs_pkg_flow values (99001693,2048);
insert into bass1.dim_gprs_pkg_flow values (90004016,2048);
insert into bass1.dim_gprs_pkg_flow values (99001138,2048);
insert into bass1.dim_gprs_pkg_flow values (90004027,2048);
insert into bass1.dim_gprs_pkg_flow values (73700002,2048);
insert into bass1.dim_gprs_pkg_flow values (90009438,2048);
insert into bass1.dim_gprs_pkg_flow values (90009491,2048);
insert into bass1.dim_gprs_pkg_flow values (90004048,2248);
insert into bass1.dim_gprs_pkg_flow values (90004049,3372);
insert into bass1.dim_gprs_pkg_flow values (90004028,5120);
insert into bass1.dim_gprs_pkg_flow values (90004054,5120);
insert into bass1.dim_gprs_pkg_flow values (99001139,5120);
insert into bass1.dim_gprs_pkg_flow values (90009439,5120);
insert into bass1.dim_gprs_pkg_flow values (73700003,5120);
insert into bass1.dim_gprs_pkg_flow values (99001517,10000);
insert into bass1.dim_gprs_pkg_flow values (99001518,10000);
insert into bass1.dim_gprs_pkg_flow values (73700004,10240);
insert into bass1.dim_gprs_pkg_flow values (90004050,10240);
insert into bass1.dim_gprs_pkg_flow values (90004055,10240);
insert into bass1.dim_gprs_pkg_flow values (90004007,13800);
insert into bass1.dim_gprs_pkg_flow values (90004001,21150);
insert into bass1.dim_gprs_pkg_flow values (90004010,23200);
insert into bass1.dim_gprs_pkg_flow values (90004006,184000);
insert into bass1.dim_gprs_pkg_flow values (90004008,188416);
insert into bass1.dim_gprs_pkg_flow values (90004002,288768);
insert into bass1.dim_gprs_pkg_flow values (90004009,471040);
insert into bass1.dim_gprs_pkg_flow values (90004003,721920);

		from bass2.dwd_product_sprom_active_20110301 a,
			 BASS1.dim_gprs_pkg_flow  b
		where a.sprom_id=b.prod_id
		  and replace(char(date(a.valid_date)),'-','')<='20110301'
		  and replace(char(date(a.expire_date)),'-','')>'20110301'
		  

CREATE TABLE BASS1.t_gprs_prod_user
 (
user_id VARCHAR(20),
prod_id integer,
valid_date TIMESTAMP,
expire_date TIMESTAMP
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING;

ALTER TABLE BASS1.t_gprs_prod_user
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
insert into BASS1.t_gprs_prod_user
  select user_id,b.prod_id,a.valid_date,a.expire_date
		from bass2.dwd_product_sprom_active_20110301 a,
			 BASS1.dim_gprs_pkg_flow  b
		where a.sprom_id=b.prod_id
		  and replace(char(date(a.valid_date)),'-','')<='20110301'
		  and replace(char(date(a.expire_date)),'-','')>'20110301'		  
select user_id,prod_id,valid_date
from  
(select a.*,row_number()over(PARTITION by user_id order by expire_date desc , valid_date desc  ) rn 
BASS1.t_gprs_prod_user a
) t where t.rn = 1

  drop table BASS1.t_gprs_prod_user2;
CREATE TABLE BASS1.t_gprs_prod_user2
 (
user_id VARCHAR(20),
prod_id integer,
valid_date TIMESTAMP
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING;

ALTER TABLE BASS1.t_gprs_prod_user2
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
  
drop table BASS1.t_gprs_prod_user;
CREATE TABLE BASS1.t_gprs_prod_user
 (
user_id VARCHAR(20),
prod_id integer,
	flow decimal(12,2),
	valid_date TIMESTAMP,
expire_date TIMESTAMP
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING;

ALTER TABLE BASS1.t_gprs_prod_user
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
insert into BASS1.t_gprs_prod_user
  select user_id,b.prod_id,b.flow,a.valid_date,a.expire_date
		from bass2.dwd_product_sprom_active_20110301 a,
			 BASS1.dim_gprs_pkg_flow  b
		where a.sprom_id=b.prod_id
		  and replace(char(date(a.valid_date)),'-','')<='20110301'
		  and replace(char(date(a.expire_date)),'-','')>'20110301'		  

  
  drop table BASS1.t_gprs_prod_user2;
CREATE TABLE BASS1.t_gprs_prod_user2
 (
user_id VARCHAR(20),
prod_id integer,
	flow decimal(12,2),
valid_date TIMESTAMP
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING;

ALTER TABLE BASS1.t_gprs_prod_user2
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
  
insert into  BASS1.t_gprs_prod_user2
select user_id,prod_id,valid_date
from  
(select a.*,row_number()over(PARTITION by user_id order by expire_date|| valid_date desc  ) rn 
BASS1.t_gprs_prod_user a
) t where t.rn = 1

 

insert into  BASS1.t_gprs_prod_user2
select user_id,prod_id,flow,valid_date
from  
(select a.*,row_number()over(PARTITION by user_id order by expire_date desc , valid_date desc   ) rn 
BASS1.t_gprs_prod_user a
) t where t.rn = 1

 
from  BASS1.t_gprs_prod_user2

CREATE TABLE BASS1.t_gprs_sum
 (
        UP_FLOWS                DECIMAL(12,2)         
        ,DOWN_FLOWS              DECIMAL(12,2)            
        ,FREE_IS_PKG             DECIMAL(12,2)              
        ,FREE_NOT_PKG             DECIMAL(12,2)              
        ,NOT_FREE_NOT_PKG        DECIMAL(12,2)         
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (UP_FLOWS
   ) USING HASHING;

ALTER TABLE BASS1.t_gprs_sum
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
                  sum(bigint(data_flow_up1+data_flow_up2))*1.0/1024/1024/1024   as up_flows
                  ,sum(bigint(data_flow_down1+data_flow_down2))*1.0/1024/1024/1024 as down_flows
                  ,sum(case when free_res_val1 is not null and (charge1+charge2+charge3+charge4) = 0 then 
             bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else 0 end     ) free_is_pkg
                  ,sum(case when free_res_val1 is  null and (charge1+charge2+charge3+charge4) = 0 then 
             bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else 0 end     ) free_not_pkg
                  ,sum( case when (charge1+charge2+charge3+charge4) > 0 then 
             bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else  0 end
             ) not_free_not_pkg 
from  bass2.CDR_GPRS_LOCAL_20110322 a
left join BASS1.t_gprs_prod_user2 b on  a.user_id = b.user_id 
where  b.user_id is null 
 drop table BASS1.t_gprs_sum2;
CREATE TABLE BASS1.t_gprs_sum2
 (
        UP_FLOWS                DECIMAL(12,5)         
        ,DOWN_FLOWS              DECIMAL(12,5)            
        ,FREE_IS_PKG             DECIMAL(12,5)              
        ,FREE_NOT_PKG             DECIMAL(12,5)              
        ,NOT_FREE_NOT_PKG        DECIMAL(12,5)         
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (UP_FLOWS
   ) USING HASHING;

ALTER TABLE BASS1.t_gprs_sum2
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
CREATE FUNCTION BASS1.FN_GET_ALL_DIM(GID VARCHAR(20),DID VARCHAR(20)) RETURNS VARCHAR(10) DETERMINISTIC NO EXTERNAL ACTION LANGUAGE SQL BEGIN ATOMIC RETURN SELECT BASS1_VALUE FROM BASS1.ALL_DIM_LKP WHERE BASS1_TBID = GID AND XZBAS_VALUE = DID; END

dim_acct_item	����ʱ���»����� 	80000512	��Ŀ��Ŀ	BASS_STD1_0074	0626	GPRS�ײͷ�
dim_acct_item	�����ײ�����ʱ���� 	80000513	��Ŀ��Ŀ	BASS_STD1_0074	0627	GPRSͨ�ŷ�

where BASS1_TBID = 'BASS_STD1_0074'
and bass1_value in ('0626','0627')
declare global temporary table session.int_check_user_status
    (
   user_id        CHARACTER(15),
   product_no     CHARACTER(15),
   test_flag      CHARACTER(1),
   sim_code       CHARACTER(15),
   usertype_id    CHARACTER(4),
   create_date    CHARACTER(15),
   brand_id       CHARACTER(4),
   time_id        int
    )                            
partitioning key           
 (
   user_id    
 ) using hashing           
with replace on commit preserve rows not logged in tbs_user_temp

--session��ʱ������������ʵ���
drop table t_int_check_user_status
create table t_int_check_user_status like     session.int_check_user_status
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHINGdrop table t_int_check_user_status
create table t_int_check_user_status like     session.int_check_user_status
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (user_id
   ) USING HASHING



--ץȡ�û��������
insert into t_int_check_user_status (
     user_id    
    ,product_no 
    ,test_flag  
    ,sim_code   
    ,usertype_id  
    ,create_date
    ,brand_id
    ,time_id )
select e.user_id
    ,e.product_no  
    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
    ,e.sim_code
    ,f.usertype_id  
    ,e.create_date  
    ,e.brand_id
    ,f.time_id       
from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id
                ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id<=20110331 and SIM_CODE = '0') e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=20110331 ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1



from 
(
select product_no from t_int_check_user_status where usertype_id NOT IN ('2010','2020','2030','9000')
except 
select product_no from  bass1.td_check_user_mobile
)t

                        where a.ITEM_ID = b.item_id 
80000103	�Ż�GPRS��	30
80000634	��E��-GPRS��	4011
80000663	M2M GPRS���ܷ�	18
80000664	M2M_GPRS���ܷ�(5Ԫ30M/��)	18
                      
                        where a.ITEM_ID = b.item_id 
80000078	�����ײ�����������	677714
80000104	���������»�����	187057
80000462	G3�����������ײ�������	206
80000531	G3�����������ײ�������	140

                          a.item_id in (80000508,80000512,80000513) 
0627	GPRSͨ�ŷ�	�ײ��û��������ֵ�GPRSͨ�ŷѣ��Լ����ײ��û���GPRSͨ�ŷ�
                          
dim_acct_item	����ʱ���»����� 	80000512	��Ŀ��Ŀ	BASS_STD1_0074	0626	GPRS�ײͷ�
dim_acct_item	�����ײ�����ʱ���� 	80000513	��Ŀ��Ŀ	BASS_STD1_0074	0627	GPRSͨ�ŷ�

drop view t_gprs_03

create view t_gprs_03
as
select 
                  sum(bigint(data_flow_up1+data_flow_up2))*1.0/1024/1024/1024   as up_flows
                  ,sum(bigint(data_flow_down1+data_flow_down2))*1.0/1024/1024/1024 as down_flows
                  --
                  ,sum(case when free_res_val1 is not null and (charge1+charge2+charge3+charge4) = 0 then 
             bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else 0 end     ) free_is_pkg
                  ,sum(case when free_res_val1 is  null and (charge1+charge2+charge3+charge4) = 0 then 
             bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else 0 end     ) free_not_pkg
             
                  ,sum( case when (charge1+charge2+charge3+charge4) > 0 then 
             bigint(data_flow_up1+data_flow_up2+data_flow_down1+data_flow_down2)*1.0/1024/1024/1024  else  0 end
             ) not_free_not_pkg 
from  bass2.CDR_GPRS_LOCAL_20110423 a,
BASS1.t_gprs_prod_user2 b 
where a.user_id = b.user_id 
 
 UP_FLOWS	DOWN_FLOWS	FREE_IS_PKG	FREE_NOT_PKG
40.15598522219	226.62631536461	128.06665484731	122.77105686100


 40.15598522219	226.62631536461	128.06665484731	122.77105686100
not_free_not_pkg
 15.94457482177
 
 
  
		    a.product_item_id                         base_prod_id,
		    b.trademark
		from bass2.dim_prod_up_product_item a,
		  bass2.Dim_prod_up_offer b
		where a.item_type = 'OFFER_PLAN' 
		and b.offer_type in ('OFFER_PLAN')
		and a.product_item_id = b.offer_id
		and a.platform_id in (1,2)
,count(0) op_time
,sum(amount) back_cnt
from bass2.DW_ACCT_PAYMENT_DM_201103 a
where a.remarks like '%SP�˷�%'
group by replace(char(date(a.OP_TIME)),'-','')
,count(0) back_cnt
,sum(amount) back_fee
from bass2.DW_ACCT_PAYMENT_DM_201103 a
where a.remarks like '%SP�˷�%'
and replace(char(date(a.OP_TIME)),'-','') = '20110330'
group by replace(char(date(a.OP_TIME)),'-','')
,char(count(0)) back_cnt
,char(bigint(sum(amount))) back_fee
from bass2.DW_ACCT_PAYMENT_DM_201103 a
where a.remarks like '%SP�˷�%'
and replace(char(date(a.OP_TIME)),'-','') = '20110330'
group by replace(char(date(a.OP_TIME)),'-','')

     when a.crm_brand_id2=70 then '1' 
     else '0' 
    end
     when a.crm_brand_id2=70 then '1' 
     else '0' 
    end
     when a.crm_brand_id2=70 then '1' 
     else '0' 
    end
 (
interface_code  char(5)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (interface_code) USING HASHING
   
  ALTER TABLE BASS1.mon_interface_not_empty
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE
 ('01002')
,('02004')
,('02008')
,('02011')
,('02014')
,('02015')
,('02016')
,('02018')
,('02019')
,('02020')
,('02021')
,('02047')
,('02049')
,('02050')
,('02052')
,('02053')
,('02063')
,('03001')
,('03002')
,('03004')
,('03005')
,('03013')
,('04002')
,('04004')
,('04005')
,('04006')
,('04007')
,('04008')
,('04011')
,('05001')
,('05002')
,('05003')
,('06021')
,('06022')
,('06023')
,('06011')
,('06012')
,('06029')
,('06031')
,('06032')
,('21001')
,('21002')
,('21007')
,('21003')
,('21008')
,('21011')
,('21012')
,('21013')
,('21014')
,('21015')
,('21020')
,('22012')
,('22013')
,('22038')
,('22039')
,('22032')
,('22033')
,('22035')
,('22052')
,('22070')
,('22073')
,('22061')
,('22062')
,('22063')
,('22064')
,('22065')
,('22049')
,('22050')
,('22055')
,('22056')
,('22101')
,('22102')
,('22103')
,('22104')
,('22105')
,('22201')
,('22202')
,('22203')
,('22080')
,('22081')
,('22082')
,('22083')
,('22084')
,('22085')
;
  
(   
VALUES ('����','1997-7-1'),('����','1949-10-1')   
)   
SELECT NAME_TEST FROM TEST WHERE BDAY_TEST='1949-10-1'
 (
	 TIME_ID            	INTEGER             ----��¼�к�        
	,OP_TIME            	CHAR(8)             ----���� ����       
	,BACK_CNT           	CHAR(12)            ----�˷ѱ���        
	,BACK_FEE           	CHAR(12)            ----�˷ѽ�� ��λ��Ԫ
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22084_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
TBS_APP_BASS1
                                            bass2.dwd_product_regsp_20110425 b
                                       where bigint(b.sp_code)>0 
                                           and bigint(substr(replace(char(date(b.valid_date)),'-',''),1,6))=201103                               
                                           and bigint(a.sp_code)=bigint(b.sp_code)   
  from 
(
select OFFER_ID,count(0) cnt
from bass2.DW_PRODUCT_INS_OFF_INS_PROD_201103
where valid_date < '2011-03-01' and expire_date >  '2011-03-01'
group by OFFER_ID
) a ,(
  	SELECT PRODUCT_ITEM_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
			WHERE ITEM_TYPE='OFFER_PLAN'
	  AND DEL_FLAG='1'
	  AND SUPPLIER_ID IS NOT NULL
) b 
where a.OFFER_ID = b.PRODUCT_ITEM_ID
--22085�ӿڵ�Ԫ���ƣ��շ��������˷Ѻ��֤�»��� 
CREATE TABLE BASS1.G_S_22085_MONTH
 (
	 TIME_ID            	INTEGER             ----��������        
	,OP_TIME            	CHAR(6)             ----�·� ����       
	,BACK_SP_NAME       	CHAR(50)            ----�˷�SP��ҵ���� ����
	,BACK_SP_CODE       	CHAR(12)            ----SP��ҵ���� ���� 
	,BACK_CNT           	CHAR(12)            ----�˷ѱ���        
	,BACK_FEE           	CHAR(12)            ----�����˷��ܶ� ��λ�� Ԫ
	,SP_ACT_INCOME      	CHAR(15)            ----����SPʵ���˿� ��λ�� Ԫ
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (OP_TIME,BACK_SP_NAME,BACK_SP_CODE
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22085_MONTH
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;


select PROGRAM_TYPE , count(0) 
--,  count(distinct PROGRAM_TYPE ) 
from bass1.int_program_data 
group by  PROGRAM_TYPE 
order by 1 
select
SEQUENCE_ID
,PROGRAM_TYPE
,'G_S_22085_MONTH.tcl' PROGRAM_NAME
,'G_S_22085_MONTH.BASS1' SOURCE_DATA
,'G_S_22085_MONTH_e' OBJECTIVE_DATA
,'G_S_22085_MONTH_f' FINAL_DATA
from bass1.int_program_data
where PROGRAM_NAME = 'G_S_22204_MONTH.tcl'
select
SEQUENCE_ID
,PROGRAM_TYPE
,'G_S_22084_DAY.tcl' PROGRAM_NAME
,'G_S_22084_DAY.BASS1' SOURCE_DATA
,'G_S_22084_DAY_e' OBJECTIVE_DATA
,'G_S_22084_DAY_f' FINAL_DATA
from bass1.int_program_data
where PROGRAM_NAME = 'G_S_22204_MONTH.tcl'
90001320
90001321
90001322
90001323
90001324
90001325


90001326
90001327
90001328

111090001320
111090001321
111090001322
111090001323
111090001324
111090001325
111090001326
111090001327
111090001328

('111090001319'
,'111090001320'
,'111090001321'
,'111090001322'
,'111090001323'
,'111090001324'
,'111090001325'
,'111090001326'
,'111090001327'
,'111090001328')
select offer_id,count(0)
from bass2.dw_product_ins_prod_201103 a 
where  offer_id
in
(111090001319
,111090001320
,111090001321
,111090001322
,111090001323
,111090001324
,111090001325
,111090001326
,111090001327
,111090001328)
group by offer_id
in
(111090001319
,111090001320
,111090001321
,111090001322
,111090001323
,111090001324
,111090001325
,111090001326
,111090001327
,111090001328)
from bass2.dw_product_ins_prod_ds a 
where  offer_id
in
(111090001319
,111090001320
,111090001321
,111090001322
,111090001323
,111090001324
,111090001325
,111090001326
,111090001327
,111090001328)
group by offer_id
 (
	 TIME_ID            	INTEGER             ----��������        
	,USER_ID            	CHAR(20)            ----�û���ʶ ����   
	,BASE_PKG_ID        	CHAR(30)            ----�����ײͱ�ʶ    
	,VALID_DT           	CHAR(8)             ----�ײ���Ч����     
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,USER_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_I_02022_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

		     count(0)
		from bass2.dw_product_ins_prod_201103 a,
		    (
		    select product_instance_id user_id from bass2.dw_product_ins_prod_201103
		    where state in ('1','4','6','8','M','7','C','9')
		      and user_type_id =1
		      and valid_type = 1
		      and bill_id not in ('D15289014474','D15289014454')
		    except
		    select user_id from bass2.dw_product_test_phone_201103
		    where sts=1
		    ) b
		where a.product_instance_id=b.user_id
		  and a.state in ('1','4','6','8','M','7','C','9')
		  and a.user_type_id =1
		  and a.valid_type = 1
		  and a.bill_id not in ('D15289014474','D15289014454')
		     count(0)
		from bass2.dw_product_ins_prod_201103 a
		where  a.state in ('1','4','6','8','M','7','C','9')
		  and a.user_type_id =1
		  and a.valid_type = 1
		  and a.bill_id not in ('D15289014474','D15289014454')
 ("XZBAS_TBNAME"      VARCHAR(100),
  "XZBAS_COLNAME"     VARCHAR(100)    NOT NULL,
  "XZBAS_VALUE"       VARCHAR(100)    NOT NULL,
  "BASS1_TBN_DESC"    VARCHAR(100)    NOT NULL,
  "BASS1_TBID"        VARCHAR(100)    NOT NULL,
  "BASS1_VALUE"       VARCHAR(100)    NOT NULL,
  "BASS1_VALUE_DESC"  VARCHAR(100)
 )
  DATA CAPTURE NONE
 IN "TBS_APP_BASS1"
 INDEX IN "TBS_INDEX"
  PARTITIONING KEY
   (BASS1_TBID
   ) USING HASHING;

ALTER TABLE "BASS1"."ALL_DIM_LKP"
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

select * from    bass2.DIM_PROD_UP_PRODUCT_ITEM 
select 'W_QQT_JC_SW58		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷ������ײ�58Ԫ' bass1_name,						0				 bass2_id from bass2.dual union all
select 'W_QQT_JC_SW88		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷ������ײ�88Ԫ' bass1_name,	         0          bass2_id from bass2.dual union all
select 'W_QQT_JC_SW128		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷ������ײ�128Ԫ' bass1_name,	      0           bass2_id from bass2.dual union all
select 'W_QQT_JC_SL58		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷ������ײ�58Ԫ' bass1_name,	111090001319       bass2_id from bass2.dual union all
select 'W_QQT_JC_SL88		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷ������ײ�88Ԫ' bass1_name,	111090001320       bass2_id from bass2.dual union all
select 'W_QQT_JC_SL128		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷ������ײ�128Ԫ' bass1_name,	111090001321     bass2_id from bass2.dual union all
select 'W_QQT_JC_SL158		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷ������ײ�158Ԫ' bass1_name,	111090001322     bass2_id from bass2.dual union all
select 'W_QQT_JC_SL188		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷ������ײ�188Ԫ' bass1_name,	111090001323     bass2_id from bass2.dual union all
select 'W_QQT_JC_SL288		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷ������ײ�288Ԫ' bass1_name,	111090001324     bass2_id from bass2.dual union all
select 'W_QQT_JC_SL388		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷ������ײ�388Ԫ' bass1_name,	111090001325     bass2_id from bass2.dual union all
select 'W_QQT_JC_SL588		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷ������ײ�588Ԫ' bass1_name,	        0         bass2_id from bass2.dual union all
select 'W_QQT_JC_SL888		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷ������ײ�888Ԫ' bass1_name,	          0       bass2_id from bass2.dual union all
select 'W_QQT_JC_BD58		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷѱ����ײ�58Ԫ' bass1_name,	111090001326       bass2_id from bass2.dual union all
select 'W_QQT_JC_BD88		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷѱ����ײ�88Ԫ' bass1_name,	111090001327       bass2_id from bass2.dual union all
select 'W_QQT_JC_BD128		' bass1_id,'ȫ��ͨȫ��ͳһ�ʷѱ����ײ�128Ԫ' bass1_name,	111090001328     bass2_id from bass2.dual 
) t      where a.PRODUCT_ITEM_ID = t.bass2_id

INSERT INTO bass1.all_dim_lkp
SELECT 'BASS2.DIM_PROD_UP_PRODUCT_ITEM' 
,A.NAME 
,CHAR(BASS2_ID)
,'ȫ��ͨȫ��ͳһ�ʷѻ����ײͱ�ʶ' 
,'BASS_STD1_0114'
,TRIM(BASS1_ID)
,TRIM(BASS1_NAME)
FROM   BASS2.DIM_PROD_UP_PRODUCT_ITEM A,
    TABLE(
SELECT 'w_qqt_jc_sw58		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷ������ײ�58Ԫ' BASS1_NAME,						0				 BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_sw88		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷ������ײ�88Ԫ' BASS1_NAME,	         0          BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_sw128		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷ������ײ�128Ԫ' BASS1_NAME,	      0           BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_sl58		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷ������ײ�58Ԫ' BASS1_NAME,	111090001319       BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_sl88		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷ������ײ�88Ԫ' BASS1_NAME,	111090001320       BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_sl128		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷ������ײ�128Ԫ' BASS1_NAME,	111090001321     BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_sl158		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷ������ײ�158Ԫ' BASS1_NAME,	111090001322     BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_sl188		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷ������ײ�188Ԫ' BASS1_NAME,	111090001323     BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_sl288		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷ������ײ�288Ԫ' BASS1_NAME,	111090001324     BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_sl388		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷ������ײ�388Ԫ' BASS1_NAME,	111090001325     BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_sl588		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷ������ײ�588Ԫ' BASS1_NAME,	        0         BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_sl888		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷ������ײ�888Ԫ' BASS1_NAME,	          0       BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_bd58		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷѱ����ײ�58Ԫ' BASS1_NAME,	111090001326       BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_bd88		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷѱ����ײ�88Ԫ' BASS1_NAME,	111090001327       BASS2_ID FROM BASS2.DUAL UNION ALL
SELECT 'w_qqt_jc_bd128		' BASS1_ID,'ȫ��ͨȫ��ͳһ�ʷѱ����ײ�128Ԫ' BASS1_NAME,	111090001328     BASS2_ID FROM BASS2.DUAL 
) T      WHERE A.product_item_id = T.BASS2_ID

		count(0),count(distinct product_instance_id)
	from  bass2.ODS_PRODUCT_INS_PROD_20110425 a
	where a.state in ('1','4','6','8','M','7','C','9')
	  and a.user_type_id =1
	  and a.valid_type = 1
	  and a.bill_id not in ('D15289014474','D15289014454')
	  and not exists (	select 1 from bass2.dwd_product_test_phone_20110425 b 
				where a.product_instance_id = b.USER_ID  and b.sts = 1
			 ) 
		20110425 TIME_ID
		,char(a.product_instance_id)  USER_ID
		,bass1.fn_get_all_dim('BASS_STD1_0114',char(a.offer_id)) BASE_PKG_ID
		,replace(char(date(a.create_date)),'-','') VALID_DT
	from  bass2.ODS_PRODUCT_INS_PROD_20110425 a
	where a.state in ('1','4','6','8','M','7','C','9')
	  and a.user_type_id =1
	  and a.valid_type = 1
	  and a.bill_id not in ('D15289014474','D15289014454')
	  and not exists (	select 1 from bass2.dwd_product_test_phone_20110425 b 
				where a.product_instance_id = b.USER_ID  and b.sts = 1
			 ) 
	  and bass1.fn_get_all_dim('BASS_STD1_0114',char(a.offer_id)) is not null 
	            (
		     select user_id from bass1.g_i_02022_day
		      where time_id =20110425
		       except
			  select user_id from bass2.dw_product_20110425
			    where usertype_id in (1,2,9) 
			    and userstatus_id in (1,2,3,6,8)
			    and test_mark<>1               
	            ) as a      
from bass2.ODS_PRODUCT_INS_PROD_20110425 a
	  and a.user_type_id =1
	  and a.valid_type = 1
	  and a.bill_id not in ('D15289014474','D15289014454')
group by product_instance_id having count(0) > 1


CREATE TABLE BASS1.G_I_77780_DAY_DOWN20110429
 (TIME_ID                char(1)         ,
  ENTERPRISE_ID          CHARACTER(20),
  ID                     CHARACTER(9),
  ENTERPRISE_NAME        CHARACTER(60),
  ORG_TYPE               CHARACTER(5),
  ADDR_CODE              CHARACTER(6),
  CITY                   CHARACTER(20),
  REGION                 CHARACTER(20),
  COUNTY                 CHARACTER(20),
  DOOR_NO                CHARACTER(60),
  AREA_CODE              CHARACTER(5),
  PHONE_NO1              CHARACTER(11),
  PHONE_NO2              CHARACTER(10),
  POST_CODE              CHARACTER(6),
  INDUSTRY_TYPE          CHARACTER(4),
  EMPLOYEE_CNT           CHARACTER(8),
  INDUSTRY_UNIT_CNT      CHARACTER(5),
  ECONOMIC_TYPE          CHARACTER(3),
  OPEN_YEAR              CHARACTER(4),
  OPEN_MONTH             CHARACTER(2),
  SHAREHOLDER            CHARACTER(1),
  GROUP_TYPE             CHARACTER(1),
  MANAGE_STYLE           CHARACTER(1),
  OPERATE_REVENUE_CLASS  CHARACTER(2),
  CAPITAL_CLASS          CHARACTER(2),
  INDUSTRY_CLASS_CODE    CHARACTER(2)    ,
  CUST_STATUS            CHARACTER(2),
  CUST_INFO_SRC_ID       CHARACTER(1),
  UPLOAD_TYPE_ID         CHARACTER(1)   
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    ID
   ) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY_DOWN20110429
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
                
select
SEQUENCE_ID
,PROGRAM_TYPE
,'G_I_02022_DAY.tcl' PROGRAM_NAME
,'G_I_02022_DAY.BASS1' SOURCE_DATA
,'G_I_02022_DAY_e' OBJECTIVE_DATA
,'G_I_02022_DAY_f' FINAL_DATA
from bass1.int_program_data
where PROGRAM_NAME = 'G_S_22204_MONTH.tcl'
 where  base_prod_id
in
('111090001319'
,'111090001320'
,'111090001321'
,'111090001322'
,'111090001323'
,'111090001324'
,'111090001325'
,'111090001326'
,'111090001327'
,'111090001328')
 (
	 TIME_ID            	INTEGER             ----��������        
	,USER_ID            	CHAR(20)            ----�û���ʶ ����   
	,ADD_PKG_ID         	CHAR(30)            ----�����ײͱ�ʶ ����
	,VALID_DT           	CHAR(8)             ----�ײ���Ч����      
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID,USER_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_I_02023_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

insert into bass1.int_program_data
select
SEQUENCE_ID
,PROGRAM_TYPE
,'G_I_02023_DAY.tcl' PROGRAM_NAME
,'G_I_02023_DAY.BASS1' SOURCE_DATA
,'G_I_02023_DAY_e' OBJECTIVE_DATA
,'G_I_02023_DAY_f' FINAL_DATA
from bass1.int_program_data
where PROGRAM_NAME = 'G_S_22204_MONTH.tcl'

replace(char(create_date),'-','')
 (
	 TIME_ID            	INTEGER             ----��������        
	,OP_TIME            	CHAR(8)             ----���� ����       
	,QRY_CNT            	CHAR(12)            ----��ѯ�� ��λ���� 
	,CANCEL_CNT         	CHAR(12)            ----�˶��� ��λ���� 
	,CANCEL_FAIL_CNT    	CHAR(12)            ----�˶�ʧ���� ��λ����
	,COMPLAINT_CNT      	CHAR(12)            ----Ͷ���� ��λ���� 
	,CANCEL_BUSI_TYPE_CNT     	CHAR(12)            ----�����˶�ҵ������ ��λ���� ȷ��Ϊҵ�������
--	,ALL_CANCEL_CNT     	CHAR(12)            ----�����˶�ҵ������ ��λ����
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (TIME_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22080_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

             replace(char(date(a.create_date)),'-','') op_time
             ,a.TYCX_QUERY             qry_cnt
             ,a.TYCX_TUIDING           cancel_cnt
             ,a.TYCX_TUIDING_FAIL      cancel_fail_cnt
             ,a.TYCX_TOUSU_LIANG       complaint_cnt
             ,b.tuiding_cnt CANCEL_BUSI_TYPE_CNT
        from bass2.ODS_THREE_ITEM_STAT_20110427 a ,
              (select  replace(char(date(a.create_date)),'-','') op_time,count(distinct a.sp_code) CANCEL_BUSI_TYPE_CNT
                       from   
                       	BASS2.ODS_PRODUCT_UNITE_CANCEL_ORDER_${timestamp} a
                        where a.sts = 1
                        group by replace(char(date(a.create_date)),'-','')
                    ) b 
        where replace(char(date(a.create_date)),'-','') = '20110427' 
				and    replace(char(date(a.create_date)),'-','') = b.op_time
        from bass2.ODS_THREE_ITEM_STAT_20110426 a ,
              (select  replace(char(date(a.create_date)),'-','') op_time
                       from   
                       	BASS2.ODS_PRODUCT_UNITE_CANCEL_ORDER_${timestamp} a
                        where a.sts = 1
                        group by replace(char(date(a.create_date)),'-','')
                    ) b 
        where replace(char(date(a.create_date)),'-','') = '20110426' 
				and    replace(char(date(a.create_date)),'-','') = b.op_time                
             ,replace(char(date(a.create_date)),'-','') op_time
             ,a.TYCX_QUERY             qry_cnt
             ,a.TYCX_TUIDING           cancel_cnt
             ,a.TYCX_TUIDING_FAIL      cancel_fail_cnt
             ,a.TYCX_TOUSU_LIANG       complaint_cnt
             ,b.CANCEL_BUSI_TYPE_CNT 				   CANCEL_BUSI_TYPE_CNT
        from bass2.ODS_THREE_ITEM_STAT_20110426 a ,
              (select  replace(char(date(a.create_date)),'-','') op_time,count(distinct a.sp_code) CANCEL_BUSI_TYPE_CNT
                       from   
                       	BASS2.ODS_PRODUCT_UNITE_CANCEL_ORDER_20110426 a
                        where a.sts = 1
                        group by replace(char(date(a.create_date)),'-','')
                    ) b 
        where replace(char(date(a.create_date)),'-','') = '20110426' 
				and    replace(char(date(a.create_date)),'-','') = b.op_time                
insert into bass1.int_program_data
select
SEQUENCE_ID
,PROGRAM_TYPE
,'G_S_22080_DAY.tcl' PROGRAM_NAME
,'G_S_22080_DAY.BASS1' SOURCE_DATA
,'G_S_22080_DAY_e' OBJECTIVE_DATA
,'G_S_22080_DAY_f' FINAL_DATA
from bass1.int_program_data
where PROGRAM_NAME = 'G_S_22204_MONTH.tcl'



 (
	 TIME_ID            	INTEGER             ----��������        
	,OP_TIME            	CHAR(6)             ----�·� ����       
	,BUSI_CODE          	CHAR(20)            ----ҵ����� ����   
	,BUSI_NAME          	CHAR(60)            ----ҵ������ ����   
	,BUSI_PROVIDER_NAME 	CHAR(60)            ----ҵ���ṩ������  
	,CANCEL_CNT         	CHAR(12)            ----�ɹ��˶���      
	,COMPLAINT_CNT      	CHAR(12)            ----Ͷ����          
	,ORDER_CNT          	CHAR(10)            ----�����û���     
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (OP_TIME
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22081_MONTH
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
CREATE TABLE BASS1.G_S_22081_MONTH_1
 (
	BUSI_CODE varchar(20)
	,ORDER_CNT integer
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (BUSI_CODE
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22081_MONTH_1
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

select
SEQUENCE_ID
,PROGRAM_TYPE
,'G_S_22081_MONTH.tcl' PROGRAM_NAME
,'G_S_22081_MONTH.BASS1' SOURCE_DATA
,'G_S_22081_MONTH_e' OBJECTIVE_DATA
,'G_S_22081_MONTH_f' FINAL_DATA
from bass1.int_program_data
where PROGRAM_NAME = 'G_S_22204_MONTH.tcl'

CREATE TABLE BASS1.G_S_22081_MONTH_2
 (
        SP_CODE                 VARCHAR(20)         
        ,SP_NAME                 VARCHAR(100)    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (SP_CODE
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22081_MONTH_2
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
          201103 TIME_ID
          ,a.op_time
          ,a.BUSI_CODE
          ,a.BUSI_NAME
          ,a.BUSI_PROVIDER_NAME
          ,a.CANCEL_CNT
          ,a.COMPLAINT_CNT
          ,b.ORDER_CNT
        from (
                select   substr(replace(char(date(a.create_date)),'-',''),1,6) op_time
                         ,a.sp_code BUSI_CODE
                         ,a.name BUSI_NAME
                         ,b.sp_name BUSI_PROVIDER_NAME
                         ,sum(case when a.sts = 1 then 1 else 0 end ) CANCEL_CNT
                         ,'0' COMPLAINT_CNT
                         ,'0' ORDER_CNT
                         from  bass2.dw_product_unite_cancel_order_201103 a ,
                                BASS1.G_S_22081_MONTH_2 b 
                         where char(a.sp_id) = b.sp_code
                         group by substr(replace(char(date(a.create_date)),'-',''),1,6)
                          ,a.sp_code
                         ,a.name
                         ,b.sp_name 
             ) a ,
             BASS1.G_S_22081_MONTH_1 b 
        where a.BUSI_CODE = b.BUSI_CODE
                from bass1.G_S_22081_MONTH
                where time_id =201103
                group by OP_TIME||BUSI_CODE||BUSI_NAME having count(0) > 1
where alarmtime >=  current timestamp - 1 days
order by alarmtime desc 
						SELECT PRODUCT_ITEM_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
								WHERE ITEM_TYPE='OFFER_PLAN'
						  AND DEL_FLAG='1'
						  AND SUPPLIER_ID IS NOT NULL
					) b 
CREATE TABLE BASS1.G_S_22081_MONTH_1
 (sp_id VARCHAR(20)
	,BUSI_CODE varchar(20)
	,ORDER_CNT integer
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (BUSI_CODE
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22081_MONTH_1
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
                from bass1.G_S_22081_MONTH
                where time_id =201103
                group by OP_TIME||BUSI_CODE||BUSI_NAME having count(0) > 1
  and usertype_id in (1,2,9) ) u
replace(char(date(a.create_date)),'-','')
,ext1 sp_id
,ext4 sp_busi_code
,count(0) alert_sms_cnt
,count(distinct case when trim(confirm_code) <>'��' and return_message is not null 
,sum(case when trim(confirm_code) <>'��' and  trim(confirm_code) = trim(return_message)  
,0 hotline_out_cnt
,0 complaint_cnt
--,sum(case when return_message is null then 1 else 0 end ) null_cnt
from bass2.ODS_HIS_DSMP_SMS_SEND_MESSAGE_20110427  a 
where  RSP_SEQ LIKE '10086901%' 
and ext4 is not null 
group by 
 replace(char(date(a.create_date)),'-','')
,ext1
,ext4
 (
	 TIME_ID            	INTEGER             ----��������        
	,OP_TIME            	CHAR(8)             ----���� ����       
	,BUSI_BILLING_TYPE  	CHAR (2)            ----ҵ��Ʒ����� ����
	,ALERT_SMS_CNT      	CHAR(12)            ----�۷����ѷ�����  
	,REPLY_SMS_CNT      	CHAR(12)            ----���Żظ���      
	,CANCEL_CNT         	CHAR(12)            ----ҵ��ɹ��˶���  
	,HOTLINE_OUT_CNT    	CHAR(12)            ----��������� ���ڵ����״ζ���72Сʱ��ѵ�ҵ�����ͳһ��0
	,COMPLAINT_CNT      	CHAR(12)            ----Ͷ����     	
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (OP_TIME,BUSI_BILLING_TYPE
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22082_DAY
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
	CREATE TABLE BASS1.G_S_22082_DAY_1
	 (
	 	OP_TIME            	CHAR(8)
		,sp_id VARCHAR(20)
		,BUSI_CODE varchar(20)
		,alert_sms_cnt integer
		,reply_sms_cnt integer
		,cancel_cnt integer
		,hotline_out_cnt integer
		,complaint_cnt integer
	 )
	  DATA CAPTURE NONE
	 IN TBS_APP_BASS1
	 INDEX IN TBS_INDEX
	  PARTITIONING KEY
	   (BUSI_CODE
	   ) USING HASHING;
	
	ALTER TABLE BASS1.G_S_22082_DAY_1
	  LOCKSIZE ROW
	  APPEND OFF
	  NOT VOLATILE;

insert into bass1.int_program_data
select
SEQUENCE_ID
,PROGRAM_TYPE
,'G_S_22082_DAY.tcl' PROGRAM_NAME
,'G_S_22082_DAY.BASS1' SOURCE_DATA
,'G_S_22082_DAY_e' OBJECTIVE_DATA
,'G_S_22082_DAY_f' FINAL_DATA
from bass1.int_program_data
where PROGRAM_NAME = 'G_S_22204_MONTH.tcl'



600902000001132273      600902              	2

2	901808              -UMGFCSD
2	931001              -DZ217
2	931001              -DZ218
2	931002              -TQLS
2	931002              -TQRKZ
2	931002              -TQRKZDQ
2	931002              TQMF
2	931048              -BXGZ
2	931048              -XHKX
2	931048              -XHTY
2	931048              -XHXW


	CREATE TABLE BASS1.G_S_22082_DAY_2
	 (
		sp_id 			VARCHAR(20)
		,BUSI_CODE 	varchar(20)
		,DELAY_TIME              INTEGER(4) 
		,BILL_FLAG               SMALLINT(2)  
	 )
	  DATA CAPTURE NONE
	 IN TBS_APP_BASS1
	 INDEX IN TBS_INDEX
	  PARTITIONING KEY
	   (BUSI_CODE
	   ) USING HASHING;
	
	ALTER TABLE BASS1.G_S_22082_DAY_2
	  LOCKSIZE ROW
	  APPEND OFF
	  NOT VOLATILE;
	CREATE TABLE BASS1.G_S_22082_DAY_2
	 (
		 sp_id 			VARCHAR(20)
		,BUSI_CODE 	    varchar(20)
		,DELAY_TIME              INTEGER
		,BILL_FLAG               SMALLINT 
	 )
	  DATA CAPTURE NONE
	 IN TBS_APP_BASS1
	 INDEX IN TBS_INDEX
	  PARTITIONING KEY
	   (sp_id,BUSI_CODE
	   ) USING HASHING;
	
	ALTER TABLE BASS1.G_S_22082_DAY_2
	  LOCKSIZE ROW
	  APPEND OFF
	  NOT VOLATILE;
				ext1 sp_id
				,ext4 sp_busi_code
				,count(0) alert_sms_cnt
				,count(distinct case when trim(confirm_code) <>'��' 
					and return_message is not null 
				      then bill_id||RSP_SEQ||ext1||ext4 end ) reply_sms_cnt
				,sum(case when trim(confirm_code) <>'��' and  trim(confirm_code) = trim(return_message)  
					then 1 else 0 end ) cancel_cnt
				,0 hotline_out_cnt
				,0 complaint_cnt
				from  bass2.DW_HIS_DSMP_SMS_SEND_MESSAGE_DM_201104  a 
				where  RSP_SEQ LIKE '10086901%' 
				and ext4 is not null 
				group by 
				 replace(char(date(a.create_date)),'-','')
				,ext1
				,ext4
			 with ur 

                                                ,count(distinct a.sp_code) CANCEL_BUSI_TYPE_CNT
                       from   
                        BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_201104 a
                        where a.sts = 1
                        and replace(char(date(a.create_date)),'-','') =  '20110427'  
                        group by replace(char(date(a.create_date)),'-','')
             ,replace(char(date(a.create_date)),'-','') op_time
             ,char(a.TYCX_QUERY)             qry_cnt
             ,char(a.TYCX_TUIDING)           cancel_cnt
             ,char(a.TYCX_TUIDING_FAIL)      cancel_fail_cnt
             ,char(a.TYCX_TOUSU_LIANG)       complaint_cnt
            -- ,char(b.CANCEL_BUSI_TYPE_CNT)       CANCEL_BUSI_TYPE_CNT
        from  bass2.DW_THREE_ITEM_STAT_DM_201104 a ,
              (select  replace(char(date(a.create_date)),'-','') op_time
                                                ,count(distinct a.sp_code) CANCEL_BUSI_TYPE_CNT
                       from   
                        BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_201104 a
                        where a.sts = 1
                        and replace(char(date(a.create_date)),'-','') =  '20110427'  
                        group by replace(char(date(a.create_date)),'-','')
                    ) b 
        where replace(char(date(a.create_date)),'-','') = '20110427' 
                                and    replace(char(date(a.create_date)),'-','') = b.op_time
         'dim_acct_item' XZBAS_TBNAME
        ,'���������»�����' XZBAS_COLNAME
        ,'80000104' XZBAS_VALUE
        ,'��Ŀ��Ŀ' BASS1_TBN_DESC
        ,'BASS_STD1_0074' BASS1_TBID
        ,'0626' BASS1_VALUE
        ,'GPRS�ײͷ�' BASS1_VALUE_DESC
        from bass2.dim_acct_item a 
        where item_id = 80000104
insert into BASS1.ALL_DIM_LKP
(
         XZBAS_TBNAME
        ,XZBAS_COLNAME
        ,XZBAS_VALUE
        ,BASS1_TBN_DESC
        ,BASS1_TBID
        ,BASS1_VALUE
        ,BASS1_VALUE_DESC
)
select 
         'dim_acct_item' XZBAS_TBNAME
        ,'G3�����������ײ�������' XZBAS_COLNAME
        ,'80000531' XZBAS_VALUE
        ,'��Ŀ��Ŀ' BASS1_TBN_DESC
        ,'BASS_STD1_0074' BASS1_TBID
        ,'0627' BASS1_VALUE
        ,'GPRSͨ�ŷ�' BASS1_VALUE_DESC
        from bass2.dim_acct_item a 
        where item_id = 80000531
;



insert into BASS1.ALL_DIM_LKP
(
         XZBAS_TBNAME
        ,XZBAS_COLNAME
        ,XZBAS_VALUE
        ,BASS1_TBN_DESC
        ,BASS1_TBID
        ,BASS1_VALUE
        ,BASS1_VALUE_DESC
)
select 
         'dim_acct_item' XZBAS_TBNAME
        ,'G3�����������ײ�������' XZBAS_COLNAME
        ,'80000462' XZBAS_VALUE
        ,'��Ŀ��Ŀ' BASS1_TBN_DESC
        ,'BASS_STD1_0074' BASS1_TBID
        ,'0627' BASS1_VALUE
        ,'GPRSͨ�ŷ�' BASS1_VALUE_DESC
        from bass2.dim_acct_item a 
        where item_id = 80000462
;



insert into BASS1.ALL_DIM_LKP
(
         XZBAS_TBNAME
        ,XZBAS_COLNAME
        ,XZBAS_VALUE
        ,BASS1_TBN_DESC
        ,BASS1_TBID
        ,BASS1_VALUE
        ,BASS1_VALUE_DESC
)
select 
         'dim_acct_item' XZBAS_TBNAME
        ,'�����ײ�����������' XZBAS_COLNAME
        ,'80000078' XZBAS_VALUE
        ,'��Ŀ��Ŀ' BASS1_TBN_DESC
        ,'BASS_STD1_0074' BASS1_TBID
        ,'0627' BASS1_VALUE
        ,'GPRSͨ�ŷ�' BASS1_VALUE_DESC
        from bass2.dim_acct_item a 
        where item_id = 80000078
;




insert into BASS1.ALL_DIM_LKP
(
         XZBAS_TBNAME
        ,XZBAS_COLNAME
        ,XZBAS_VALUE
        ,BASS1_TBN_DESC
        ,BASS1_TBID
        ,BASS1_VALUE
        ,BASS1_VALUE_DESC
)
select 
         'dim_acct_item' XZBAS_TBNAME
        ,'���������»�����' XZBAS_COLNAME
        ,'80000104' XZBAS_VALUE
        ,'��Ŀ��Ŀ' BASS1_TBN_DESC
        ,'BASS_STD1_0074' BASS1_TBID
        ,'0626' BASS1_VALUE
        ,'GPRS�ײͷ�' BASS1_VALUE_DESC
        from bass2.dim_acct_item a 
        where item_id = 80000104


select * from BASS1.ALL_DIM_LKP
where BASS1_TBID = 'BASS_STD1_0074'
and bass1_value in ('0626','0627')
table (
select '00991399X' id ,       '89301560001719' ent_id,      '3'  type from bass2.dual union all
select '00991399X' id ,       '89308931002159' ent_id,      '1'  type from bass2.dual union all
select '219663597' id ,       '89403001180810' ent_id,      '2'  type from bass2.dual union all
select '219663597' id ,       '89401560001290' ent_id,      '1'  type from bass2.dual union all
select '433205933' id ,       '89403001180136' ent_id,      '2'  type from bass2.dual union all
select '433205933' id ,       '89401560000761' ent_id,      '1'  type from bass2.dual union all
select '433208683' id ,       '89403000939855' ent_id,      '2'  type from bass2.dual union all
select '433208683' id ,       '89403001062209' ent_id,      '1'  type from bass2.dual union all
select '71091507X' id ,       '89301560001340' ent_id,      '3'  type from bass2.dual union all
select '71091507X' id ,       '              ' ent_id,      '2'  type from bass2.dual union all
select '724901576' id ,       '89303001627014' ent_id,      '3'  type from bass2.dual union all
select '724901576' id ,       '89303000084579' ent_id,      '1'  type from bass2.dual union all
select '724903408' id ,       '89403001201978' ent_id,      '2'  type from bass2.dual union all
select '724903408' id ,       '89401560000346' ent_id,      '1'  type from bass2.dual union all
select '741930838' id ,       '89302999049682' ent_id,      '2'  type from bass2.dual union all
select '741930838' id ,       '89303001225874' ent_id,      '1'  type from bass2.dual union all
select 'DX0908507' id ,       '89401560001112' ent_id,      '1'  type from bass2.dual union all
select 'DX0908507' id ,       '89403001395933' ent_id,      '2'  type from bass2.dual union all
select 'DX0915539' id ,       '89403001180125' ent_id,      '2'  type from bass2.dual union all
select 'DX0915539' id ,       '89403001424049' ent_id,      '1'  type from bass2.dual union all
select 'DX0927142' id ,       '89403001395728' ent_id,      '2'  type from bass2.dual union all
select 'DX0927142' id ,       '89403000162592' ent_id,      '1'  type from bass2.dual union all
select 'DX0932398' id ,       '89401560000442' ent_id,      '1'  type from bass2.dual union all
select 'DX0932398' id ,       '89403001180944' ent_id,      '2'  type from bass2.dual union all
select 'K39846332' id ,       '89303001232669' ent_id,      '3'  type from bass2.dual union all
select 'K39846332' id ,       '89302999633984' ent_id,      '1'  type from bass2.dual union all
select 'XZLZK0043' id ,       '89103001051734' ent_id,      '2'  type from bass2.dual union all
select 'XZLZK0043' id ,       '89102999829280' ent_id,      '1'  type from bass2.dual union all
select 'XZLZK0044' id ,       '89102999086604' ent_id,      '1'  type from bass2.dual union all
select 'XZLZK0044' id ,       '89103001061802' ent_id,      '2'  type from bass2.dual
) t , 
             ,replace(char(date(a.create_date)),'-','') op_time
             ,char(a.TYCX_QUERY)             qry_cnt
             ,char(a.TYCX_TUIDING)           cancel_cnt
             ,char(a.TYCX_TUIDING_FAIL)      cancel_fail_cnt
             ,char(a.TYCX_TOUSU_LIANG)       complaint_cnt
             --,char(b.CANCEL_BUSI_TYPE_CNT)       CANCEL_BUSI_TYPE_CNT
        from  bass2.DW_THREE_ITEM_STAT_DM_201104 a
             ,replace(char(date(a.create_date)),'-','') op_time
             ,char(a.TYCX_QUERY)             qry_cnt
             ,char(a.TYCX_TUIDING)           cancel_cnt
             ,char(a.TYCX_TUIDING_FAIL)      cancel_fail_cnt
             ,char(a.TYCX_TOUSU_LIANG)       complaint_cnt
             ,char(b.CANCEL_BUSI_TYPE_CNT)       CANCEL_BUSI_TYPE_CNT
        from  bass2.DW_THREE_ITEM_STAT_DM_201104 a ,
              (select  replace(char(date(a.create_date)),'-','') op_time
                                                ,count(distinct a.sp_code) CANCEL_BUSI_TYPE_CNT
                       from   
                        BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_201104 a
                        where a.sts = 1
                        and replace(char(date(a.create_date)),'-','') =  '20110428'  
                        group by replace(char(date(a.create_date)),'-','')
                    ) b 
        where replace(char(date(a.create_date)),'-','') = '20110428' 
                                and    replace(char(date(a.create_date)),'-','') = b.op_time
        ,TYCX_QUERY
        ,TYCX_TUIDING
        ,TYCX_TUIDING_FAIL
        ,TYCX_TUIDING_AVG
        --,TYCX_FIRST20_BUSI_NAME
        ,TYCX_TOUSU_LIANG
        ,KOUFEI_TIXING
        ,KOUFEI_DXHFL
        ,KOUFEI_REXWH
        ,KOUFEI_TUIDINGL
        --,KOUFEI_TDFIRST20_NAME
        ,KOUFEI_TOUSU_LIAN
        ,MW_SHOULILIANG
        ,MW_TUIFEI
        --,MW_ZYFIRST20_NAME
        ,CREATE_DATE
        ,TYCX_TUIDING
        ,TYCX_TUIDING_FAIL
        ,TYCX_TUIDING_AVG
        --,TYCX_FIRST20_BUSI_NAME
        ,TYCX_TOUSU_LIANG
        ,KOUFEI_TIXING
        ,KOUFEI_DXHFL
        ,KOUFEI_REXWH
        ,KOUFEI_TUIDINGL
        --,KOUFEI_TDFIRST20_NAME
        ,KOUFEI_TOUSU_LIAN
        ,MW_SHOULILIANG
        ,MW_TUIFEI
        --,MW_ZYFIRST20_NAME
        ,CREATE_DATE
                                                ,count(distinct a.sp_code) CANCEL_BUSI_TYPE_CNT
                       from   
                        BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_201104 a
                        where a.sts = 1
                        and replace(char(date(a.create_date)),'-','') =  '20110428'  
                        group by replace(char(date(a.create_date)),'-','')
select * from    bass1.G_S_22082_DAY
CREATE TABLE BASS1.G_I_77780_DAY_SNAP_DOWN20110429
 (TIME_ID                char(1)         ,
  PROV_ID          CHARACTER(5), 
  ENTERPRISE_ID          CHARACTER(20),
  USER_ID                     CHARACTER(20),
  SNAP_INCOME        INTEGER
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    USER_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY_SNAP_DOWN20110429
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;                        
drop table  BASS1.G_I_77780_DAY_SNAP_DOWN20110429
CREATE TABLE BASS1.G_I_77780_DAY_SNAP_DOWN20110429
 (TIME_ID                char(1)         ,
  op_time							char(6),
  PROV_ID          CHARACTER(5), 
  ENTERPRISE_ID          CHARACTER(20),
  USER_ID                     CHARACTER(20),
  SNAP_INCOME        INTEGER
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    USER_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY_SNAP_DOWN20110429
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

CREATE TABLE BASS1.G_I_77780_DAY_SNAP_DOWN20110429
 (TIME_ID                char(1)         ,
  op_time							char(6),
  PROV_ID          CHARACTER(5), 
  ENTERPRISE_ID          CHARACTER(20),
  USER_ID                     CHARACTER(20),
  SNAP_INCOME        decimal(10,2)
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (ENTERPRISE_ID,
    USER_ID
   ) USING HASHING;

ALTER TABLE BASS1.G_I_77780_DAY_SNAP_DOWN20110429
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
values
('bass2.DIM_PROD_UP_PRODUCT_ITEM','ȫ��ͨר�����Ű�','111090001363','ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ','BASS_STD1_0114','QW_QQT_DJ_DX0001','ȫ��ͨȫ��ͳһ�ʷ��ײ�ר�����ݰ�-���Ű�')
,('bass2.DIM_PROD_UP_PRODUCT_ITEM','ȫ��ͨר�����Ű�','111090001364','ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ','BASS_STD1_0114','QW_QQT_DJ_CX0001','ȫ��ͨȫ��ͳһ�ʷ��ײ�ר�����ݰ�-���Ű�')
,('bass2.DIM_PROD_UP_PRODUCT_ITEM','ȫ��ͨר��������','111090001365','ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ','BASS_STD1_0114','QW_QQT_DJ_ZX0001','ȫ��ͨȫ��ͳһ�ʷ��ײ�ר�����ݰ�-ȫ��ͨ������')
,('bass2.DIM_PROD_UP_PRODUCT_ITEM','ȫ��ͨר���Ķ���','111090001366','ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ','BASS_STD1_0114','QW_QQT_DJ_YD0001','ȫ��ͨȫ��ͳһ�ʷ��ײ�ר�����ݰ�-ȫ��ͨ�Ķ���')
,('bass2.DIM_PROD_UP_PRODUCT_ITEM','ȫ��ͨר�����ְ�','111090001367','ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ','BASS_STD1_0114','QW_QQT_DJ_YY0001','ȫ��ͨȫ��ͳһ�ʷ��ײ�ר�����ݰ�-ȫ��ͨ���ְ�')
,('bass2.DIM_PROD_UP_PRODUCT_ITEM','ȫ��ͨר�������Ѷ��','111090001368','ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ','BASS_STD1_0114','QW_QQT_DJ_FHZX0001','ȫ��ͨȫ��ͳһ�ʷ��ײ�ר�����ݰ�-ȫ��ͨ�����Ѷ��')

insert into bass1.int_program_data
select
SEQUENCE_ID
,PROGRAM_TYPE
,'G_I_02023_DAY.tcl' PROGRAM_NAME
,'G_I_02023_DAY.BASS1' SOURCE_DATA
,'G_I_02023_DAY_e' OBJECTIVE_DATA
,'G_I_02023_DAY_f' FINAL_DATA
from bass1.int_program_data
where PROGRAM_NAME = 'G_S_22204_MONTH.tcl'



                count(0)
                from  bass2.Dw_product_ins_off_ins_prod_ds a 
                ,                bass2.dw_product_20110428 b
                    where b.usertype_id in (1,2,9) 
                    and b.userstatus_id in (1,2,3,6,8)
                    and b.test_mark<>1
                          and a.OP_TIME = '2011-04-28'
                          and a.state=1
                    and date(a.VALID_DATE)<='2011-04-28'
                    and a.valid_type = 1
	CREATE TABLE BASS1.G_I_02023_DAY_1
	 (
			 USER_ID            	CHAR(20)            ----�û���ʶ ����   
			,ADD_PKG_ID         	CHAR(30)            ----�����ײͱ�ʶ ����
			,VALID_DT           	CHAR(8)             ----�ײ���Ч����      
	 )
	  DATA CAPTURE NONE
	 IN TBS_APP_BASS1
	 INDEX IN TBS_INDEX
	  PARTITIONING KEY
	   (USER_ID
	   ) USING HASHING;
	
	ALTER TABLE BASS1.G_I_02023_DAY_1
	  LOCKSIZE ROW
	  APPEND OFF
	  NOT VOLATILE;
                                USER_ID
                                ,ADD_PKG_ID
                                ,VALID_DT
                        FROM (
                                SELECT
                                 a.PRODUCT_INSTANCE_ID as USER_ID
                                ,bass1.fn_get_all_dim('BASS_STD1_0115',char(a.offer_id)) as ADD_PKG_ID
                                ,replace(char(date(a.VALID_DATE)),'-','') as VALID_DT 
                                ,row_number()over(partition by a.PRODUCT_INSTANCE_ID,a.offer_id order by EXPIRE_DATE desc ,VALID_DATE desc  ) rn 
                                from  bass2.Dw_product_ins_off_ins_prod_ds a 
                                     where a.offer_id = 112094500001 
                                 and a.OP_TIME = '2011-04-28'
                                 and date(a.VALID_DATE)<='2011-04-28'
                            ) AS T where t.rn = 1 
                         with ur 

CREATE TABLE BASS2.DIM_TERM_TAC
 (ID             INTEGER,
  TAC_NUM        VARCHAR(15),
  TERM_ID        VARCHAR(10),
  TERM_MODEL     VARCHAR(50),
  TERMPROD_ID    VARCHAR(10),
  TERMPROD_NAME  VARCHAR(200),
  NET_TYPE       CHARACTER(1),
  TERM_TYPE      CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_DIM
  PARTITIONING KEY
   (ID
   ) USING HASHING;

ALTER TABLE BASS2.DIM_TERM_TAC
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
drop table BASS2.DIM_TERM_TAC


drop table BASS2.DIM_TERM_TAC_0430
CREATE TABLE BASS2.DIM_TERM_TAC_0430
 (ID             INTEGER,
  TAC_NUM        VARCHAR(15),
  TERM_ID        VARCHAR(10),
  TERM_MODEL     VARCHAR(50),
  TERMPROD_ID    VARCHAR(10),
  TERMPROD_NAME  VARCHAR(200),
  NET_TYPE       CHARACTER(1),
  TERM_TYPE      CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_DIM
  PARTITIONING KEY
   (ID
   ) USING HASHING;
ALTER TABLE BASS2.DIM_TERM_TAC_0430
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
  
  

 (ID             INTEGER,
  TAC_NUM        VARCHAR(15),
  TERM_ID        VARCHAR(10),
  TERM_MODEL     VARCHAR(50),
  TERMPROD_ID    VARCHAR(10),
  TERMPROD_NAME  VARCHAR(200),
  NET_TYPE       CHARACTER(1),
  TERM_TYPE      CHARACTER(1)
 )
  DATA CAPTURE NONE
 IN TBS_DIM
  PARTITIONING KEY
   (ID
   ) USING HASHING;
ALTER TABLE BASS2.DIM_TERM_TAC
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;

delete from BASS2.DIM_TERM_TAC 
insert into BASS2.DIM_TERM_TAC
select * from BASS2.DIM_TERM_TAC_0430

 insert into BASS2.DIM_TERM_TAC
select 
ID,
TAC_NUM,
TERM_ID,
TERM_MODEL,
TERMPROD_ID,
TERMPROD_NAME,
NET_TYPE,
TERM_TYPE from BASS2.DIM_TERM_TAC_20110430BAK
where net_type <>'2';
commit;
select tac_nuM,count(*) from BASS2.DIM_TERM_TAC_20110331BAK
group by tac_nuM
having count(*)>1
control_code in (select  control_code from   app.sch_control_runlog where flag= 1)
group by time_id 

select product_no,count(0),count(distinct user_id) from   bass1.g_a_02004_day 
where time_id = 20110429
group by PRODUCT_NO 

select product_no,count(0) from   bass1.g_a_02004_day 
where time_id = 20110429
group by PRODUCT_NO having count(0) > 1


select user_id,count(0) from   bass1.g_a_02004_day 
where time_id = 20110429
group by user_id having count(0) > 1

select count(0),count(distinct a.product_no),count(distinct b.user_id) from   bass1.g_a_02004_day a
, bass1.g_a_02008_day b 
where a.time_id = 20110429
and b.time_id = 20110429
and a.user_id = b.user_id 


select count(0),count(distinct a.product_no),count(distinct a.user_id) from   bass1.g_a_02004_day a
where a.time_id = 20110428

select count(0),count(distinct a.product_no),count(distinct b.user_id) from   bass1.g_a_02004_day a
, bass1.g_a_02008_day b 
where a.time_id = 20110428
and b.time_id = 20110428
and a.user_id = b.user_id 


select * from G_S_22302_DAY

select count(0),count(distinct enterprise_id) from G_S_22302_DAY where time_id in (20110423,20110424)

select * from BASS1.MON_ALL_INTERFACE a where a.INTERFACE_CODE = '04008'

 
 select * from  app.sch_control_before where control_code = 'BASS1_INT_CHECK_Z345_DAY.tcl'
 select * from  app.sch_control_before where before_control_code = 'BASS1_INT_CHECK_Z345_DAY.tcl'
 BASS1_INT_CHECK_Z345_DAY.tcl	int -s INT_CHECK_Z345_DAY.tcl	2	׼ȷ��ָ��R022�����еش��û��������䶯�ʳ������ſ��˷�Χ	2011-04-27 3:09:29.549279	[NULL]	-1	[NULL]
BASS1_INT_CHECK_Z345_DAY.tcl	int -s INT_CHECK_Z345_DAY.tcl	2	׼ȷ��ָ��R021���������û��������䶯�ʳ������ſ��˷�Χ	2011-04-27 3:07:10.948531	[NULL]	-1	[NULL]

select * from   BASS1.G_RULE_CHECK  where rule_code in ('R021')
and time_id > 20110401

select * from   BASS1.G_RULE_CHECK  where rule_code in ('R021')
and time_id > 20110401


select * from   BASS1.G_RULE_CHECK  where rule_code in ('R021')
and time_id > 20110401
20110426	R021	929980.00000	953866.00000	-0.02504	0.00000
20110425	R021	953866.00000	954734.00000	-0.00090	0.00000

select * from   BASS1.G_RULE_CHECK  where rule_code in ('R022')
and time_id in (20110425,20110426)


20110426	R022	373681.00000	386079.00000	-0.03211	0.00000
20110425	R022	386079.00000	388145.00000	-0.00532	0.00000

select count(0) from   g_a_02004_day where time_id = 20110426

select count(0) from   g_a_02008_day where time_id = 20110426



R159_2
select * from   BASS1.G_RULE_CHECK  where rule_code in ('R159_2')
and time_id in (20110425,20110426)

select * from   BASS1.G_RULE_CHECK  where rule_code in ('C1')
and time_id > 20110425

20110428	C1	2155029.00000	1990225.00000	0.08281	0.00000
20110427	C1	1990225.00000	1987809.00000	0.00122	0.00000
20110426	C1	1987809.00000	2017461.00000	-0.01470	0.00000

	CREATE TABLE BASS1.G_S_22083_MONTH_1
	 (
		sp_id VARCHAR(20)
		,BUSI_CODE varchar(20)
		,cancel_cnt integer
		,complaint_cnt integer
	 )
	  DATA CAPTURE NONE
	 IN TBS_APP_BASS1
	 INDEX IN TBS_INDEX
	  PARTITIONING KEY
	   (sp_id,BUSI_CODE
	   ) USING HASHING;
	
	ALTER TABLE BASS1.G_S_22083_MONTH_1
	  LOCKSIZE ROW
	  APPEND OFF
	  NOT VOLATILE;
select
SEQUENCE_ID
,PROGRAM_TYPE
,'G_S_22083_MONTH.tcl' PROGRAM_NAME
,'G_S_22083_MONTH.BASS1' SOURCE_DATA
,'G_S_22083_MONTH_e' OBJECTIVE_DATA
,'G_S_22083_MONTH_f' FINAL_DATA
from bass1.int_program_data
where PROGRAM_NAME = 'G_S_22204_MONTH.tcl'

 (
	 TIME_ID            	INTEGER             ----��������        
	,OP_TIME            	CHAR(6)             ----�·� ����       
	,BUSI_CODE          	CHAR(20)            ----ҵ����� ����   
	,BUSI_NAME          	CHAR(60)            ----ҵ������ ����   
	,BUSI_PROVIDER_NAME 	CHAR(60)            ----ҵ���ṩ������  
	,BUSI_BILLING_TYPE  	CHAR (2)            ----ҵ��Ʒ�����    
	,CANCEL_CNT         	CHAR(12)            ----�ɹ��˶���      
	,COMPLAINT_CNT      	CHAR(12)            ----Ͷ����          
	,ORDER_CNT          	CHAR(10)            ----�����û���    
 )
  DATA CAPTURE NONE
 IN TBS_APP_BASS1
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   (OP_TIME,BUSI_CODE,BUSI_NAME
   ) USING HASHING;

ALTER TABLE BASS1.G_S_22083_MONTH
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;


                BILL_ID,
                RSP_SEQ,
                MESSAGE_TYPE,
                SRCTYPE,
                OPTCODE,
                CREATE_DATE,
                FLAG,
                SEND_DATE,
                MSP_END_DATE,
                DONE_DATE,
                CONFIRM_TYPE,
                CONFIRM_CODE,
                RETURN_MESSAGE,
                RETURN_DATE,
                EXTEND_SEQ,
                EXT1,
                EXT2,
                EXT3,
                EXT4
	CREATE TABLE BASS1.G_S_22083_MONTH_2
	 (
		 sp_id 			     VARCHAR(20)
		,BUSI_CODE 	     varchar(20)
		,DELAY_TIME      INTEGER
		,BILL_FLAG       SMALLINT 
	 )
	  DATA CAPTURE NONE
	 IN TBS_APP_BASS1
	 INDEX IN TBS_INDEX
	  PARTITIONING KEY
	   (sp_id,BUSI_CODE
	   ) USING HASHING;
	
	ALTER TABLE BASS1.G_S_22083_MONTH_2
	  LOCKSIZE ROW
	  APPEND OFF
	  NOT VOLATILE;

select op_time , count(0) 
--,  count(distinct op_time ) 
from bass2.DW_HIS_DSMP_SMS_SEND_MESSAGE_DM_201104 
group by  op_time 
order by 1 


                BILL_ID,
                RSP_SEQ,
                MESSAGE_TYPE,
                SRCTYPE,
                OPTCODE,
                CREATE_DATE,
                FLAG,
                SEND_DATE,
                MSP_END_DATE,
                DONE_DATE,
                CONFIRM_TYPE,
                CONFIRM_CODE,
                RETURN_MESSAGE,
                RETURN_DATE,
                EXTEND_SEQ,
                EXT1,
                EXT2,
                EXT3,
                EXT4
--,  count(distinct STATE ) 
from bass2.ODS_DIM_UP_SP_SERVICE_20110428 
group by  STATE 
order by 1 

--,  count(distinct DEL_FLAG ) 
from bass2.ODS_DIM_UP_SP_SERVICE_20110428 
group by  DEL_FLAG 
order by 1 

	CREATE TABLE BASS1.G_S_22083_MONTH_3
	 (
	        SP_CODE                 VARCHAR(20)         
	        ,SP_NAME                 VARCHAR(100)    
	 )
	  DATA CAPTURE NONE
	 IN TBS_APP_BASS1
	 INDEX IN TBS_INDEX
	  PARTITIONING KEY
	   (SP_CODE
	   ) USING HASHING;
	
	ALTER TABLE BASS1.G_S_22083_MONTH_3
	  LOCKSIZE ROW
	  APPEND OFF
	  NOT VOLATILE;
from (
                                                select valid_date,OFFER_ID,count(0)  order_cnt
                                                from bass2.DW_PRODUCT_INS_OFF_INS_PROD_201103 p ,
                                                (select distinct user_id from bass2.dw_product_201103
                                                        where userstatus_id in (1,2,3,6,8)
                                                and usertype_id in (1,2,9) 
                                        ) u
                                                where valid_date < '2011-03-01' and expire_date >  '2011-03-01'
                                                and p.PRODUCT_INSTANCE_ID = u.user_id
                                                group by valid_date,OFFER_ID
                                        ) a ,
                                        (
                                                SELECT PRODUCT_ITEM_ID,SUPPLIER_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
                                                                WHERE ITEM_TYPE='OFFER_PLAN'
                                                  AND DEL_FLAG='1'
                                                  AND SUPPLIER_ID IS NOT NULL
                                        ) b 
                                where a.OFFER_ID = b.PRODUCT_ITEM_ID
group by      valid_date
                           
from (
                                                select valid_date,OFFER_ID,count(0)  order_cnt
                                                from bass2.DW_PRODUCT_INS_OFF_INS_PROD_201103 p ,
                                                (select distinct user_id from bass2.dw_product_201103
                                                        where userstatus_id in (1,2,3,6,8)
                                                and usertype_id in (1,2,9) 
                                        ) u
                                                where valid_date < '2011-03-01' and expire_date >  '2011-03-01'
                                                and p.PRODUCT_INSTANCE_ID = u.user_id
                                                group by valid_date,OFFER_ID
                                        ) a ,
                                        (
                                                SELECT PRODUCT_ITEM_ID,SUPPLIER_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
                                                                WHERE ITEM_TYPE='OFFER_PLAN'
                                                  AND DEL_FLAG='1'
                                                  AND SUPPLIER_ID IS NOT NULL
                                        ) b 
                                where a.OFFER_ID = b.PRODUCT_ITEM_ID
group by      valid_date
                           
                                                from bass2.DW_PRODUCT_INS_OFF_INS_PROD_201103
from (
                        select OFFER_ID,valid_date,expire_date,CREATE_DATE
                        from bass2.DW_PRODUCT_INS_OFF_INS_PROD_201103 p ,
                        (select distinct user_id from bass2.dw_product_201103
                                where userstatus_id in (1,2,3,6,8)
                        and usertype_id in (1,2,9) 
                ) u
                        where valid_date < expire_date and expire_date > date(current timestamp)
                ) a ,
                (
                        SELECT PRODUCT_ITEM_ID,SUPPLIER_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
                                        WHERE ITEM_TYPE='OFFER_PLAN'
                          AND DEL_FLAG='1'
                          AND SUPPLIER_ID IS NOT NULL
                ) b 
        where a.OFFER_ID = b.PRODUCT_ITEM_ID
                        select OFFER_ID,valid_date,expire_date,CREATE_DATE
                        from bass2.DW_PRODUCT_INS_OFF_INS_PROD_201103 p ,
                        (select distinct user_id from bass2.dw_product_201103
                                where userstatus_id in (1,2,3,6,8)
                        and usertype_id in (1,2,9) 
                ) u
                        where valid_date < expire_date and expire_date > date(current timestamp)
                ) a ,
                (
                        SELECT PRODUCT_ITEM_ID,SUPPLIER_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
                                        WHERE ITEM_TYPE='OFFER_PLAN'
                          AND DEL_FLAG='1'
                          AND SUPPLIER_ID IS NOT NULL
                ) b 
        where a.OFFER_ID = b.PRODUCT_ITEM_ID
	CREATE TABLE BASS1.G_S_22083_MONTH_4
	 (sp_id VARCHAR(20)
		,BUSI_CODE varchar(20)
		,ORDER_CNT integer
	 )
	  DATA CAPTURE NONE
	 IN TBS_APP_BASS1
	 INDEX IN TBS_INDEX
	  PARTITIONING KEY
	   (BUSI_CODE
	   ) USING HASHING;
	
	ALTER TABLE BASS1.G_S_22083_MONTH_4
	  LOCKSIZE ROW
	  APPEND OFF
	  NOT VOLATILE;
    
								WHERE ITEM_TYPE='OFFER_PLAN'
						  AND DEL_FLAG='1'
						  AND SUPPLIER_ID IS NOT NULL
                        from   bass1.G_S_22083_MONTH_1 a 
                               join  bass1.G_S_22083_MONTH_2 b  on a.SP_ID = b.SP_ID and a.BUSI_CODE = b.BUSI_CODE
                join (
								WHERE ITEM_TYPE='OFFER_PLAN'
						  AND DEL_FLAG='1'
						  AND SUPPLIER_ID IS NOT NULL
                                JOIN BASS1.G_S_22083_MONTH_3 D ON a.sp_id = d.SP_CODE
                                left join BASS1.G_S_22083_MONTH_4 e on a.sp_id = e.SP_ID and a.BUSI_CODE = b.BUSI_CODE
                        group by 
                     a.BUSI_CODE
                     ,c.OPERATOR_NAME 
                     ,d.SP_NAME
                     ,case when b.bill_flag = 3 and DELAY_TIME = 72 then '11' 
                                when b.bill_flag = 3 and b.DELAY_TIME = 0 then '12'
                                                                else '20' end   
                        from   bass1.G_S_22083_MONTH_1 a 
                               join  bass1.G_S_22083_MONTH_2 b  on a.SP_ID = b.SP_ID and a.BUSI_CODE = b.BUSI_CODE
               left  join (
								WHERE ITEM_TYPE='OFFER_PLAN'
						  AND DEL_FLAG='1'
						  AND SUPPLIER_ID IS NOT NULL
bass2.DIM_PROD_UP_PRODUCT_ITEM	ȫ��ͨר�����Ű�	111090001364	ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ	BASS_STD1_0114	QW_QQT_DJ_CX0001	ȫ��ͨȫ��ͳһ�ʷ��ײ�ר�����ݰ�-���Ű�			
bass2.DIM_PROD_UP_PRODUCT_ITEM	ȫ��ͨר��������	111090001365	ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ	BASS_STD1_0114	QW_QQT_DJ_ZX0001	ȫ��ͨȫ��ͳһ�ʷ��ײ�ר�����ݰ�-ȫ��ͨ������			
bass2.DIM_PROD_UP_PRODUCT_ITEM	ȫ��ͨר���Ķ���	111090001366	ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ	BASS_STD1_0114	QW_QQT_DJ_YD0001	ȫ��ͨȫ��ͳһ�ʷ��ײ�ר�����ݰ�-ȫ��ͨ�Ķ���			
bass2.DIM_PROD_UP_PRODUCT_ITEM	ȫ��ͨר�����ְ�	111090001367	ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ	BASS_STD1_0114	QW_QQT_DJ_YY0001	ȫ��ͨȫ��ͳһ�ʷ��ײ�ר�����ݰ�-ȫ��ͨ���ְ�			
bass2.DIM_PROD_UP_PRODUCT_ITEM	ȫ��ͨר�������Ѷ��	111090001368	ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײͱ�ʶ	BASS_STD1_0114	QW_QQT_DJ_FHZX0001	ȫ��ͨȫ��ͳһ�ʷ��ײ�ר�����ݰ�-ȫ��ͨ�����Ѷ��			

QW_QQT_JC_SW88	ȫ��ͨȫ��ͳһ�ʷ������ײ�88Ԫ
QW_QQT_JC_SW128	ȫ��ͨȫ��ͳһ�ʷ������ײ�128Ԫ
QW_QQT_JC_SL58	ȫ��ͨȫ��ͳһ�ʷ������ײ�58Ԫ
QW_QQT_JC_SL88	ȫ��ͨȫ��ͳһ�ʷ������ײ�88Ԫ
QW_QQT_JC_SL128	ȫ��ͨȫ��ͳһ�ʷ������ײ�128Ԫ
QW_QQT_JC_SL158	ȫ��ͨȫ��ͳһ�ʷ������ײ�158Ԫ
QW_QQT_JC_SL188	ȫ��ͨȫ��ͳһ�ʷ������ײ�188Ԫ
QW_QQT_JC_SL288	ȫ��ͨȫ��ͳһ�ʷ������ײ�288Ԫ
QW_QQT_JC_SL388	ȫ��ͨȫ��ͳһ�ʷ������ײ�388Ԫ
QW_QQT_JC_SL588	ȫ��ͨȫ��ͳһ�ʷ������ײ�588Ԫ
QW_QQT_JC_SL888	ȫ��ͨȫ��ͳһ�ʷ������ײ�888Ԫ
QW_QQT_JC_BD58	ȫ��ͨȫ��ͳһ�ʷѱ����ײ�58Ԫ
QW_QQT_JC_BD88	ȫ��ͨȫ��ͳһ�ʷѱ����ײ�88Ԫ
QW_QQT_JC_BD128	ȫ��ͨȫ��ͳһ�ʷѱ����ײ�128Ԫ
     
select 'QW_QQT_JC_SW58' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�58Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SW88' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�88Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SW128' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�128Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL58' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�58Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL88' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�88Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL128' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�128Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL158' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�158Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL188' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�188Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL288' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�288Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL388' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�388Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL588' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�588Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL888' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�888Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_BD58' a ,'ȫ��ͨȫ��ͳһ�ʷѱ����ײ�58Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_BD88' a ,'ȫ��ͨȫ��ͳһ�ʷѱ����ײ�88Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_BD128' a ,'ȫ��ͨȫ��ͳһ�ʷѱ����ײ�128Ԫ' b from  bass2.dual ) t where a.bass1_id = t.a
                20110429 TIME_ID
                ,char(a.product_instance_id)  USER_ID
                ,bass1.fn_get_all_dim('BASS_STD1_0114',char(a.offer_id)) BASE_PKG_ID
                ,replace(char(date(a.create_date)),'-','') VALID_DT
        from  bass2.ODS_PRODUCT_INS_PROD_20110429 a
                20110429 TIME_ID ,offer_id
                ,char(a.product_instance_id)  USER_ID
                ,bass1.fn_get_all_dim('BASS_STD1_0114',char(a.offer_id)) BASE_PKG_ID
                ,replace(char(date(a.create_date)),'-','') VALID_DT
        from  bass2.ODS_PRODUCT_INS_PROD_20110429 a
--,  count(distinct MODULE ) 
from app.sch_control_map 
group by  MODULE 
order by 1 
insert into app.sch_control_map
values
 (2,'G_I_02022_DAY.tcl','BASS1_G_I_02022_DAY.tcl')
,(2,'G_I_02023_DAY.tcl','BASS1_G_I_02023_DAY.tcl')
,(2,'G_S_22080_DAY.tcl','BASS1_G_S_22080_DAY.tcl')
,(2,'G_S_22081_MONTH.tcl','BASS1_G_S_22081_MONTH.tcl')
,(2,'G_S_22082_DAY.tcl','BASS1_G_S_22082_DAY.tcl')
,(2,'G_S_22083_MONTH.tcl','BASS1_G_S_22083_MONTH.tcl')
,(2,'G_S_22084_DAY.tcl','BASS1_G_S_22084_DAY.tcl')
,(2,'G_S_22085_MONTH.tcl','BASS1_G_S_22085_MONTH.tcl')

values
(2,'EXP_G_I_02022_DAY','BASS1_EXP_G_I_02022_DAY')
,(2,'EXP_G_I_02023_DAY','BASS1_EXP_G_I_02023_DAY')
,(2,'EXP_G_S_22080_DAY','BASS1_EXP_G_S_22080_DAY')
,(2,'EXP_G_S_22081_MONTH','BASS1_EXP_G_S_22081_MONTH')
,(2,'EXP_G_S_22082_DAY','BASS1_EXP_G_S_22082_DAY')
,(2,'EXP_G_S_22083_MONTH','BASS1_EXP_G_S_22083_MONTH')
,(2,'EXP_G_S_22084_DAY','BASS1_EXP_G_S_22084_DAY')
,(2,'EXP_G_S_22085_MONTH','BASS1_EXP_G_S_22085_MONTH')


select upper(control_code) from app.sch_control_task where upper(control_code) like '%ODS_PRODUCT_INS_PROD_%' 
	or upper(control_code) like '%ALL_DIM_LKP%' 
	or upper(control_code) like '%DWD_PRODUCT_TEST_PHONE_%' 
	or upper(control_code) like '%DW_PRODUCT_%' 
	or upper(control_code) like '%DW_PRODUCT_INS_OFF_INS_PROD_DS%' 
	or upper(control_code) like '%ALL_DIM_LKP%' 
	or upper(control_code) like '%DW_PRODUCT_%' 
	or upper(control_code) like '%DW_THREE_ITEM_STAT_DM_%' 
	or upper(control_code) like '%DW_PRODUCT_UNITE_CANCEL_ORDER_DM_%' 	
	or upper(control_code) like '%DW_PRODUCT_INS_OFF_INS_PROD_%' 
	or upper(control_code) like '%DW_PRODUCT_%' 
	or upper(control_code) like '%DIM_PROD_UP_PRODUCT_ITEM%' 
	or upper(control_code) like '%DW_PRODUCT_SP_INFO_%' 
	or upper(control_code) like '%DW_PRODUCT_UNITE_CANCEL_ORDER_%' 
	or upper(control_code) like '%DW_HIS_DSMP_SMS_SEND_MESSAGE_DM_%' 
	or upper(control_code) like '%DIM_PM_SP_OPERATOR_CODE%' 
	or upper(control_code) like '%DIM_PM_SERV_TYPE_VS_EXPR%' 
	or upper(control_code) like '%DW_HIS_DSMP_SMS_SEND_MESSAGE_DM_%' 
	or upper(control_code) like '%DIM_PM_SP_OPERATOR_CODE%' 
	or upper(control_code) like '%DIM_PM_SERV_TYPE_VS_EXPR%' 
	or upper(control_code) like '%DW_PRODUCT_SP_INFO_%' 
	or upper(control_code) like '%DW_PRODUCT_INS_OFF_INS_PROD_%' 
	or upper(control_code) like '%DW_PRODUCT_%' 
	or upper(control_code) like '%DIM_PROD_UP_PRODUCT_ITEM%' 
	or upper(control_code) like '%ODS_DIM_UP_SP_SERVICE_%' 


BASS2_Dw_acct_payment_dm.tcl

table 
(
select ' BASS1_G_I_02022_DAY.tcl ' a,' BASS2_Dw_product_ins_prod_ds.tcl ' b,' ods_product_ins_prod_ ' c from bass2.dual union all
select ' BASS1_G_I_02022_DAY.tcl ' a,' BASS2_Dwd_product_test_phone_ds.tcl ' b,' dwd_product_test_phone_ ' c from bass2.dual union all
select ' BASS1_G_I_02022_DAY.tcl ' a,' BASS2_Dw_product_ds.tcl ' b,' dw_product_ ' c from bass2.dual union all
select ' BASS1_G_I_02023_DAY.tcl ' a,' BASS2_Dw_product_ins_off_ins_prod_ds.tcl ' b,' dw_product_ins_off_ins_prod_ds  ' c from bass2.dual union all
select ' BASS1_G_I_02023_DAY.tcl ' a,' BASS2_Dw_product_ds.tcl ' b,' dw_product_ ' c from bass2.dual union all
select ' BASS1_G_S_22080_DAY.tcl ' a,' BASS2_Dw_three_item_stat_dm.tcl ' b,' dw_three_item_stat_dm_  ' c from bass2.dual union all
select ' BASS1_G_S_22080_DAY.tcl ' a,' BASS2_Dw_product_unite_cancel_order_dm.tcl ' b,' bass2 ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dw_product_ins_off_ins_prod_ms.tcl ' b,' dw_product_ins_off_ins_prod_ ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dw_product_ms.tcl ' b,' dw_product_  ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dim_prod_up_product_item_ds.tcl ' b,' dim_prod_up_product_item ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dw_product_sp_info_ms.tcl ' b,' dw_product_sp_info_ ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dw_product_unite_cancel_order_dm.tcl ' b,' dw_product_unite_cancel_order_ ' c from bass2.dual union all
select ' BASS1_G_S_22082_DAY.tcl ' a,' BASS2_Dw_his_dsmp_sms_send_message_dm.tcl ' b,' dw_his_dsmp_sms_send_message_dm_ ' c from bass2.dual union all
select ' BASS1_G_S_22082_DAY.tcl ' a,' BASS2_Dim_pm_sp_operator_code_ms.tcl ' b,' dim_pm_sp_operator_code  ' c from bass2.dual union all
select ' BASS1_G_S_22082_DAY.tcl ' a,' BASS2_Dim_pm_serv_type_vs_expr_ds.tcl ' b,' dim_pm_serv_type_vs_expr ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dw_his_dsmp_sms_send_message_dm.tcl ' b,' dw_his_dsmp_sms_send_message_dm_  ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dim_pm_sp_operator_code_ms.tcl ' b,' dim_pm_sp_operator_code ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dim_pm_serv_type_vs_expr_ds.tcl ' b,' dim_pm_serv_type_vs_expr  ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dw_product_sp_info_ms.tcl ' b,' dw_product_sp_info_  ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dw_product_ins_off_ins_prod_ms.tcl ' b,' dw_product_ins_off_ins_prod_ ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dw_product_ms.tcl ' b,' dw_product_  ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dim_prod_up_product_item_ds.tcl ' b,' dim_prod_up_product_item ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' TR1_L_18009 ' b,' ods_dim_up_sp_service_  ' c from bass2.dual ) t
) a 
table 
(
select ' BASS1_G_I_02022_DAY.tcl ' a,' BASS2_Dw_product_ins_prod_ds.tcl ' b,' ods_product_ins_prod_ ' c from bass2.dual union all
select ' BASS1_G_I_02022_DAY.tcl ' a,' BASS2_Dwd_product_test_phone_ds.tcl ' b,' dwd_product_test_phone_ ' c from bass2.dual union all
select ' BASS1_G_I_02022_DAY.tcl ' a,' BASS2_Dw_product_ds.tcl ' b,' dw_product_ ' c from bass2.dual union all
select ' BASS1_G_I_02023_DAY.tcl ' a,' BASS2_Dw_product_ins_off_ins_prod_ds.tcl ' b,' dw_product_ins_off_ins_prod_ds  ' c from bass2.dual union all
select ' BASS1_G_I_02023_DAY.tcl ' a,' BASS2_Dw_product_ds.tcl ' b,' dw_product_ ' c from bass2.dual union all
select ' BASS1_G_S_22080_DAY.tcl ' a,' BASS2_Dw_three_item_stat_dm.tcl ' b,' dw_three_item_stat_dm_  ' c from bass2.dual union all
select ' BASS1_G_S_22080_DAY.tcl ' a,' BASS2_Dw_product_unite_cancel_order_dm.tcl ' b,' bass2 ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dw_product_ins_off_ins_prod_ms.tcl ' b,' dw_product_ins_off_ins_prod_ ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dw_product_ms.tcl ' b,' dw_product_  ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dim_prod_up_product_item_ds.tcl ' b,' dim_prod_up_product_item ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dw_product_sp_info_ms.tcl ' b,' dw_product_sp_info_ ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dw_product_unite_cancel_order_dm.tcl ' b,' dw_product_unite_cancel_order_ ' c from bass2.dual union all
select ' BASS1_G_S_22082_DAY.tcl ' a,' BASS2_Dw_his_dsmp_sms_send_message_dm.tcl ' b,' dw_his_dsmp_sms_send_message_dm_ ' c from bass2.dual union all
select ' BASS1_G_S_22082_DAY.tcl ' a,' BASS2_Dim_pm_sp_operator_code_ms.tcl ' b,' dim_pm_sp_operator_code  ' c from bass2.dual union all
select ' BASS1_G_S_22082_DAY.tcl ' a,' BASS2_Dim_pm_serv_type_vs_expr_ds.tcl ' b,' dim_pm_serv_type_vs_expr ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dw_his_dsmp_sms_send_message_dm.tcl ' b,' dw_his_dsmp_sms_send_message_dm_  ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dim_pm_sp_operator_code_ms.tcl ' b,' dim_pm_sp_operator_code ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dim_pm_serv_type_vs_expr_ds.tcl ' b,' dim_pm_serv_type_vs_expr  ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dw_product_sp_info_ms.tcl ' b,' dw_product_sp_info_  ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dw_product_ins_off_ins_prod_ms.tcl ' b,' dw_product_ins_off_ins_prod_ ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dw_product_ms.tcl ' b,' dw_product_  ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dim_prod_up_product_item_ds.tcl ' b,' dim_prod_up_product_item ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' TR1_L_18009 ' b,' ods_dim_up_sp_service_  ' c from bass2.dual ) t
table 
(
select ' BASS1_G_I_02022_DAY.tcl ' a,' BASS2_Dw_product_ins_prod_ds.tcl ' b,' ods_product_ins_prod_ ' c from bass2.dual union all
select ' BASS1_G_I_02022_DAY.tcl ' a,' BASS2_Dwd_product_test_phone_ds.tcl ' b,' dwd_product_test_phone_ ' c from bass2.dual union all
select ' BASS1_G_I_02022_DAY.tcl ' a,' BASS2_Dw_product_ds.tcl ' b,' dw_product_ ' c from bass2.dual union all
select ' BASS1_G_I_02023_DAY.tcl ' a,' BASS2_Dw_product_ins_off_ins_prod_ds.tcl ' b,' dw_product_ins_off_ins_prod_ds  ' c from bass2.dual union all
select ' BASS1_G_I_02023_DAY.tcl ' a,' BASS2_Dw_product_ds.tcl ' b,' dw_product_ ' c from bass2.dual union all
select ' BASS1_G_S_22080_DAY.tcl ' a,' BASS2_Dw_three_item_stat_dm.tcl ' b,' dw_three_item_stat_dm_  ' c from bass2.dual union all
select ' BASS1_G_S_22080_DAY.tcl ' a,' BASS2_Dw_product_unite_cancel_order_dm.tcl ' b,' bass2 ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dw_product_ins_off_ins_prod_ms.tcl ' b,' dw_product_ins_off_ins_prod_ ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dw_product_ms.tcl ' b,' dw_product_  ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dim_prod_up_product_item_ds.tcl ' b,' dim_prod_up_product_item ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dw_product_sp_info_ms.tcl ' b,' dw_product_sp_info_ ' c from bass2.dual union all
select ' BASS1_G_S_22081_MONTH.tcl ' a,' BASS2_Dw_product_unite_cancel_order_dm.tcl ' b,' dw_product_unite_cancel_order_ ' c from bass2.dual union all
select ' BASS1_G_S_22082_DAY.tcl ' a,' BASS2_Dw_his_dsmp_sms_send_message_dm.tcl ' b,' dw_his_dsmp_sms_send_message_dm_ ' c from bass2.dual union all
select ' BASS1_G_S_22082_DAY.tcl ' a,' BASS2_Dim_pm_sp_operator_code_ms.tcl ' b,' dim_pm_sp_operator_code  ' c from bass2.dual union all
select ' BASS1_G_S_22082_DAY.tcl ' a,' BASS2_Dim_pm_serv_type_vs_expr_ds.tcl ' b,' dim_pm_serv_type_vs_expr ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dw_his_dsmp_sms_send_message_dm.tcl ' b,' dw_his_dsmp_sms_send_message_dm_  ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dim_pm_sp_operator_code_ms.tcl ' b,' dim_pm_sp_operator_code ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dim_pm_serv_type_vs_expr_ds.tcl ' b,' dim_pm_serv_type_vs_expr  ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dw_product_sp_info_ms.tcl ' b,' dw_product_sp_info_  ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dw_product_ins_off_ins_prod_ms.tcl ' b,' dw_product_ins_off_ins_prod_ ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dw_product_ms.tcl ' b,' dw_product_  ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' BASS2_Dim_prod_up_product_item_ds.tcl ' b,' dim_prod_up_product_item ' c from bass2.dual union all
select ' BASS1_G_S_22083_MONTH.tcl ' a,' TR1_L_18009 ' b,' ods_dim_up_sp_service_  ' c from bass2.dual ) t
select * from 
app.sch_control_alarm 
where alarmtime >=  current timestamp - 1 days
and flag = -1 

select * from app.sch_control_alarm 
update app.sch_control_alarm 
set flag = 1 
where flag = -1 
and  alarmtime >=  current timestamp - 1 days

insert into app.sch_control_before values('BASS1_EXP_G_S_22085_MONTH','BASS1_G_S_22085_MONTH.tcl');


insert into app.sch_control_before values('BASS1_G_S_22085_MONTH.tcl','BASS2_Dw_acct_payment_dm.tcl');


or control_code like 'BASS1%22082%'
or control_code like 'BASS1%22084%'
or control_code like 'BASS1%22085%'
or control_code like 'BASS1%22081%'
or control_code like 'BASS1%02023%'
or control_code like 'BASS1%22083%'
or control_code like 'BASS1%02022%'
select * from app.sch_control_before 
where control_code in (
select control_code from app.sch_control_task where
(
   control_code like 'BASS1%22080%'
or control_code like 'BASS1%22082%'
or control_code like 'BASS1%22084%'
or control_code like 'BASS1%22085%'
or control_code like 'BASS1%22081%'
or control_code like 'BASS1%02023%'
or control_code like 'BASS1%22083%'
or control_code like 'BASS1%02022%'
)
and function_desc not like '%����%'
)
select distinct * from app.sch_control_before 
where control_code in (
select control_code from app.sch_control_task where
(
   control_code like 'BASS1%22080%'
or control_code like 'BASS1%22082%'
or control_code like 'BASS1%22084%'
or control_code like 'BASS1%22085%'
or control_code like 'BASS1%22081%'
or control_code like 'BASS1%02023%'
or control_code like 'BASS1%22083%'
or control_code like 'BASS1%02022%'
)
and function_desc not like '%����%'
)
select  control_code,before_control_code , count(0) from app.sch_control_before 
where control_code in (
select control_code from app.sch_control_task where
(
   control_code like 'BASS1%22080%'
or control_code like 'BASS1%22082%'
or control_code like 'BASS1%22084%'
or control_code like 'BASS1%22085%'
or control_code like 'BASS1%22081%'
or control_code like 'BASS1%02023%'
or control_code like 'BASS1%22083%'
or control_code like 'BASS1%02022%'
)
and function_desc not like '%����%'
)
select  control_code||before_control_code 
where control_code in (
select control_code from app.sch_control_task where
(
   control_code like 'BASS1%22080%'
or control_code like 'BASS1%22082%'
or control_code like 'BASS1%22084%'
or control_code like 'BASS1%22085%'
or control_code like 'BASS1%22081%'
or control_code like 'BASS1%02023%'
or control_code like 'BASS1%22083%'
or control_code like 'BASS1%02022%'
)
and function_desc not like '%����%'
)

select * from  app.sch_control_before 
where control_code||before_control_code  in (
select  control_code||before_control_code 
where control_code in (
select control_code from app.sch_control_task where
(
   control_code like 'BASS1%22080%'
or control_code like 'BASS1%22082%'
or control_code like 'BASS1%22084%'
or control_code like 'BASS1%22085%'
or control_code like 'BASS1%22081%'
or control_code like 'BASS1%02023%'
or control_code like 'BASS1%22083%'
or control_code like 'BASS1%02022%'
)
and function_desc not like '%����%'
)
group by control_code,before_control_code having count(0) > 1
)


select * from  app.sch_control_before 
where control_code||before_control_code  in (
select  control_code||before_control_code from app.sch_control_before 
where control_code in (
select control_code from app.sch_control_task where
(
   control_code like 'BASS1%22080%'
or control_code like 'BASS1%22082%'
or control_code like 'BASS1%22084%'
or control_code like 'BASS1%22085%'
or control_code like 'BASS1%22081%'
or control_code like 'BASS1%02023%'
or control_code like 'BASS1%22083%'
or control_code like 'BASS1%02022%'
)
and function_desc not like '%����%'
)
group by control_code,before_control_code having count(0) > 1
)

delete from  app.sch_control_before 
where control_code||before_control_code  in (
select  control_code||before_control_code from app.sch_control_before 
where control_code in (
select control_code from app.sch_control_task where
(
   control_code like 'BASS1%22080%'
or control_code like 'BASS1%22082%'
or control_code like 'BASS1%22084%'
or control_code like 'BASS1%22085%'
or control_code like 'BASS1%22081%'
or control_code like 'BASS1%02023%'
or control_code like 'BASS1%22083%'
or control_code like 'BASS1%02022%'
)
and function_desc not like '%����%'
)
group by control_code,before_control_code having count(0) > 1
)

values ('BASS1_EXP_G_I_02022_DAY','BASS1_G_I_02022_DAY.tcl')
,('BASS1_EXP_G_I_02023_DAY','BASS1_G_I_02023_DAY.tcl')
,('BASS1_EXP_G_S_22080_DAY','BASS1_G_S_22080_DAY.tcl')
,('BASS1_EXP_G_S_22081_MONTH','BASS1_G_S_22081_MONTH.tcl')
,('BASS1_EXP_G_S_22082_DAY','BASS1_G_S_22082_DAY.tcl')
,('BASS1_EXP_G_S_22083_MONTH','BASS1_G_S_22083_MONTH.tcl')


(
 '22080'
,'22082'
,'22084'
,'22085'
,'22081'
,'02023'
,'22083'
,'02022'
)



DIM_PM_SP_OPERATOR_CODE
ODS_PM_SP_OPERATOR_CODE_201103
ODS_PM_SP_OPERATOR_CODE_20110426
ODS_PM_SP_OPERATOR_CODE_20110427
ODS_PM_SP_OPERATOR_CODE_20110428
ODS_PM_SP_OPERATOR_CODE_20110429
ODS_PM_SP_OPERATOR_CODE_20110430
ODS_PM_SP_OPERATOR_CODE_YYYYMMDD

                  OPERATOR_CODE VARCHAR(24) NOT NULL , 
                  SP_CODE BIGINT NOT NULL , 
                  BILL_FLAG SMALLINT NOT NULL , 
                  SP_TYPE INTEGER NOT NULL,
                 DISTRIBUTE BY HASH(OPERATOR_CODE)   
                   IN TBS_ODS_OTHER INDEX IN TBS_INDEX ; 

ALTER TABLE BASS2   .ODS_PM_SP_OPERATOR_CODE_YYYYMM LOCKSIZE TABLE;



from  BASS1.ALL_DIM_LKP 
where BASS1_TBID = 'BASS_STD1_0114'
      and bass1_value like 'QW_QQT_JC%'

                  OPERATOR_CODE VARCHAR(24) NOT NULL , 
                  SP_CODE BIGINT NOT NULL , 
                  BILL_FLAG SMALLINT NOT NULL , 
                  SP_TYPE INTEGER NOT NULL,
                  OPERATOR_NAME varchar(256)
                  )   
                 DISTRIBUTE BY HASH(OPERATOR_CODE)   
                   IN TBS_ODS_OTHER INDEX IN TBS_INDEX ; 

ALTER TABLE BASS2   .ODS_PM_SP_OPERATOR_CODE_201104 LOCKSIZE TABLE;


DIM_PM_SP_OPERATOR_CODE
ODS_PM_SP_OPERATOR_CODE_201103
drop table ODS_PM_SP_OPERATOR_CODE_20110426
drop table ODS_PM_SP_OPERATOR_CODE_20110427
drop table ODS_PM_SP_OPERATOR_CODE_20110428
drop table ODS_PM_SP_OPERATOR_CODE_20110429
drop table ODS_PM_SP_OPERATOR_CODE_20110430
drop table ODS_PM_SP_OPERATOR_CODE_YYYYMMDD

drop table bass2.ODS_PM_SP_OPERATOR_CODE_20110427
drop table bass2.ODS_PM_SP_OPERATOR_CODE_20110428
drop table bass2.ODS_PM_SP_OPERATOR_CODE_20110429
drop table bass2.ODS_PM_SP_OPERATOR_CODE_20110430
drop table bass2.ODS_PM_SP_OPERATOR_CODE_YYYYMMDD

 ('d','02022','�û�ѡ��ȫ��ͨȫ��ͳһ�����ʷ��ײ�','ÿ��ȫ��','ÿ��13��ǰ')
,('d','02023','�û�ѡ��ȫ��ͨר�������ʷ��ײ�','ÿ��ȫ��','ÿ��13��ǰ')
,('d','22080','ͳһ��ѯ�˶��ջ���','ÿ������','ÿ��13��ǰ')
,('d','22082','ҵ��۷����������ջ���','ÿ������','ÿ��13��ǰ')
,('d','22084','�շ��������˷Ѻ��֤�ջ���','ÿ������','ÿ��13��ǰ')
('m','22081','ͳһ��ѯ���˶��»���','ÿ������','ÿ��8��ǰ')
,('m','22083','ҵ��۷����������»���','ÿ������','ÿ��8��ǰ')
,('m','22085','�շ��������˷Ѻ��֤�»���','ÿ������','ÿ��8��ǰ')

select UPLOAD_TIME , count(0) 
--,  count(distinct UPLOAD_TIME ) 
from bass1.mon_all_interface 
group by  UPLOAD_TIME 
order by 1 

BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = 'ÿ��3��ǰ'

select time_id , count(0) 
--,  count(distinct time_id ) 
from bass1.g_i_02018_month 
group by  time_id 
order by 1 

                    a.product_item_id                         base_prod_id,
                    a.name                                    base_prod_name,
                    case when a.del_flag='1' then '1'
                    else '2' end                              prod_status,
                    replace(char(date(a.create_date)),'-','') prod_begin_time,
                    replace(char(date(a.exp_date)),'-','')    prod_end_time,
                    a.platform_id,
                    b.trademark
                from bass2.dim_prod_up_product_item a,
                  bass2.Dim_prod_up_offer b
                where a.item_type = 'OFFER_PLAN' 
                and b.offer_type IN ('OFFER_PLAN')
                and a.product_item_id = b.offer_id
                and a.platform_id in (1,2)
                and replace(char(date(a.create_date)),'-','')<='20110430'
                and replace(char(date(b.create_date)),'-','')<='20110430'
(89110016, 89210016, 89310016, 89410016, 89510016, 89610016, 89710016, 89110017, 89210017, 89310017, 89410017, 89510017, 89610017, 89710017, 89110018, 89210018, 89310018, 89410018, 89510018, 89610018, 89710018)
(89110016, 89210016, 89310016, 89410016, 89510016, 89610016, 89710016, 89110017, 89210017, 89310017, 89410017, 89510017, 89610017, 89710017, 89110018, 89210018, 89310018, 89410018, 89510018, 89610018, 89710018)
  SELECT 'bass2.DIM_PROD_UP_PRODUCT_ITEM' bas_tb,a.name ,char(a.PRODUCT_ITEM_ID)
  ,'ȫ��ͨȫ��ͳһ�ʷѻ����ײͱ�ʶ' bass1_name
  ,'BASS_STD1_0114'
  ,
case 
when name like '%����%128%' then 'QW_QQT_JC_SW128'
when name like '%����%88%' then 'QW_QQT_JC_SW88'
when name like '%����%58%' then 'QW_QQT_JC_SW58'
when name like '%����%128%' then 'QW_QQT_JC_BD128'
when name like '%����%88%' then 'QW_QQT_JC_BD88'
when name like '%����%58%' then 'QW_QQT_JC_BD58'
--
when name like '%����%888%' then 'QW_QQT_JC_SL888'
when name like '%����%588%' then 'QW_QQT_JC_SL588'
when name like '%����%388%' then 'QW_QQT_JC_SL388'
when name like '%����%288%' then 'QW_QQT_JC_SL288'
when name like '%����%188%' then 'QW_QQT_JC_SL188'
when name like '%����%158%' then 'QW_QQT_JC_SL158'
when name like '%����%128%' then 'QW_QQT_JC_SL128'
when name like '%����%88%' then 'QW_QQT_JC_SL88'
when name like '%����%58%' then 'QW_QQT_JC_SL58'
end 
  bass1_id
  FROM bass2.DIM_PROD_UP_PRODUCT_ITEM a
WHERE ITEM_TYPE='OFFER_PLAN'
and extend_id in
(89110016, 89210016, 89310016, 89410016, 89510016, 89610016, 89710016, 89110017, 89210017, 89310017, 89410017, 89510017, 89610017, 89710017, 89110018, 89210018, 89310018, 89410018, 89510018, 89610018, 89710018)
) a,
      table (
select 'QW_QQT_JC_SW58' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�58Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SW88' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�88Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SW128' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�128Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL58' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�58Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL88' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�88Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL128' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�128Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL158' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�158Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL188' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�188Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL288' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�288Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL388' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�388Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL588' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�588Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_SL888' a ,'ȫ��ͨȫ��ͳһ�ʷ������ײ�888Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_BD58' a ,'ȫ��ͨȫ��ͳһ�ʷѱ����ײ�58Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_BD88' a ,'ȫ��ͨȫ��ͳһ�ʷѱ����ײ�88Ԫ' b from  bass2.dual union all
select 'QW_QQT_JC_BD128' a ,'ȫ��ͨȫ��ͳһ�ʷѱ����ײ�128Ԫ' b from  bass2.dual ) t where a.bass1_id = t.a
     
     

         'bass2.DIM_PROD_UP_PRODUCT_ITEM' XZBAS_TBNAME
        ,a.name XZBAS_COLNAME
        ,char(a.PRODUCT_ITEM_ID) XZBAS_VALUE
        ,'ȫ��ͨȫ��ͳһ�ʷѻ����ײͱ�ʶ' BASS1_TBN_DESC
        ,'BASS_STD1_0114' BASS1_TBID
from bass2.DIM_PROD_UP_PRODUCT_ITEM      a    
WHERE ITEM_TYPE='OFFER_PLAN'
and extend_id in
(89110016, 89210016, 89310016, 89410016, 89510016, 89610016, 89710016, 89110017, 89210017, 89310017, 89410017, 89510017, 89610017, 89710017, 89110018, 89210018, 89310018, 89410018, 89510018, 89610018, 89710018)

select 
         'bass2.DIM_PROD_UP_PRODUCT_ITEM' XZBAS_TBNAME
        ,a.name XZBAS_COLNAME
        ,char(a.PRODUCT_ITEM_ID) XZBAS_VALUE
        ,'ȫ��ͨȫ��ͳһ�ʷѻ����ײͱ�ʶ' BASS1_TBN_DESC
        ,'BASS_STD1_0114' BASS1_TBID
from bass2.DIM_PROD_UP_PRODUCT_ITEM      a    
WHERE ITEM_TYPE='OFFER_PLAN'
and extend_id in
(89110016, 89210016, 89310016, 89410016, 89510016, 89610016, 89710016, 89110017, 89210017, 89310017, 89410017, 89510017, 89610017, 89710017, 89110018, 89210018, 89310018, 89410018, 89510018, 89610018, 89710018)

					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0114'
					      and bass1_value like 'QW_QQT_JC%'
where BASS1_TBID = 'BASS_STD1_0114'
                    a.product_item_id                         base_prod_id,
                    a.name                                    base_prod_name,
                    case when a.del_flag='1' then '1'
                    else '2' end                              prod_status,
                    replace(char(date(a.create_date)),'-','') prod_begin_time,
                    replace(char(date(a.exp_date)),'-','')    prod_end_time,
                    a.platform_id,
                    b.trademark
                from bass2.dim_prod_up_product_item a,
                  bass2.Dim_prod_up_offer b
                where a.item_type = 'OFFER_PLAN' 
                --and b.offer_type IN ('OFFER_PLAN')
                and a.product_item_id = b.offer_id
                and a.platform_id in (1,2)
                and replace(char(date(a.create_date)),'-','')<='20110430'
                and replace(char(date(b.create_date)),'-','')<='20110430'
--,  count(distinct time_id ) 
from G_I_02019_MONTH
group by  time_id 
order by 1 


select * from   app.sch_control_before where control_code = 'BASS2_Dw_product_sp_info_ms.tcl'

select * from   app.sch_control_runlog where control_code = 'TR1_L_11039'

select * from   app.sch_control_task where control_code = 'TR1_L_11039'

select * from   bass2.ODS_PRODUCT_SP_INFO_201105


select * from   app.sch_control_runlog 
where control_code like  'BASS1%2208%MONTH%'




CREATE TABLE "BASS2   "."ODS_PRODUCT_SP_INFO_201104"  (
                  "SP_INFO_ID" VARCHAR(12) NOT NULL , 
                  "SERV_TYPE" VARCHAR(50) , 
                  "SP_CODE" VARCHAR(20) , 
                  "SP_NAME" VARCHAR(100) , 
                  "SP_TYPE" INTEGER , 
                  "SERV_CODE" VARCHAR(50) , 
                  "PROV_CODE" VARCHAR(20) , 
                  "BAL_PROV" VARCHAR(20) , 
                  "DEV_CODE" VARCHAR(20) , 
                  "PLATFORM_ID" INTEGER , 
                  "IS_USER_CONFIRM" SMALLINT , 
                  "IS_GLOBAL" SMALLINT , 
                  "IS_THIRD_CONFIRM" SMALLINT , 
                  "DESCRIPTION" VARCHAR(200) , 
                  "VALID_DATE" TIMESTAMP , 
                  "EXPIRE_DATE" TIMESTAMP , 
                  "DEL_FLAG" VARCHAR(1) , 
                  "SP_SHORT_NAME" VARCHAR(100) , 
                  "STATE" VARCHAR(1) , 
                  "SP_SVC_ID" VARCHAR(32) , 
                  "CSR_TEL" VARCHAR(128) , 
                  "CSR_URL" VARCHAR(128) , 
                  "SP_EN_NAME" VARCHAR(128) , 
                  "SERV_ID_ALIAS" VARCHAR(128) , 
                  "DONE_DATE" TIMESTAMP , 
                  "OPER_MODE" VARCHAR(2) )   
                 DISTRIBUTE BY HASH("SP_INFO_ID",  
                 "SP_CODE")   
                   IN "TBS_ODS_OTHER" INDEX IN "TBS_INDEX" ; 

ALTER TABLE "BASS2   "."ODS_PRODUCT_SP_INFO_201104" LOCKSIZE TABLE;




select * from   bass1.g_s_22082_day


--,  count(distinct time_id ) 
from BASS1.G_S_04003_DAY 
group by  time_id 
order by 1 desc
BASS2_Dim_pm_serv_type_vs_expr_ds.tcl
BASS2_Dim_pm_sp_operator_code_ms.tcl
BASS2_Dw_his_dsmp_sms_send_message_dm.tcl

BASS2_Dw_product_unite_cancel_order_dm.tcl
BASS2_Dw_three_item_stat_dm.tcl

0

BASS2_Dw_three_item_stat_dm.tcl

'BASS2_dw_ng1_chl_uniformterm_mm.tcl',
'BASS2_Dw_acct_sale_discount_ms.tcl',
'BASS2_Dw_channel_msg_ms.tcl',
'BASS2_Dw_channel_named_busi_ms.tcl',
'BASS2_Dw_chl_e_10086_user_dm.tcl',
'BASS2_Dw_chl_e_all_user_mt.tcl',
'BASS2_Dw_chl_e_sms_user_dm.tcl',
'BASS2_Dw_chl_e_touch_user_dm.tcl',
'BASS2_Dw_chl_e_ussd_user_dm.tcl',
'BASS2_Dw_chl_e_wap_user_dm.tcl',
'BASS2_Dw_chl_e_web_user_dm.tcl',
'BASS2_Dw_ent_adc_ms.tcl',
'BASS2_Dw_ent_imp_vocation_ms.tcl',
'BASS2_Dw_ent_mas_ms.tcl',
'BASS2_Dw_ent_msg_ms.tcl',
'BASS2_Dw_ent_snapshot_mem_ms.tcl',
'BASS2_Dw_ent_snapshot_ms.tcl',
'BASS2_Dw_ent_subpro_mr_ms.tcl',
'BASS2_Dw_ent_target_ms.tcl',
'BASS2_Dw_ent_term_ms.tcl',
'BASS2_Dw_ent_use_prod_ms.tcl',
'BASS2_Dw_enterprise_industry_apply.tcl',
'BASS2_Dw_enterprise_keycust_msg_mm.tcl',
'BASS2_Dw_enterprise_member_mid_ms.tcl',
'BASS2_Dw_enterprise_msg_ext_ms.tcl',
'BASS2_Dw_enterprise_new_unipay_ms.tcl',
'BASS2_Dw_enterprise_unipay_ms.tcl',
'BASS2_Dw_have_value_ms.tcl',
'BASS2_Dw_have_value_snapshot_ms.tcl',
'BASS2_Dw_ng1_chl_e_useinfo_ms.tcl',
'BASS2_Dw_ng1_chl_like_chl_ms.tcl',
'BASS2_Dw_ng1_chl_phy_useinfo_ms.tcl',
'BASS2_Dw_ng1_chl_selfterm_userinfo_ms.tcl',
'BASS2_Dw_ng1_chl_smssend_useinfo_ms.tcl',
'BASS2_Dw_ng1_chl_term_custinfo_ms.tcl',
'BASS2_Dw_ng1_chl_term_msg_ms.tcl',
'BASS2_Dw_ng1_univ_term_custcharac_ms.tcl',
'BASS2_Dw_product_activity_ms.tcl',
'BASS2_Dw_product_cust_unite_ms.tcl',
'BASS2_Dw_product_ext_ms_1.tcl',
'BASS2_Dw_product_ext_ms.tcl',
'BASS2_Dw_product_snapshot_ms.tcl',
'BASS2_Dw_product_td_addon_ms.tcl',
'BASS2_Dw_product_td_gene_ms.tcl',
'BASS2_Dw_product_td_gprs_ms.tcl',
'BASS2_Dw_product_td_income_ms.tcl',
'BASS2_Dw_product_td_ms.tcl',
'BASS2_Dw_td_check_user_ms.tcl',
'BASS2_group_collection.tcl',
'BASS2_group_dmrn_potential_basic.tcl',
'BASS2_group_inc.tcl',
'BASS2_group_info.tcl',
'BASS2_group_level_change.tcl',
'BASS2_group_mbmreco.tcl',
'BASS2_group_memberloss.tcl',
'BASS2_group_nums.tcl',
'BASS2_group_perspect_base_list.tcl',
'BASS2_group_perspect_base.tcl',
'BASS2_group_perspect_basic.tcl',
'BASS2_group_perspect_fw.tcl',
'BASS2_group_perspect_group_base.tcl',
'BASS2_group_perspect_group_member_base.tcl',
'BASS2_group_perspect_load.tcl',
'BASS2_group_perspect_roam.tcl',
'BASS2_group_perspect_sms_detail.tcl',
'BASS2_group_perspect_sms.tcl',
'BASS2_group_perspect.tcl',
'BASS2_group_score.tcl',
'BASS2_mart_comp_users_mm.tcl',
'BASS2_mart_ent_manager_accosche_mm.tcl',
'BASS2_mart_ent_manager_accosche_order_mm.tcl',
'BASS2_mart_ent_manager_index_mm.tcl',
'BASS2_mart_ent_sinent_multi_anal_mm.tcl',
'BASS2_mart_entmember_call_yyyymm.tcl',
'BASS2_mart_kpi_monthly.tcl',
'BASS2_mart_product_yyyymm.tcl',
'BASS2_mart_vip_callandfee_yyyymm.tcl',
'BASS2_mart_vip_manager_accosche_mm.tcl',
'BASS2_mart_vip_manager_accosche_order_mm.tcl',
'BASS2_mart_vip_manager_index_mm.tcl',
'BASS2_region_custchange_monthly.tcl',
'BASS2_region_enterprisekpi_monthly.tcl',
'BASS2_region_examkpi_monthly_2011.tcl',
'BASS2_region_examkpi_monthly_qz_2011.tcl',
'BASS2_region_feedis_monthly.tcl',
'BASS2_region_gridsnapkpi_monthly.tcl',
'BASS2_region_si_kpi_monthly.tcl',
'BASS2_region_snapshotkpi2_monthly_fetion.tcl',
'BASS2_region_snapshotkpi2_monthly.tcl',
'BASS2_region_snapshotkpi3_monthly_fetion.tcl',
'BASS2_region_snapshotkpi3_monthly.tcl',
'BASS2_regioncell_channelplan_monthly.tcl',
'BASS2_regioncell_character_monthly.tcl',
'BASS2_regioncell_custservice_monthly.tcl',
'BASS2_regioncell_hotcell_monthly.tcl',
'BASS2_regioncell_rualdevelop_monthly.tcl',
'BASS2_regioncell_snapshotkpi_monthly.tcl',
'BASS2_regionchannel_kpi_monthly.tcl',
'BASS2_regionpromo_monthly.tcl',
'BASS2_Region_download_mm.tcl',
'BASS2_Region_user_gridsnap_ms.tcl',
'BASS2_st_product_td_call_mm.tcl',
'BASS2_st_product_td_gene_mm.tcl',
'BASS2_st_product_td_gprs_mm.tcl',
'BASS2_st_product_td_owe_mm.tcl',
'BASS2_stat_mart_ent_manager_index_mm.tcl',
'BASS2_td_cust_base_info_ms.tcl',
'BASS2_td_cust_data_info_ms.tcl',
'BASS2_td_cust_info_view_ms.tcl',
'BASS2_td_cust_net_info_ms.tcl',
'BASS2_td_cust_voice_info_ms.tcl',
'BASS2_td_cust_voice_info_other_ms.tcl',
'BASS2_td_doublecard_0001_m.tcl',
'BASS2_td_doublecard_0002_m.tcl',
'BASS2_td_doublecard_0003_m.tcl',
'DMK_mart_bad_analysis_mm.tcl',
'DMK_mart_bad_woff_analysis_mm.tcl',
'DMK_mart_ent_vpmn_latent.tcl',
'DMK_mart_online_product_anal.tcl',
'DMK_mart_online_product_yyyymm.tcl',
'DMK_mart_product_yyyymm_add.tcl',
'DMK_mart_region_newsreport_mm.tcl',
'DMK_mart_regioncomp_call_mm.tcl',
'DMK_martd_snapshot_user_1_mm.tcl',
'DMK_martd_snapshot_user_mm_3.tcl',
'DMK_martd_snapshot_user_mm.tcl',
'DMK_stat_enterprise_actbad_detail_mm.tcl',
'DMK_stat_month_call_mm.tcl',
'DMK_stat_owe_callback_detail_yyyymm.tcl',
'DMK_stat_owe_callback_mm.tcl',
'DMK_stat_region_actbad_detail_mm.tcl',
'DMK_stat_region_entnewbusiuser_mm.tcl',
'DMK_stat_region_newuser_mm.tcl',
'MPM_mtl_dmsale_consume_lmonth_yyyymm.tcl',
'MPM_mtl_dmsale_output.tcl',
'MPM_mtl_gindex.tcl',
'MPM_mtl_PartUser.tcl',
'MPM_mtl_UserBase.tcl',
'MPM_mtl_UserExt.tcl',
'NG1_Dw_ng1_co_careuser_mm.tcl',
'NG1_Dw_ng1_co_unsatisfy_mm.tcl',
'NG1_Dw_ng1_custsvc_co_aim_dm.tcl',
'NG1_Dw_ng1_product_ms.tcl',
'NG1CUSTUSG_call_analy_mm.tcl',
'NG1CUSTUSG_call_mou_analy_mm.tcl',
'NG1CUSTUSG_consumption_analy_mm.tcl',
'NG1CUSTUSG_cring_analysis_mm.tcl',
'NG1CUSTUSG_cust_analy_mm.tcl',
'NG1CUSTUSG_databusi_analy_mm.tcl',
'NG1CUSTUSG_entcust_analy_mm.tcl',
'NG1CUSTUSG_interbusi_infoanaly_mm.tcl',
'NG1CUSTUSG_interroam_in_dataanaly_mm.tcl',
'NG1CUSTUSG_interroam_out_dataanaly_mm.tcl',
'NG1CUSTUSG_interroam_out_voiceanaly_mm.tcl',
'NG1CUSTUSG_intertga_roam_out_voiceanaly_mm.tcl',
'NG1CUSTUSG_mms_analy_mm.tcl',
'NG1CUSTUSG_mobile_newspaper_analysis_mm.tcl',
'NG1CUSTUSG_sms_analy_mm.tcl',
'NG1CUSTUSG_sms_mms_out_busianaly_mm.tcl',
'NG1CUSTUSG_vipcust_analy_mm.tcl',
'NG1CUSTUSG_wirelessmusic_analy_mm.tcl',
'NG1ENT_dw_ng1_ent_acct_ms.tcl',
'NG1ENT_dw_ng1_ent_acctmem_ms.tcl',
'NG1ENT_dw_ng1_ent_busistat_ms.tcl',
'NG1ENT_dw_ng1_ent_cm_hidemember_ms.tcl',
'NG1ENT_dw_ng1_ent_comp_hidemember_ms.tcl',
'NG1ENT_dw_ng1_ent_costbenefit_ms.tcl',
'NG1ENT_dw_ng1_ent_infofee_ms.tcl',
'NG1ENT_dw_ng1_ent_linker_ms.tcl',
'NG1ENT_dw_ng1_ent_mem_mobmail_ms.tcl',
'NG1ENT_dw_ng1_ent_mem_univ_ms.tcl',
'NG1ENT_dw_ng1_ent_mem_vpmn_ms.tcl',
'NG1ENT_dw_ng1_ent_memshould_ms.tcl',
'NG1ENT_dw_ng1_ent_mobmail_ms.tcl',
'NG1ENT_dw_ng1_ent_ms.tcl',
'NG1ENT_dw_ng1_ent_off_base_ms.tcl',
'NG1ENT_dw_ng1_ent_owefee_ms.tcl',
'NG1ENT_dw_ng1_ent_platform_product_ms.tcl',
'NG1ENT_dw_ng1_ent_platform_use_ms.tcl',
'NG1ENT_dw_ng1_ent_promemshd_ms.tcl',
'NG1ENT_dw_ng1_ent_scorereturn_ms.tcl',
'NG1ENT_dw_ng1_ent_should_ms.tcl',
'NG1ENT_dw_ng1_ent_subpro_cr_ms.tcl',
'NG1ENT_dw_ng1_ent_subpro_det_cr_ms.tcl',
'NG1ENT_dw_ng1_ent_subpro_swbd_calldtl_ms.tcl',
'NG1ENT_dw_ng1_ent_subpro_swbd_ms.tcl',
'NG1ENT_dw_ng1_ent_subpro_swbdcall_ms.tcl',
'NG1ENT_dw_ng1_ent_subpro_swbdin_ms.tcl',
'NG1ENT_dw_ng1_ent_subpromemcall_ms.tcl',
'NG1ENT_dw_ng1_ent_subproshould_ms.tcl',
'NG1ENT_dw_ng1_ent_term_call_ms.tcl',
'NG1ENT_dw_ng1_ent_term_user_ms.tcl',
'NG1ENT_dw_ng1_ent_value_det_info_ms.tcl',
'NG1ENT_dw_ng1_ent_vipscorereturn_ms.tcl',
'NG1ENT_dw_ng1_ent_vpmn_ms.tcl',
'NG1ENT_dw_ng1_entmem_busistat_ms.tcl',
'NG1ENT_st_ng1_ent_busi_use_mm.tcl',
'NG1ENT_st_ng1_ent_busistat_mm.tcl',
'NG1ENT_st_ng1_ent_calllac_mm.tcl',
'NG1ENT_st_ng1_ent_complaint_mm.tcl',
'NG1ENT_st_ng1_ent_costbenefit_mm.tcl',
'NG1ENT_st_ng1_ent_custincome_mm.tcl',
'NG1ENT_st_ng1_ent_industry_use_mm.tcl',
'NG1ENT_st_ng1_ent_industryapp_mm.tcl',
'NG1ENT_st_ng1_ent_levchange_mm.tcl',
'NG1ENT_st_ng1_ent_manage_mm.tcl',
'NG1ENT_st_ng1_ent_markprog_mm.tcl',
'NG1ENT_st_ng1_ent_mem_univ_mm.tcl',
'NG1ENT_st_ng1_ent_mm.tcl',
'NG1ENT_st_ng1_ent_owe_analy_mm.tcl',
'NG1ENT_st_ng1_ent_platform_use_mm.tcl',
'NG1ENT_st_ng1_ent_precust_mm.tcl',
'NG1ENT_st_ng1_ent_prosol_adcmas_mm.tcl',
'NG1ENT_st_ng1_ent_prosol_mm.tcl',
'NG1ENT_st_ng1_ent_serivce_mm.tcl',
'NG1ENT_st_ng1_ent_sershould_mm.tcl',
'NG1ENT_st_ng1_ent_smslac_ad_mm.tcl',
'NG1ENT_st_ng1_ent_subpro_cr_mm.tcl',
'NG1ENT_st_ng1_ent_subpro_pmail_mm.tcl',
'NG1ENT_st_ng1_ent_subpro_swbd_mm.tcl',
'NG1ENT_st_ng1_ent_subpro_vpmn_mm.tcl',
'NG1ENT_st_ng1_ent_subprocall_mm.tcl',
'NG1ENT_st_ng1_ent_subproshould_mm.tcl',
'NG1ENT_st_ng1_ent_term_nums_mm.tcl',
'NG1ENT_st_ng1_ent_term_use_mm.tcl',
'NG1ENT_st_ng1_ent_total_fee_mm.tcl',
'NG1REP_dw_ng1_report_0001.tcl',
'NG1REP_dw_ng1_report_0005.tcl',
'NG1REP_dw_ng1_report_0006.tcl',
'NG1REP_dw_ng1_report_0008.tcl',
'NG1REP_dw_ng1_report_0009.tcl',
'NG1REP_dw_ng1_report_0010.tcl',
'NG1REP_dw_ng1_report_0011.tcl',
'NG1REP_dw_ng1_report_0012.tcl',
'NG1REP_dw_ng1_report_0014.tcl',
'NG1REP_dw_ng1_report_0015.tcl',
'NG1REP_dw_ng1_report_0016.tcl',
'NG1REP_dw_ng1_report_0018.tcl',
'NG1REP_dw_ng1_report_0020.tcl',
'NG1st_ng1_csvc_complaint_point_mm.tcl',
'NG1st_ng1_csvc_complaint_specust_mm.tcl',
'NG1st_ng1_csvc_locale_complaint_mm.tcl',
'NG1st_ng1_csvc_othernet_consult_mm.tcl',
'NG1st_ng1_csvc_re_complaint_mm.tcl',
'NG1st_ng1_csvc_timelimit_mm.tcl',
'NG1st_ng1_csvc_update_complaint_mm.tcl',
'NG1st_ng1_custdev_brand_change_analy_ms.tcl',
'NG1st_ng1_custdev_brand_new_off_analy_ms.tcl',
'NG1st_ng1_custdev_brand_total_analy_ms.tcl',
'REP_app_fxyx_offuser_list_total_m.tcl',
'REP_app_fxyx_onuser_list_arpu.tcl',
'REP_app_fxyx_onuser_list_credit.tcl',
'REP_app_fxyx_onuser_list_fee_m.tcl',
'REP_app_fxyx_onuser_list_planrank.tcl',
'REP_app_fxyx_onuser_list_total_m.tcl',
'REP_App_ent_churn_analysis_mm.tcl',
'REP_App_ent_churn_entalarm_list_ms.tcl',
'REP_App_ent_churn_entalarm_type_ms.tcl',
'REP_App_ent_churn_memalarm_list_ms.tcl',
'REP_App_ent_churn_memalarm_type_ms.tcl',
'REP_App_ent_churn_memstable_type_ms.tcl',
'REP_App_ent_churn_vipalarm_type_ms.tcl',
'REP_App_ent_evaluate_coneandlvl_mm.tcl',
'REP_App_ent_evaluate_conesurvey_mm.tcl',
'REP_App_ent_evaluate_entscorecalt_mm.tcl',
'REP_App_ent_evaluate_entscoremant_mm.tcl',
'REP_App_ent_evaluate_feesurvey_mm.tcl',
'REP_App_ent_evaluate_mktfee_mm.tcl',
'REP_App_ent_evaluate_planeffect_mm.tcl',
'REP_App_ent_evaluate_planimpact_mm.tcl',
'REP_App_ent_evaluate_plannow_mm.tcl',
'REP_App_xysc_comp_user_ms.tcl',
'REP_App_xysc_new_user_ms.tcl',
'REP_App_xysc_new_user_xs_ms.tcl',
'REP_App_xysc_school_user_ms.tcl',
'REP_channel_bbusi_reward_ms.tcl',
'REP_channel_charge_reward_ms.tcl',
'REP_channel_etop_reward_ms.tcl',
'REP_channel_nbusi_reward_ms.tcl',
'REP_channel_user_reward_ms.tcl',
'REP_home_user_allcall_ms.tcl',
'REP_home_user_base_call_ms.tcl',
'REP_home_user_call_2.tcl',
'REP_home_user_call_3.tcl',
'REP_home_user_call_opp.tcl',
'REP_home_user_callfee.tcl',
'REP_home_user_channel_concat_ms.tcl',
'REP_home_user_channel_info.tcl',
'REP_home_user_comp_dig.tcl',
'REP_home_user_fee_analysis.tcl',
'REP_home_user_fee.tcl',
'REP_home_user_match_busi.tcl',
'REP_home_user_promo_trend.tcl',
'REP_home_user_promofav_analysis.tcl',
'REP_home_user_promotype_analysis.tcl',
'REP_home_user_sens_analysis.tcl',
'REP_home_user_termtype_analysis.tcl',
'REP_report_stat_twocity_mm.tcl',
'REP_st_echannel_report_mm.tcl',
'REP_st_ng1_chl_phy_rwd_openphone_mm.tcl',
'REP_stat_acct_td_001.tcl',
'REP_stat_acct_td_002.tcl',
'REP_stat_acct_td_003.tcl',
'REP_stat_acct_td_004.tcl',
'REP_stat_areaserver_0001_0010.tcl',
'REP_stat_busichl_0001.tcl',
'REP_stat_busichl_0002.tcl',
'REP_stat_channel_0004.tcl',
'REP_stat_channel_0006.tcl',
'REP_stat_channel_0007.tcl',
'REP_stat_channel_0021.tcl',
'REP_stat_channel_0022.tcl',
'REP_stat_channel_0023.tcl',
'REP_stat_channel_0024.tcl',
'REP_stat_channel_reward_0002.tcl',
'REP_stat_channel_reward_0003.tcl',
'REP_stat_channel_reward_0004.tcl',
'REP_stat_channel_reward_0005.tcl',
'REP_stat_channel_reward_0006.tcl',
'REP_stat_channel_reward_0007.tcl',
'REP_stat_channel_reward_0008.tcl',
'REP_stat_channel_reward_0009.tcl',
'REP_stat_channel_reward_0010.tcl',
'REP_stat_channel_reward_0011.tcl',
'REP_stat_channel_reward_0012.tcl',
'REP_stat_channel_reward_0013.tcl',
'REP_stat_channel_reward_0014.tcl',
'REP_stat_channel_reward_0015.tcl',
'REP_stat_channel_reward_0016.tcl',
'REP_stat_channel_reward_0017.tcl',
'REP_stat_channel_reward_0018.tcl',
'REP_stat_channel_reward_0019.tcl',
'REP_stat_channel_reward_0020.tcl',
'REP_stat_channel_reward_detail_ms.tcl',
'REP_stat_cust_value_0002.tcl',
'REP_stat_cust_value_0003.tcl',
'REP_stat_data_0023_rkz.tcl',
'REP_stat_data_0023.tcl',
'REP_stat_data_0024.tcl',
'REP_stat_data_0026.tcl',
'REP_stat_data_0030.tcl',
'REP_stat_data_0031.tcl',
'REP_stat_data_0032.tcl',
'REP_stat_data_0033.tcl',
'REP_stat_data_0034.tcl',
'REP_stat_data_0035.tcl',
'REP_stat_data_0036.tcl',
'REP_stat_data_0038.tcl',
'REP_stat_data_0039.tcl',
'REP_stat_data_0040.tcl',
'REP_stat_data_0041.tcl',
'REP_stat_data_0043.tcl',
'REP_stat_data_0044.tcl',
'REP_stat_data_0045.tcl',
'REP_stat_data_0046.tcl',
'REP_stat_data_0047.tcl',
'REP_stat_data_0048.tcl',
'REP_stat_data_0049.tcl',
'REP_stat_data_0050.tcl',
'REP_stat_data_0051.tcl',
'REP_stat_data_0052.tcl',
'REP_stat_data_0053.tcl',
'REP_stat_data_0055.tcl',
'REP_stat_data_0056.tcl',
'REP_stat_data_0057.tcl',
'REP_stat_data_0058.tcl',
'REP_stat_data_0059.tcl',
'REP_stat_data_0060.tcl',
'REP_stat_data_0061.tcl',
'REP_stat_data_0062.tcl',
'REP_stat_data_0063.tcl',
'REP_stat_data_0064.tcl',
'REP_stat_data_0065.tcl',
'REP_stat_data_0066.tcl',
'REP_stat_ecustsvc_0006.tcl',
'REP_stat_ecustsvc_0009.tcl',
'REP_stat_ecustsvc_0010.tcl',
'REP_stat_ecustsvc_0011.tcl',
'REP_stat_ecustsvc_0012.tcl',
'REP_stat_ecustsvc_0013.tcl',
'REP_stat_ecustsvc_0014.tcl',
'REP_stat_ecustsvc_0016.tcl',
'REP_stat_ecustsvc_0017.tcl',
'REP_stat_ecustsvc_0018.tcl',
'REP_stat_ecustsvc_0022.tcl',
'REP_stat_ecustsvc_0024.tcl',
'REP_stat_ecustsvc_0025.tcl',
'REP_stat_ecustsvc_0031.tcl',
'REP_stat_ecustsvc_0032.tcl',
'REP_stat_ecustsvc_0033.tcl',
'REP_stat_ecustsvc_0034.tcl',
'REP_stat_ecustsvc_0035.tcl',
'REP_stat_ecustsvc_0036.tcl',
'REP_stat_ecustsvc_0037.tcl',
'REP_stat_ecustsvc_0038.tcl',
'REP_stat_ecustsvc_0039.tcl',
'REP_stat_ecustsvc_0040.tcl',
'REP_stat_ecustsvc_0041.tcl',
'REP_stat_ecustsvc_0043.tcl',
'REP_stat_ecustsvc_0045.tcl',
'REP_stat_ecustsvc_0046_a.tcl',
'REP_stat_ecustsvc_0046_b.tcl',
'REP_stat_ecustsvc_0047.tcl',
'REP_stat_ecustsvc_0048.tcl',
'REP_stat_ecustsvc_0049.tcl',
'REP_stat_ecustsvc_0050.tcl',
'REP_stat_enterprise_0002.tcl',
'REP_stat_enterprise_0005.tcl',
'REP_stat_enterprise_0009.tcl',
'REP_stat_enterprise_0013.tcl',
'REP_stat_enterprise_0014.tcl',
'REP_stat_enterprise_0015.tcl',
'REP_stat_enterprise_0016.tcl',
'REP_stat_enterprise_0017.tcl',
'REP_stat_enterprise_0018.tcl',
'REP_stat_enterprise_0020.tcl',
'REP_stat_enterprise_0021.tcl',
'REP_stat_enterprise_0023.tcl',
'REP_stat_enterprise_0024.tcl',
'REP_stat_enterprise_0025.tcl',
'REP_stat_enterprise_0026.tcl',
'REP_stat_enterprise_0027.tcl',
'REP_stat_enterprise_0028.tcl',
'REP_stat_enterprise_0029.tcl',
'REP_stat_enterprise_0030.tcl',
'REP_stat_enterprise_0031.tcl',
'REP_stat_enterprise_0032_2010_new.tcl',
'REP_stat_enterprise_0032_2010.tcl',
'REP_stat_enterprise_0032_2011_1.tcl',
'REP_stat_enterprise_0032.tcl',
'REP_stat_enterprise_0033_2010.tcl',
'REP_stat_enterprise_0033.tcl',
'REP_stat_enterprise_0034_2010.tcl',
'REP_stat_enterprise_0034.tcl',
'REP_stat_enterprise_0035_2010_new.tcl',
'REP_stat_enterprise_0035_2010.tcl',
'REP_stat_enterprise_0035.tcl',
'REP_stat_enterprise_0036.tcl',
'REP_stat_enterprise_0037.tcl',
'REP_stat_enterprise_0041.tcl',
'REP_stat_enterprise_0044.tcl',
'REP_stat_enterprise_0047.tcl',
'REP_stat_enterprise_0048.tcl',
'REP_stat_enterprise_0049.tcl',
'REP_stat_enterprise_0050.tcl',
'REP_stat_enterprise_0051.tcl',
'REP_stat_enterprise_0052.tcl',
'REP_stat_enterprise_0054_a.tcl',
'REP_stat_enterprise_0054_b.tcl',
'REP_stat_enterprise_0055_a.tcl',
'REP_stat_enterprise_0055_b.tcl',
'REP_stat_imei_0001.tcl',
'REP_stat_local_0001.tcl',
'REP_stat_market_0014.tcl',
'REP_stat_market_0029.tcl',
'REP_stat_market_0040.tcl',
'REP_stat_market_0042.tcl',
'REP_stat_market_0063.tcl',
'REP_stat_market_0064.tcl',
'REP_stat_market_0065.tcl',
'REP_stat_market_0066.tcl',
'REP_stat_market_0067.tcl',
'REP_stat_market_0068.tcl',
'REP_stat_market_0069.tcl',
'REP_stat_market_0070.tcl',
'REP_stat_market_0073.tcl',
'REP_stat_market_0074_mm.tcl',
'REP_stat_market_0075_mm.tcl',
'REP_stat_market_0076.tcl',
'REP_stat_market_0096_user.tcl',
'REP_stat_market_0096.tcl',
'REP_stat_market_0097.tcl',
'REP_stat_market_0098.tcl',
'REP_stat_market_0099.tcl',
'REP_stat_market_0100.tcl',
'REP_stat_market_0101.tcl',
'REP_stat_market_0104.tcl',
'REP_stat_market_0105.tcl',
'REP_stat_market_0108.tcl',
'REP_stat_market_0110_user.tcl',
'REP_stat_market_0110.tcl',
'REP_stat_market_0113.tcl',
'REP_stat_market_0114.tcl',
'REP_stat_market_0116_a.tcl',
'REP_stat_market_0116_b.tcl',
'REP_stat_market_0116_c.tcl',
'REP_stat_market_0116_d.tcl',
'REP_stat_market_0116_e.tcl',
'REP_stat_market_0118.tcl',
'REP_stat_market_0119.tcl',
'REP_stat_market_0120.tcl',
'REP_stat_market_0121.tcl',
'REP_stat_market_0122.tcl',
'REP_stat_market_0123.tcl',
'REP_stat_market_0125.tcl',
'REP_stat_market_0126.tcl',
'REP_stat_market_0128.tcl',
'REP_stat_market_0132.tcl',
'REP_stat_market_0133.tcl',
'REP_stat_market_0134.tcl',
'REP_stat_market_td_009.tcl',
'REP_stat_market_td_010.tcl',
'REP_stat_market_td_011.tcl',
'REP_stat_market_td_012.tcl',
'REP_stat_market_td_013.tcl',
'REP_stat_market_td_014.tcl',
'REP_stat_market_td_015.tcl',
'REP_stat_market_td_016.tcl',
'REP_stat_mobile_value_0002.tcl',
'REP_stat_network_0006.tcl',
'REP_stat_network_0011.tcl',
'REP_stat_network_0012.tcl',
'REP_stat_network_0016.tcl',
'REP_stat_network_td_001.tcl',
'REP_stat_network_td_002.tcl',
'REP_stat_network_td_003.tcl',
'REP_stat_newproduct_kpi.tcl',
'REP_stat_plan_0001.tcl',
'REP_stat_plan_0002.tcl',
'REP_stat_rep_channel_2cityin1_mm.tcl',
'REP_stat_rep_city_2cityin1_mm.tcl',
'REP_stat_rep_long_pkg_incomeanddurn.tcl',
'REP_stat_sp_busi_cancel_mm.tcl',
'REP_stat_sp_cancel_top5_mm.tcl',
'REP_stat_sprom_value_0002.tcl',
'REP_stat_threebrand_analyzing_0033.tcl',
'REP_stat_xysc_001.tcl',
'REP_stat_xysc_002.tcl',
'REP_stat_xysc_003.tcl',
'REP_stat_xysc_004.tcl',
'REP_stat_xysc_005.tcl',
'REP_stat_xysc_006.tcl',
'REP_stat_xysc_007.tcl',
'REP_stat_xysc_008.tcl',
'REP_stat_xysc_009.tcl',
'REP_St_ng1_chl_develop_arpu_mm.tcl',
'REP_St_ng1_chl_phy_analy_mm.tcl',
'REP_St_ng1_chl_phy_area_loadpeak_mm.tcl',
'REP_St_ng1_chl_phy_chl_cmps_mm.tcl',
'REP_St_ng1_chl_phy_newuser_live_mm.tcl',
'REP_St_ng1_chl_phy_newuser_srv_mm.tcl',
'REP_St_ng1_chl_res_resalert_mm.tcl',
'REP_St_ng1_chl_term_custanaly_mm.tcl',
'REP_St_ng1_chl_term_custinfo_mm.tcl',
'REP_St_ng1_chl_term_info_mm.tcl',
'REP_St_ng1_chl_term_markinfo_mm.tcl',
'REP_St_ng1_chl_user_payfee_mm.tcl',
'REP_St_ng1_chl_user_query_mm.tcl',
'REP_St_ng1_chl_user_score_mm.tcl',
'REP_St_ng1_chl_xp_busirec_mm.tcl',
'REP_St_ng1_echl_acceptsuc_rate_mm.tcl',
'REP_St_ng1_echl_busi_det_mm.tcl',
'REP_St_ng1_echl_busi_impt_mm.tcl',
'REP_St_ng1_echl_busi_mm.tcl',
'REP_St_ng1_echl_busianaly_mm.tcl',
'REP_St_ng1_echl_kpi_mm.tcl',
'REP_St_ng1_echl_sms_user_busi_mm.tcl',
'REP_St_ng1_echl_sms_user_menu_mm.tcl',
'REP_St_ng1_echl_user_busi_det_mm.tcl',
'REP_St_ng1_echl_wap_user_busi_mm.tcl',
'REP_St_ng1_echl_wap_user_oper_mm.tcl',
'REP_St_ng1_echl_web_user_busi_mm.tcl',
'REP_St_ng1_echl_web_user_oper_mm.tcl',
'REP_St_ng1_term_cycle_query_mm.tcl',
'REP_td_cust_busi_user_dtl.tcl',
'REP_td_cust_follow_friend_dtl.tcl',
'REP_td_cust_sjzq_user_dtl.tcl',
'REP_td_cust_vip_netuser_dtl.tcl',
'REP_td_cust_wlan_user_dtl.tcl'
) 
where alarmtime >=  timestamp('20110402'||'000000') 
--and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc 
where time_id=int(replace(char(current date - 2 days),'-',''))
and return_flag=1
except
select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=0
)

where time_id=int(replace(char(current date - 2 days),'-',''))
and return_flag=1
except
select unit_code from app.g_runlog 
where time_id=int(replace(char(current date - 1 days),'-',''))
and return_flag=0
          (SELECT control_code, before_control_code 
             FROM app.sch_control_before
             WHERE before_control_code = 'BASS2_Dw_acct_should_dtl_today_ds.tcl'
           UNION ALL
           SELECT b.control_code,b.before_control_code 
             FROM app.sch_control_before as b, n
             WHERE b.before_control_code = n.control_code)
SELECT distinct c.control_code FROM n,app.sch_control_task c
where n.control_code = c.control_code
and c.deal_time = 1
and c.control_code not like 'OLAP_%'
(
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = 'ÿ��3��ǰ'
)

BASS1_G_I_02019_MONTH.tcl	2011-04-02 17:56:01.577983	2011-04-02 17:56:13.359047	11	-2
BASS1_G_I_02021_MONTH.tcl	2011-04-02 17:58:01.525347	2011-04-02 18:17:19.011434	1157	-2
BASS1_G_I_02020_MONTH.tcl	2011-04-02 17:58:40.074220	2011-04-02 17:59:01.097497	21	-2

set flag = -2 
where control_code in 
(
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.TYPE = 'm'
and b.control_code like '%MONTH%'
and upload_time = 'ÿ��3��ǰ'
)

set flag = -2 
where control_code in 
(
select control_code from app.sch_control_runlog 
where control_code in 
            select distinct control_code 
            where  before_control_code in (
                    select b.CONTROL_CODE from    
                    BASS1.MON_ALL_INTERFACE a
                    , app.sch_control_task b 
                    and a.TYPE = 'm'
                    and b.control_code like '%MONTH%'
                    and upload_time = 'ÿ��3��ǰ'
            )
and control_code like '%CHECK%'
and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
)
and flag = 0
and date(endtime) <= current date
)

--���˵�������
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
select control_code from app.sch_control_runlog 
where control_code in (
select distinct control_code from   app.sch_control_before 
where  before_control_code in (
    select b.CONTROL_CODE from    
    BASS1.MON_ALL_INTERFACE a
    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
    and a.TYPE = 'm'
    and b.control_code like '%MONTH%'
    and upload_time = 'ÿ��3��ǰ'
    )
    and control_code like 'BASS1%EXP%'
)
and flag = 0
and date(endtime) <= current date
)
and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')
		where control_code in 
		(
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��3��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)
		where control_code in (
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��3��ǰ'
				)
		and control_code like '%CHECK%'
		and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��3��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��3��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)
		and date(endtime) <= current date and month(endtime)  = month(current timestamp)
BASS1_EXP_G_I_06029_MONTH	2011-05-01 12:04:04.291375	2011-05-01 12:06:46.503925	162	-2

(
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��3��ǰ'
)        
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��3��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��3��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)
		--and flag <> 0
		and date(endtime) <= current date and month(endtime)  = month(current timestamp)
BASS1_EXP_G_I_02019_MONTH	2011-04-02 17:57:22.727616	2011-04-02 17:58:03.608087	40	-2
BASS1_EXP_G_I_02020_MONTH	2011-04-02 18:01:55.786727	2011-04-02 18:15:35.176069	819	-2
BASS1_EXP_G_I_02021_MONTH	2011-04-02 18:17:23.337166	2011-04-02 18:53:23.070741	2159	-2

BASS1_G_I_02019_MONTH.tcl	2011-04-02 17:56:01.577983	2011-04-02 17:56:13.359047	11	-2
BASS1_G_I_02021_MONTH.tcl	2011-04-02 17:58:01.525347	2011-04-02 18:17:19.011434	1157	-2
BASS1_G_I_02020_MONTH.tcl	2011-04-02 17:58:40.074220	2011-04-02 17:59:01.097497	21	-2


		where control_code in 
		(
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��3��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)
		and flag = 0
		and date(endtime) < current date
		and month(endtime)  = month(current timestamp)
		where control_code in 
		(
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��3��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)     
		where control_code in 
		where control_code in (
		where control_code in (
		where control_code in (
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��3��ǰ'
	union all
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��3��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)	
	union all 
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��3��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)	
	
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��3��ǰ'
	union all
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��3��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)	
	union all 
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��3��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)	
)
 = 111090001340
('06001'
,'02061'
,'02062'
,'02022'
,'02023'
,'02056'
,'04014'
,'02055'
,'02064'
,'02058'
,'02060'
,'02057'
,'04003'
,'04010'
,'04009'
,'04012'
,'02013'
,'21009'
,'21005'
,'21004'
,'21016'
,'01006'
,'01007')
 ('22080','ͳһ��ѯ�˶��ջ���')
,('22081','ͳһ��ѯ���˶��»���')
,('22082','ҵ��۷����������ջ���')
,('22083','ҵ��۷����������»���')
,('22084','�շ��������˷Ѻ��֤�ջ���')
,('22085','�շ��������˷Ѻ��֤�»���')
('BASS1_G_S_02047_MONTH.tcl'
,'BASS1_G_I_02015_MONTH.tcl'
,'BASS1_G_I_06002_MONTH.tcl'
,'BASS1_G_I_06021_MONTH.tcl'
,'BASS1_G_A_01005_MONTH.tcl'
,'BASS1_G_I_02016_MONTH.tcl'
,'BASS1_G_I_06023_MONTH.tcl'
,'BASS1_G_I_02014_MONTH.tcl'
,'BASS1_G_I_06022_MONTH.tcl')                         
,'BASS1_G_I_02015_MONTH.tcl'
,'BASS1_G_I_06002_MONTH.tcl'
,'BASS1_G_I_06021_MONTH.tcl'
,'BASS1_G_A_01005_MONTH.tcl'
,'BASS1_G_I_02016_MONTH.tcl'
,'BASS1_G_I_06023_MONTH.tcl'
,'BASS1_G_I_02014_MONTH.tcl'
,'BASS1_G_I_06022_MONTH.tcl')    
where 
	 control_code like '%01005%MONTH%'
or control_code like '%02014%MONTH%'
or control_code like '%02015%MONTH%'
or control_code like '%02016%MONTH%'
or control_code like '%02047%MONTH%'
or control_code like '%06021%MONTH%'
or control_code like '%06022%MONTH%'
or control_code like '%06023%MONTH%'
or control_code like '%06002%MONTH%'
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��8��ǰ'
	union all
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��8��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)	
	union all 
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��8��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)	
	
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
		(
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��8��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)

)
  
set flag = -2 
where control_code in 
(
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��8��ǰ'
)
and 		 date(endtime) < current date
and month(endtime)  = month(current timestamp)
set flag = -2 
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
		(
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��8��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)
		and flag = 0
)
and date(endtime) < current date
and month(endtime)  = month(current timestamp) 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��8��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)
		and flag = 0
		and date(endtime) <= current date 
		and month(endtime)  = month(current timestamp)
)
and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')
	and date(endtime) < current date
	and month(endtime)  = month(current timestamp)
set flag = -2 
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��8��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)
		and flag = 0
		and date(endtime) <= current date 
		and month(endtime)  = month(current timestamp)
)
and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')
	and date(endtime) < current date
	and month(endtime)  = month(current timestamp)
		
update BASS1.G_I_02005_MONTH
set CHG_VIP_TIME = '20100917'
where user_id = '89160000265019'
and time_id = 201104


update BASS1.G_I_02005_MONTH
set CHG_VIP_TIME = '20110321'
where user_id = '89460000740915'
and time_id = 201104


update  app.sch_control_task a
set  time_value = 310
where control_code in 
('BASS1_G_S_02047_MONTH.tcl'
,'BASS1_G_I_02015_MONTH.tcl'
,'BASS1_G_I_06002_MONTH.tcl'
,'BASS1_G_I_06021_MONTH.tcl'
,'BASS1_G_A_01005_MONTH.tcl'
,'BASS1_G_I_02016_MONTH.tcl'
,'BASS1_G_I_06023_MONTH.tcl'
,'BASS1_G_I_02014_MONTH.tcl'
,'BASS1_G_I_06022_MONTH.tcl')    
and time_value = 312

update  app.sch_control_task a
set  time_value = 310
where control_code in 
('BASS1_G_S_02047_MONTH.tcl'
,'BASS1_G_I_02015_MONTH.tcl'
,'BASS1_G_I_06002_MONTH.tcl'
,'BASS1_G_I_06021_MONTH.tcl'
,'BASS1_G_A_01005_MONTH.tcl'
,'BASS1_G_I_02016_MONTH.tcl'
,'BASS1_G_I_06023_MONTH.tcl'
,'BASS1_G_I_02014_MONTH.tcl'
,'BASS1_G_I_06022_MONTH.tcl')    
and time_value = 312

	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(b.control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��10��ǰ'
update  app.sch_control_task a
set time_value = 310
where control_code in 
(
 'BASS1_G_S_22103_MONTH.tcl'
,'BASS1_G_S_22106_MONTH.tcl'
,'BASS1_G_I_02005_MONTH.tcl'
,'BASS1_G_S_22105_MONTH.tcl'
,'BASS1_G_S_22009_MONTH.tcl'
,'BASS1_G_S_22101_MONTH.tcl'
)    
and time_value = 212
		  bass2.Dim_prod_up_offer b
		where a.item_type = 'OFFER_PLAN' 
		and a.product_item_id = b.offer_id
                  bass2.Dim_prod_up_offer b
                where a.item_type = 'OFFER_PLAN' 
                and a.product_item_id = b.offer_id
                and a.platform_id in (1,2)
                and replace(char(date(a.create_date)),'-','')<='20110430'
                and replace(char(date(b.create_date)),'-','')<='20110430'
select time_id , count(0) 
--,  count(distinct time_id ) 
from bass1.G_A_02052_MONTH 
group by  time_id 
order by 1 

from
(
select time_id,user_id,region_flag
        ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.G_A_02052_MONTH
where time_id<=201007
) a where row_id = 1 
from
(
select time_id,user_id,region_flag
        ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.G_A_02052_MONTH
where time_id<=200907
) a where row_id = 1 
        from (
                select count(0)  cnt
                from bass1.G_I_02018_MONTH
                where time_id =201104
                group by base_prod_id having count(0) > 1
                ) t 
--,  count(distinct time_id ) 
from G_I_02021_MONTH 
group by  time_id 
order by 1 

	CREATE TABLE BASS1.g_i_02019_month_1
	 (
		  base_prod_id        bigint,
			trademark           bigint
	 )
	  DATA CAPTURE NONE
	 IN TBS_APP_BASS1
	 INDEX IN TBS_INDEX
	  PARTITIONING KEY
	   (base_prod_id
	   ) USING HASHING;
	
	ALTER TABLE BASS1.g_i_02019_month_1
	  LOCKSIZE ROW
	  APPEND OFF
	  NOT VOLATILE;
	 (
		  base_prod_id        bigint,
			base_prod_name      character(200),
			prod_status         character(1),
			prod_begin_time     character(8),
			prod_end_time       character(8),
			platform_id         int,
			trademark           int
	 )
	  DATA CAPTURE NONE
	 IN TBS_APP_BASS1
	 INDEX IN TBS_INDEX
	  PARTITIONING KEY
	   (base_prod_id
	   ) USING HASHING;
	
	ALTER TABLE BASS1.g_i_02019_month_2
	  LOCKSIZE ROW
	  APPEND OFF
	  NOT VOLATILE;
		       base_prod_id
		    from bass1.g_i_02019_month_2
from         t_region_flag a
inner join t_int_check_user_status b on a.user_id = b.user_id
inner join      t_BASE_BILL_DURATION c on b.PRODUCT_NO = c.PRODUCT_NO
group by region_flag
1	227778905
2	298697296
3	138782850
[NULL]	15339226


select count(0) 
,count(distinct a.user_id) tb_user_cnt
,count(distinct b.user_id) tb_region_cnt
,sum(case when a.user_id is not null and b.user_id is null then 1 else 0 end ) no_region_cnt
,sum(case when a.user_id is  null and b.user_id is not null then 1 else 0 end ) no_user_sts_cnt
from   t_int_check_user_status1003 a
full join t_region_flag1003 b  on a.user_id = b.user_id 

,count(distinct a.user_id) tb_user_cnt
,count(distinct b.user_id) tb_region_cnt
,count(case when a.user_id is not null and b.user_id is null then a.user_id end ) no_region_cnt
,count(case when a.user_id is not null and b.user_id is not null then a.user_id end ) join_cnt
,sum(case when a.user_id is  null and b.user_id is not null then 1 else 0 end ) no_user_sts_cnt
from    (select * from t_int_check_user_status a where  a.TEST_FLAG = '0' ) a
full join t_region_flag b  on a.user_id = b.user_id 
join t_region_flag c on a.user_id = c.user_id 

select REGION_FLAG , count(0) 
--,  count(distinct REGION_FLAG ) 
from G_A_02052_MONTH 
group by  REGION_FLAG 
order by 1 

	 (
		  base_prod_id        bigint
	 )
	  DATA CAPTURE NONE
	 IN TBS_APP_BASS1
	 INDEX IN TBS_INDEX
	  PARTITIONING KEY
	   (base_prod_id
	   ) USING HASHING;
	
	ALTER TABLE BASS1.g_i_02019_month_4
	  LOCKSIZE ROW
	  APPEND OFF
	  NOT VOLATILE;
	
select time_id , count(0) 
--,  count(distinct time_id ) 
from G_I_02021_MONTH 
group by  time_id 
order by 1 

201104	4542241
201103	4490016
201102	4410991
201101	4403127
201012	4325562


select * from syscat.indexes where tabname = upper('tablename')
�鿴���
select * from SYSCAT.REFERENCES where tabname = upper('tablename')
�鿴������
select * from SYSCAT.TRIGGERS where tabname = upper('tablename')

							from  BASS1.ALL_DIM_LKP  b
							where BASS1_TBID = 'BASS_STD1_0114'
					      and bass1_value like 'QW_QQT_JC%'
				        and a.BASE_PROD_ID = b.XZBAS_VALUE 
set a.BASE_PROD_ID = (select bass1_value 
							from  BASS1.ALL_DIM_LKP  b
							where b.BASS1_TBID = 'BASS_STD1_0114'
					      and b.bass1_value like 'QW_QQT_JC%'
 set a.ENTERPRISE_ID = (select b.new_enterprise_id from  BASS1.grp_id_old_new_map_20110330  b 
 				where   a.ENTERPRISE_ID = b.old_enterprise_id )
		from bass2.dw_product_ins_prod_201104 a
	  left join (select xzbas_value  as offer_id ,bass1_value bass1_offer_id
							from  BASS1.ALL_DIM_LKP 
							where BASS1_TBID = 'BASS_STD1_0114'
					      and bass1_value like 'QW_QQT_JC%'
				      ) d on char(a.offer_id) = d.offer_id 
		    ,(
		    select product_instance_id user_id from bass2.dw_product_ins_prod_201104
		    where state in ('1','4','6','8','M','7','C','9')
		      and user_type_id =1
		      and valid_type = 1
		      and bill_id not in ('D15289014474','D15289014454')
		    except
		    select user_id from bass2.dw_product_test_phone_201104
		    where sts=1
		    ) b
		where a.product_instance_id=b.user_id
		  and a.state in ('1','4','6','8','M','7','C','9')
		  and a.user_type_id =1
		  and a.valid_type = 1
		  and a.bill_id not in ('D15289014474','D15289014454')
select time_id , count(0) 
--,  count(distinct time_id ) 
from g_i_02020_month 
group by  time_id 
order by 1 

201104	1623498
201103	1598727
201102	1570869
201101	1568523
201012	1567598

1623498

select BASE_PROD_ID , count(0) 
--,  count(distinct BASE_PROD_ID ) 
from g_i_02020_month 
group by  BASE_PROD_ID 
order by 1 


	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
    ,app.sch_control_runlog c 
	where a.INTERFACE_CODE = substr(b.control_code , 11,5)
    and b.control_code = c.control_code 
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��8��ǰ'
        

select b.*
and          upload_time = 'ÿ��8��ǰ'
--,  count(distinct STAFF_ORG_ID ) 
from BASS2.dw_acct_payment_dm_201104 
group by  STAFF_ORG_ID 
order by 1 

--,  count(distinct CHANNEL_ID ) 
from BASS2.dw_acct_payment_dm_201104 
group by  CHANNEL_ID 
order by 1 
select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22062_MONTH 
group by  time_id 
order by 1 

201103	1147
201102	1065
201101	1101
201012	1179
201011	1198

     sum(bigint(new_users))*1.0000/1723732*100
     ,sum(bigint(hand_cnt))*1.0000/1723732*100
     ,sum(bigint(card_sale_cnt))*1.0000/1723732*100
     ,sum(bigint(accept_cnt))*1.0000/1723732*100
     ,sum(bigint(term_sale_cnt))*1.0000/1723732*100
     ,sum(bigint(accept_bas_cnt))*1.0000/1723732*100     
     ,sum(bigint(query_bas_cnt))*1.0000/1723732*100     
from g_s_22062_month
where time_id =201104
select   sum(cnt)
  from (
select accept_type,CHANNEL_STATUS,
     sum(bigint(new_users))+
    sum(bigint(hand_cnt))+
    sum(bigint(card_sale_cnt))+
    sum(bigint(accept_cnt))+
    --sum(bigint(imp_accept_cnt))+
    sum(bigint(term_sale_cnt))+
    --sum(bigint(other_sale_cnt))+
    sum(bigint(accept_bas_cnt)) +
    sum(bigint(query_bas_cnt)) cnt
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201104
  and b.time_id =201104
group by accept_type,CHANNEL_STATUS
) t 
   
     sum(bigint(new_users)),
    sum(bigint(hand_cnt)),
    sum(bigint(card_sale_cnt)),
    sum(bigint(accept_cnt)),
    sum(bigint(term_sale_cnt)),
    sum(bigint(other_sale_cnt)),
    sum(bigint(accept_bas_cnt)),
    sum(bigint(query_bas_cnt))
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201104
  and b.time_id =201104
     sum(bigint(new_users))
    ,sum(bigint(hand_cnt))
    ,sum(bigint(card_sale_cnt))
    ,sum(bigint(accept_cnt))
    ,sum(bigint(term_sale_cnt))
    ,sum(bigint(other_sale_cnt))
    ,sum(bigint(accept_bas_cnt))
    ,sum(bigint(query_bas_cnt))
from g_s_22062_month a,g_i_06021_month b
where a.CHANNEL_ID=b.CHANNEL_ID
  and a.time_id =201104
  and b.time_id =201104
          ,case when opt_code in ('4464','4465','4864','4865') then '2' else '1' end
          ,0
          ,count(distinct payment_id)
          ,sum(amount)
          ,sum(case when opt_code='4158' AND state='0' then 1 else 0 end )
          ,sum(case when opt_code='4158' AND state='0' then amount else 0 end )
          ,0
          ,0
          ,0
          ,0
          ,0
          ,0
          ,0
from BASS2.dw_acct_payment_dm_201104
group by staff_org_id
                  ,case when opt_code in ('4464','4465','4864','4865') then '2' else '1' end


set flag = -2 
where control_code in 
(
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��15��ǰ'
)
and  flag = 0 

--����У�����
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
		(
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��15��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)
		and flag = 0
		and date(endtime) < current date
		and month(endtime)  = month(current timestamp)
)

          ,sum(case when opt_code='4158' AND state='0' then 1 else 0 end )
from BASS2.dw_acct_payment_dm_201104
group by staff_org_id
                  
select * from t_region_flag
where 
user_id 
in 
(
 '89357333501989'
,'89457332997454'
,'89657333835100'
,'89557333500619'
,'89157333653495'
,'89501150037395'
,'89401140015226'
,'89157332357663'
,'89157333426659'
,'89257332794215'
,'89457332253242'
,'89657332860422'
,'89357334150278'
,'89357334219964'
,'89257333683395'
,'89157333830554'
,'89157333760965'
,'89757333409956'
)

                 from
                (
                 select user_id,usertype_id from
                 (
                  select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id
                  from bass1.G_A_02008_DAY
                  where time_id<=20110331
                 ) k
                where k.row_id=1 
                ) a
                inner join (select distinct user_id from G_A_02004_DAY
                where time_id<=20110331
                ) c
                 on a.user_id=c.user_id
                
                left outer join (select user_id,region_flag
                from
                (
                select user_id,region_flag,row_number()over(partition by user_id order by time_id desc) row_id 
                from bass1.G_A_02052_MONTH
                ) k
                where k.row_id=1) b
                 on a.user_id=b.user_id
                  and b.REGION_FLAG in('1','2','3')
                where  a.usertype_id not in ('2010','2020','2030','9000')
a.user_id 
in 
(
 '89357333501989'
,'89457332997454'
,'89657333835100'
,'89557333500619'
,'89157333653495'
,'89501150037395'
,'89401140015226'
,'89157332357663'
,'89157333426659'
,'89257332794215'
,'89457332253242'
,'89657332860422'
,'89357334150278'
,'89357334219964'
,'89257333683395'
,'89157333830554'
,'89157333760965'
,'89757333409956'
,'89257333515849'
)
                with ur
in (
		 '89357333501989'
		,'89457332997454'
		,'89657333835100'
		,'89557333500619'
		,'89157333653495'
		,'89501150037395'
		,'89401140015226'
		,'89157332357663'
		,'89157333426659'
		,'89257332794215'
		,'89457332253242'
		,'89657332860422'
		,'89357334150278'
		,'89357334219964'
		,'89257333683395'
		,'89157333830554'
		,'89157333760965'
		,'89757333409956'
        ,'89257333515849'
                with ur
                 from
                (
	                 select user_id,usertype_id from
	                 (
	                  select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id
	                  from bass1.G_A_02008_DAY
	                  where time_id<=20110331
	                 ) k
                where k.row_id=1 
                ) a
                inner join (
		                select distinct user_id from G_A_02004_DAY
		                where usertype_id<>'3'
		                and time_id<=20110331               
			   ) c
                 on a.user_id=c.user_id
                left outer join 
		(
			select user_id,region_flag
			from
			(
				select user_id,region_flag,row_number()over(partition by user_id order by time_id desc) row_id 
				from bass1.G_A_02052_MONTH
	                ) k
	                where k.row_id=1
                ) b on a.user_id=b.user_id and b.REGION_FLAG in('1','2','3')
                where  a.usertype_id not in ('2010','2020','2030','2040','9000')
		and a.user_id in 
		(
		 '89357333501989'
		,'89457332997454'
		,'89657333835100'
		,'89557333500619'
		,'89157333653495'
		,'89501150037395'
		,'89401140015226'
		,'89157332357663'
		,'89157333426659'
		,'89257332794215'
		,'89457332253242'
		,'89657332860422'
		,'89357334150278'
		,'89357334219964'
		,'89257333683395'
		,'89157333830554'
		,'89157333760965'
		,'89757333409956'
,'89257333515849'        
		)
                with ur                  
                 from
                (
	                 select user_id,usertype_id from
	                 (
	                  select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id
	                  from bass1.G_A_02008_DAY
	                  where time_id<=20110331
	                 ) k
                where k.row_id=1 
                ) a
                inner join (
		                select distinct user_id from G_A_02004_DAY
		                where time_id<=20110331               
			   ) c
                 on a.user_id=c.user_id
                left outer join 
		(
			select user_id,region_flag
			from
			(
				select user_id,region_flag,row_number()over(partition by user_id order by time_id desc) row_id 
				from bass1.G_A_02052_MONTH
	                ) k
	                where k.row_id=1
                ) b on a.user_id=b.user_id 
                where  a.user_id 
in (
		 '89357333501989'
		,'89457332997454'
		,'89657333835100'
		,'89557333500619'
		,'89157333653495'
		,'89501150037395'
		,'89401140015226'
		,'89157332357663'
		,'89157333426659'
		,'89257332794215'
		,'89457332253242'
		,'89657332860422'
		,'89357334150278'
		,'89357334219964'
		,'89257333683395'
		,'89157333830554'
		,'89157333760965'
		,'89757333409956'
        ,'89457332338084'
        ,'89257333515849'
        )
                with ur                
			from
			(
				select user_id,region_flag,row_number()over(partition by user_id order by time_id desc) row_id 
				from bass1.G_A_02052_MONTH where time_id <= 201103
	                ) k
	                where k.row_id=1
--,  count(distinct time_id ) 
from bass1.G_A_02052_MONTH 
group by  time_id 
order by 1 

--,  count(distinct length(user_id) ) 
from bass1.G_A_02052_MONTH 
group by  length(trim(user_id))
order by 1 


select * from channel.CHANNEL_LOCAL_BUSI @dbl_ggdb 

--DROP TABLE bass1.ODS_SC_SCRD_ORD_INFO_YYYYMM;			
CREATE TABLE bass2.ODS_SC_SCRD_ORD_INFO_201103 (			
op_time	VARCHAR(8)	--CBOSS�����ݱ�������ֶ�	
,ORD_SEQ	VARCHAR(20)	--������	
,SUB_ORD_SEQ	VARCHAR(20)	--���쳣�������	
,MOB_NUM	VARCHAR(50)	--�û��ֻ����� 	
,ORDER_SUM_POINT	BIGINT	--����Ӧ���ܻ��� 	
,ORD_TYPE	SMALLINT	--01:��������02:�쳣����"	
,EXP_ORD_TYPE	VARCHAR(10)	--01-�����˻� 02-���ջ��� 03-�º��˻� 04-�º󻻻�	
,EXP_REASON	VARCHAR(10)	--01��������  02����Ʒ�� 03���ͻ�	
,ORD_STS	VARCHAR(6)	--����������001-������  002�����ڴ�������  003-�ѷ��� 004����ǩ��  005������  006���û�����  007�������˵�  008�����ջ���  009������ǩ�� 011������ǩ�ջ��ֻع��ɹ�  012���������ֻع��ɹ� 013��ʡ�ھ��ջ��ֻع��ɹ�	
,ORG_ID	VARCHAR(10)	--0001  ����ƽ̨0002  ��������0003  CRM����0004  WAP����"	
,ITEM_ID	VARCHAR(16)	--��ƷID	
,ITEM_NAME	VARCHAR(256)	--��Ʒ���� 	
,ITEM_TYPE	VARCHAR(2)	--��Ʒ���ࣺ01ȫ����02ʡ��	
,ITEM_POINT	BIGINT	--���� 	
,ITEM_POINT_VALUE	BIGINT	--���ּ�ֵ 	
,TYPE1	VARCHAR(10)	--00:ʵ����,01:������,02:������	
,ITEM_E_PRICE	decimal(10,2)	--�һ��۸�	
,ITEM_G_POINT	INTEGER	--ȫ��ͨ����ֵ	
 )
  DATA CAPTURE NONE
 IN TBS_3H
 INDEX IN TBS_INDEX
  PARTITIONING KEY
   ( ord_seq,mob_num )
 USING HASHING;


insert into etl_load_table_map values('M11222','ODS_SC_SCRD_ORD_INFO_YYYYMM','ͳһ����ƽ̨����',0,'AMS.SCRD_ORD_INFO_YYYYMMDD');

insert into bass2.USYS_TABLE_MAINTAIN values(11038,'ͳһ����ƽ̨����','ODS_SC_SCRD_ORD_INFO','1','month',255,'0','','','BASS2','ODS_SC_SCRD_ORD_INFO_YYYYMM','TBS_3H','TBS_INDEX','ORD_SEQ,MOB_NUM',1);
insert into app.sch_control_task values ('TR1_L_11222',1,2,'ODS_SC_SCRD_ORD_INFO_YYYYMM',0,-1,'ͳһ����ƽ̨����','-','TR1_BOSS',2,'-');

select ITEM_TYPE , count(0) 
--,  count(distinct ITEM_TYPE ) 
from bass2.ODS_SC_SCRD_ORD_INFO_201104 
group by  ITEM_TYPE 
order by 1 

01	17

select type1 , count(0) 
--,  count(distinct type1 ) 
from bass2.ODS_SC_SCRD_ORD_INFO_201104 
group by  type1 
order by 1 
00	12
ʵ����	5

�ܲ������̳ǵĻ��ֻ�����ʽ����ջ����̳�һ��BOSS��Ŧ��ʡBOSS�Ľӿڹ淶��һ��BOSS��Ŧϵͳ�ӿڹ淶 ����ͳһ����ƽ̨���б�����4.2�½ڡ���Ʒȫ�����½ӿڡ����ݡ�
��4.2.1��Ʒȫ�����½ӿڡ�Ϊ��Ʒ��Ϣ�ӿڣ�����ƽ̨ÿ��Ὣȫ����Ʒ��Ϣͬ����ʡBOSS��
���С�ItemType���ֶ�Ϊ��Ʒ���࣬����ȡֵ�ǡ�01��ȫ����02��ʡ�ڡ�����Type1���ֶ�Ϊ��Ʒ��𣬾���ȡֵ�ǡ�00��ʵ����

				 mob_num 
				,ITEM_TYPE
				,case 
					when type1 = 'ʵ����' then '00'
					when type1 = '������' then '01'
					when type1 = '������' then '02'
					else type1 end type1 
				,sum(value(ORDER_SUM_POINT,0)) ORDER_SUM_POINT
				,count(distinct ORD_SEQ)  cnt
	 from bass2.ODS_SC_SCRD_ORD_INFO_201104 a
	 			  group by 
				 mob_num 
				,ITEM_TYPE
				,case 
					when type1 = 'ʵ����' then '00'
					when type1 = '������' then '01'
					when type1 = '������' then '02'
					else type1 end 
				 mob_num 
				,ITEM_TYPE
				,case 
					when type1 = 'ʵ����' then '00'
					when type1 = '������' then '01'
					when type1 = '������' then '02'
					else type1 end type1 
				,sum(value(ORDER_SUM_POINT,0)) ORDER_SUM_POINT
				,count(distinct ORD_SEQ)  cnt
	 from bass2.ODS_SC_SCRD_ORD_INFO_201104 a
	 			  group by 
				 mob_num 
				,ITEM_TYPE
				,case 
					when type1 = 'ʵ����' then '00'
					when type1 = '������' then '01'
					when type1 = '������' then '02'
					else type1 end 
	  a.feedback_id,
	  b.user_id,
	  sum(a.used_point),
	  sum(a.feedback_cnt)
	from (                   
					select mob_num,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' feedback_id
					,sum(ORDER_SUM_POINT)  used_point
					,sum(cnt) feedback_cnt 
						from 
						(
						select
										 mob_num 
										,ITEM_TYPE
										,case 
											when type1 = 'ʵ����' then '00'
											when type1 = '������' then '01'
											when type1 = '������' then '02'
											else type1 end type1 
										,sum(value(ORDER_SUM_POINT,0)) ORDER_SUM_POINT
										,count(distinct ORD_SEQ)  cnt
							 from bass2.ODS_SC_SCRD_ORD_INFO_201104 a
							 			  group by 
										 mob_num 
										,ITEM_TYPE
										,case 
											when type1 = 'ʵ����' then '00'
											when type1 = '������' then '01'
											when type1 = '������' then '02'
											else type1 end 
						) t
						group by                    
						mob_num,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00'
						) a, 
	 bass2.dw_product_201104 b
	 where a.mob_num = b.product_no
	    and b.userstatus_id in (1,2,3,6,8) 
		  and b.usertype_id in (1,2,9) 
		  and b.test_mark=0
	  b.user_id         
	  a.feedback_id,
	  b.user_id,
	  sum(a.used_point),
	  sum(a.feedback_cnt)
	from (                   
					select mob_num,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' feedback_id
					,sum(ORDER_SUM_POINT)  used_point
					,sum(cnt) feedback_cnt 
						from 
						(
						select
										 mob_num 
										,ITEM_TYPE
										,case 
											when type1 = 'ʵ����' then '00'
											when type1 = '������' then '01'
											when type1 = '������' then '02'
											else type1 end type1 
										,sum(value(ORDER_SUM_POINT,0)) ORDER_SUM_POINT
										,count(distinct ORD_SEQ)  cnt
							 from bass2.ODS_SC_SCRD_ORD_INFO_201104 a
							 			where substr(OP_TIME,1,6) = '201104'
							 			  group by 
										 mob_num 
										,ITEM_TYPE
										,case 
											when type1 = 'ʵ����' then '00'
											when type1 = '������' then '01'
											when type1 = '������' then '02'
											else type1 end 
						) t
						group by                    
						mob_num,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00'
						) a, 
	 bass2.dw_product_201104 b
	 where a.mob_num = b.product_no
	    and b.userstatus_id in (1,2,3,6,8) 
		  and b.usertype_id in (1,2,9) 
		  and b.test_mark=0
         
group by 	  a.feedback_id,
	  b.user_id
	

select * from channel.CHANNEL_LOCAL_BUSI @dbl_ggdb 

select 
	  a.feedback_id,
	  b.user_id,
	  sum(a.used_point),
	  sum(a.feedback_cnt)
	from (                   
					select mob_num,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' feedback_id
					,sum(ORDER_SUM_POINT)  used_point
					,sum(cnt) feedback_cnt 
						from 
						(
						select
										 mob_num 
										,ITEM_TYPE
										,case 
											when type1 = 'ʵ����' then '00'
											when type1 = '������' then '01'
											when type1 = '������' then '02'
											else type1 end type1 
										,sum(value(ORDER_SUM_POINT,0)) ORDER_SUM_POINT
										,count(distinct case when value(ORDER_SUM_POINT,0) > 0 then  ORD_SEQ||mob_num  end )  cnt
							 from bass2.ODS_SC_SCRD_ORD_INFO_201104 a
							 			where substr(OP_TIME,1,6) = '201104'
							 			  group by 
										 mob_num 
										,ITEM_TYPE
										,case 
											when type1 = 'ʵ����' then '00'
											when type1 = '������' then '01'
											when type1 = '������' then '02'
											else type1 end 
										having sum(value(ORDER_SUM_POINT,0)) > 0
						) t
						group by                    
						mob_num,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00'
						) a, 
	 bass2.dw_product_201104 b
	 where a.mob_num = b.product_no
	    and b.userstatus_id in (1,2,3,6,8) 
		  and b.usertype_id in (1,2,9) 
		  and b.test_mark=0
group by 	  a.feedback_id,
	  b.user_id
	
  
                  "TIME_ID" INTEGER NOT NULL , 
                  "POINT_FEEDBACK_ID" CHAR(4) NOT NULL , 
                  "USER_ID" CHAR(20) NOT NULL , 
                  "USED_POINT" CHAR(8) NOT NULL , 
                  "T_USED_POINT" CHAR(8) NOT NULL , 
                  "TONE_USED_POINT" CHAR(8) NOT NULL , 
                  "TTWO_USED_POINT" CHAR(8) NOT NULL , 
                  "TTHREE_USED_POINT" CHAR(8) NOT NULL , 
                  "USED_COUNT" CHAR(8) NOT NULL )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "USER_ID")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 


insert into "BASS1   "."G_S_02007_MONTH_B20110504" 

--,  count(distinct POINT_FEEDBACK_ID ) 
from G_S_02007_MONTH 
group by  POINT_FEEDBACK_ID 
order by 1 

where time_id= in (201101,201102,201102)
and return_flag=1
          a.feedback_id,
          b.user_id,
          sum(a.used_point),
          count(distinct mob_num||feedback_id) feedback_cnt
        from (                   
                                        select mob_num,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' feedback_id
                                        ,sum(ORDER_SUM_POINT)  used_point
                                                from 
                                                (
                                                select
                                                                                 mob_num 
                                                                                ,ITEM_TYPE
                                                                                ,case 
                                                                                        when type1 = '����??����' then '00'
                                                                                        when type1 = '��?��D����' then '01'
                                                                                        when type1 = 'o?���¨���' then '02'
                                                                                        else type1 end type1 
                                                                                ,sum(value(ORDER_SUM_POINT,0)) ORDER_SUM_POINT
                                                         from bass2.ODS_SC_SCRD_ORD_INFO_201104 a
                                                                                where substr(OP_TIME,1,6) = '201104'
                                                                                  group by 
                                                                                 mob_num 
                                                                                ,ITEM_TYPE
                                                                                ,case 
                                                                                        when type1 = '����??����' then '00'
                                                                                        when type1 = '��?��D����' then '01'
                                                                                        when type1 = 'o?���¨���' then '02'
                                                                                        else type1 end 
                                                                                having sum(value(ORDER_SUM_POINT,0)) > 0
                                                ) t
                                                group by                    
                                                mob_num,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00'
                                                ) a, 
         bass2.dw_product_201104 b
         where a.mob_num = b.product_no
            and b.userstatus_id in (1,2,3,6,8) 
                  and b.usertype_id in (1,2,9) 
                  and b.test_mark=0
group by          a.feedback_id,
          b.user_id
        
type1,count(0)
							 from bass2.ODS_SC_SCRD_ORD_INFO_201104 a
							 			where substr(OP_TIME,1,6) = '201104'
							 			  group by 
type1

201012	6421
201101	1880
201102	816
201103	1086
201104	1128

201012	6421
201101	2144
201102	983
201103	1259
201104	1128

where time_id in (201101,201102,201103)
and return_flag=1
--,  count(distinct time_id ) 
from G_S_02007_MONTH 
group by  time_id 
order by 1 
                        where entity_type in(72,73 ) and rec_status=1
            CHANNEL_ID
                                                ,'1'
                                                ,0
                                                ,0
                                                ,0
                                                ,count(0)
                                                ,sum(value(card_value,0)/100)
                                                ,0
                                                ,0
                                                ,0
                                                ,0
                                                ,0
                                                ,0
                                                ,0
                        from  BASS2.dw_channel_local_busi_201104
                        where entity_type in(72,73 ) and rec_status=1
                        group by CHANNEL_ID
            CHANNEL_ID
                                                ,'1'
                                                ,0
                                                ,0
                                                ,0
                                                ,count(0)
                                                ,sum(value(card_value,0)/100)
                                                ,0
                                                ,0
                                                ,0
                                                ,0
                                                ,0
                                                ,0
                                                ,0
                        from  BASS2.dw_channel_local_busi_201104
                        where entity_type in(72,73 ) and rec_status=1
                        and substr(char(date(done_date)),1,7) = '2011-04'
                        group by CHANNEL_ID
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


SELECT * FROM bass1.MON_ALL_INTERFACE  t
WHERE t.INTERFACE_CODE not IN (
select substr(a.control_code,15,5) from   app.sch_control_runlog  A
where control_code like 'BASS1_EXP%MONTH'
AND date(a.begintime) < '2011-05-01'
AND FLAG = 0
)
AND TYPE='m'

	                from bass2.STAT_ZD_VILLAGE_USERS_201104
                      where month_new_mark = 1
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
    ,app.sch_control_runlog c 
	where a.INTERFACE_CODE = substr(b.control_code , 11,5)
    and b.control_code = c.control_code 
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
        and a.INTERFACE_CODE = '22081'
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
    ,app.sch_control_runlog c 
	where a.INTERFACE_CODE = substr(b.control_code , 11,5)
    and b.control_code = c.control_code 
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
        and a.INTERFACE_CODE = '05001'
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
    ,app.sch_control_runlog c 
	where a.INTERFACE_CODE = substr(b.control_code , 11,5)
    and b.control_code = c.control_code 
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��8��ǰ'
                           

(
 '22085'
,'22081'
,'22083'
)
select b.CONTROL_CODE from    
(
 '22085'
,'22081'
,'22083'
)
from 
(
select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
from APP.G_FILE_REPORT a
where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
and err_code='00'
and length(filename)=length('s_13100_201002_03014_01_001.dat')
) t where rn = 1
            
update app.sch_control_task  a
set time_value = 510
where a.control_code = 'BASS1_G_A_02052_MONTH.tcl'
and       time_value = 512



select time_id , count(0) 
--,  count(distinct time_id ) 
from bass1.g_a_02004_day 
group by  time_id 
order by 1 

select time_id , count(0) 
--,  count(distinct time_id ) 
from bass1.g_a_02008_day 
group by  time_id 
order by 1 

	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��8��ǰ'
	union all
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��8��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)	
	union all 
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��8��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)	
	
	select *
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
    ,app.sch_control_runlog c 
	where a.INTERFACE_CODE = substr(b.control_code , 11,5)
    and b.control_code = c.control_code 
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��8��ǰ'
        
    
set flag = -2 
where control_code in 
(
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��3��ǰ'
)
set flag = -2 
where control_code in 
(
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��8��ǰ'
'22040'
,'22085'
,'02017'
,'03018'
,'22305'
,'03017'
,'22072'
,'21020'
,'21011'
,'22304'
,'22401'
,'21012'
,'22303'
,'22036'
,'03012'
,'22306'
,'02007'
,'02006'
,'21006'
,'21008'
,'22204'
,'03015'
,'22307'
,'03016')
)

set flag = -2 
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
		(
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��8��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)
		and flag = 0
		and date(endtime) < current date
		and month(endtime)  = month(current timestamp)
)

set flag = -2 
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��8��ǰ'
'22040'
,'22085'
,'02017'
,'03018'
,'22305'
,'03017'
,'22072'
,'21020'
,'21011'
,'22304'
,'22401'
,'21012'
,'22303'
,'22036'
,'03012'
,'22306'
,'02007'
,'02006'
,'21006'
,'21008'
,'22204'
,'03015'
,'22307'
,'03016')
						    )
						    and control_code like 'BASS1%EXP%'
						)
		and flag = 0
		and date(endtime) <= current date 
		and month(endtime)  = month(current timestamp)        
)
and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')



select upload_time , count(0) 
--,  count(distinct upload_time ) 
from bass1.MON_ALL_INTERFACE 
group by  upload_time 
order by 1 
ÿ��8��ǰ
ÿ��5��ǰ
ÿ��3��ǰ
ÿ��15��ǰ
ÿ��10��ǰ

ÿ��15��ǰ
ÿ��13��ǰ
ÿ��11��ǰ

from 
(
select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
from APP.G_FILE_REPORT a
where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
and err_code='00'
and length(filename)=length('s_13100_201002_03014_01_001.dat')
) t where rn = 1)
set deadline = (
select 
case 
when UPLOAD_TIME = 'ÿ��9��ǰ' then 9
when UPLOAD_TIME = 'ÿ��11��ǰ' then 11
when UPLOAD_TIME = 'ÿ��13��ǰ' then 13
when UPLOAD_TIME = 'ÿ��15��ǰ' then 15
when UPLOAD_TIME = 'ÿ��3��ǰ' then 3
when UPLOAD_TIME = 'ÿ��5��ǰ' then 5
when UPLOAD_TIME = 'ÿ��8��ǰ' then 8
when UPLOAD_TIME = 'ÿ��10��ǰ' then 10
when UPLOAD_TIME = 'ÿ��11��ǰ' then 11
when UPLOAD_TIME = 'ÿ��15��ǰ' then 15
end deadline
from  bass1.MON_ALL_INTERFACE b where a.INTERFACE_CODE = b.INTERFACE_CODE and a.type = b.type) 

where time_id in (201103)
and return_flag=1
--,  count(distinct BUSI_BILLING_TYPE ) 
from G_S_22083_MONTH 
group by  BUSI_BILLING_TYPE 
order by 1 

select bill_flag , count(0) 
--,  count(distinct bill_flag ) 
from bass1.G_S_22083_MONTH_2 
group by  bill_flag 
order by 1 

                             ,'201104' op_time
                             ,a.BUSI_CODE
                             ,c.OPERATOR_NAME BUSI_NAME
                             ,d.SP_NAME BUSI_PROVIDER_NAME
                             ,case when b.bill_flag = 3 and DELAY_TIME = 72 then '11' 
                                        when b.bill_flag = 3 and b.DELAY_TIME = 0 then '12'
                                                                        else '20' end  BUSI_BILLING_TYPE                             , b.BILL_FLAG f1,c.BILL_FLAG f2
                             ,char(sum(a.CANCEL_CNT))   CANCEL_CNT 
                             ,char(sum(a.COMPLAINT_CNT)) COMPLAINT_CNT   
                             ,char(sum(value(e.ORDER_CNT,0))) ORDER_CNT      
                        			from   bass1.G_S_22083_MONTH_1 a 
			       join  bass1.G_S_22083_MONTH_2 b  on a.SP_ID = b.SP_ID and a.BUSI_CODE = b.BUSI_CODE
			       join bass2.DIM_PM_SP_OPERATOR_CODE c on  a.SP_ID = char(c.SP_CODE) and a.BUSI_CODE = c.OPERATOR_CODE
				JOIN BASS1.G_S_22083_MONTH_3 D ON a.sp_id = d.SP_CODE
				left join BASS1.G_S_22083_MONTH_4 e on a.sp_id = e.SP_ID and a.BUSI_CODE = b.BUSI_CODE
			group by 
                     a.BUSI_CODE
                     ,c.OPERATOR_NAME 
                     ,d.SP_NAME
                     ,case when b.bill_flag = 3 and DELAY_TIME = 72 then '11' 
                                when b.bill_flag = 3 and b.DELAY_TIME = 0 then '12'
                                                                else '20' end      
select * from app.sch_control_before where control_code = 'BASS1_G_S_22302_DAY.tcl'

SELECT * FROM app.sch_control_RUNLOG
WHERE control_code IN (
select BEFORE_CONTROL_CODE  from app.sch_control_before where control_code = 'BASS1_G_S_22302_DAY.tcl'
)

BASS2_Dw_enterprise_industry_apply.tcl



SELECT * FROM app.sch_control_RUNLOG
WHERE control_code IN (
select BEFORE_CONTROL_CODE  from app.sch_control_before where 
control_code like 'BASS1_G_I_02018_MONTH.tcl'
)



select * from    app.sch_control_before
where control_code like '%02018%'


select *  from app.sch_control_runlog where control_code = 'BASS1_G_I_02018_MONTH.tcl'


select time_id,count(0)
from BASS1.G_S_22302_DAY
group by time_id 
order by 1 desc 


alter table BASS1.G_S_04003_DAY ALTER column FLOWUP 	SET DATA TYPE CHARACTER(13)
alter table BASS1.G_S_04003_DAY ALTER column FLOWDOWN  SET DATA TYPE CHARACTER(13)

FLOWUP                         SYSIBM    CHARACTER                13     0 No    
FLOWDOWN                       SYSIBM    CHARACTER                13     0 No   



select time_id , count(0) from   BASS1.G_USER_LST
group by time_id 





select ent_busi_type from   BASS1.G_S_22302_DAY where time_id = 20110501
except   
select ent_busi_type from   BASS1.G_S_22302_DAY where time_id = 20110430




select ent_busi_type,count(0)
from BASS1.G_S_22302_DAY
where time_id = 20110501
group by ent_busi_type 
order by 1 desc 




select ent_busi_type,count(0)
from BASS1.G_S_22302_DAY
where time_id = 20110430
group by ent_busi_type 
order by 1 desc 






select * from app.g_unit_info
where unit_code in ('02022','02023','22080','22082','22084')

update app.g_unit_info
set table_name = lower(table_name)
where unit_code in ('22081','22083','2208')





select * from   g_s_04003_day where time_id = 20110502


select over_prod_id , bigint(over_prod_id) from bass1.g_i_02019_month b  where  b.time_id = 201104




CREATE TABLE "BASS1   "."G_S_04003_DAY_B20110504"  (
                  "TIME_ID" INTEGER NOT NULL , 
                  "PRODUCT_NO" CHAR(15) NOT NULL , 
                  "IMSI" CHAR(15) NOT NULL , 
                  "HOME_LOCN" CHAR(6) NOT NULL , 
                  "ROAM_LOCN" CHAR(6) NOT NULL , 
                  "USER_TYPE" CHAR(1) NOT NULL , 
                  "ROAM_TYPE_ID" CHAR(3) NOT NULL , 
                  "START_DATE" CHAR(8) NOT NULL , 
                  "START_TIME" CHAR(6) NOT NULL , 
                  "END_DATE" CHAR(8) NOT NULL , 
                  "END_TIME" CHAR(6) NOT NULL , 
                  "CALL_DURATION" CHAR(6) NOT NULL , 
                  "FLOWUP" CHAR(13) NOT NULL , 
                  "FLOWDOWN" CHAR(13) NOT NULL , 
                  "SVCITEM_ID" CHAR(2) NOT NULL , 
                  "SERVICE_CODE" CHAR(2) NOT NULL , 
                  "WLAN_ATTESTATION_CODE" CHAR(2) NOT NULL , 
                  "HOTSPOT_AREA_ID" CHAR(16) NOT NULL , 
                  "AS_IP" CHAR(8) NOT NULL , 
                  "ATTESTATION_AS_IP" CHAR(8) NOT NULL , 
                  "CALL_FEE" CHAR(6) NOT NULL , 
                  "INFO_FEE" CHAR(6) NOT NULL , 
                  "SERVICE_ID" CHAR(8) NOT NULL , 
                  "ISP_ID" CHAR(6) NOT NULL , 
                  "BELONG_OPER_ID" CHAR(5) NOT NULL , 
                  "ROAM_OPER_ID" CHAR(5) NOT NULL , 
                  "REASON_OF_STOP_CODE" CHAR(2) NOT NULL )   
                 DISTRIBUTE BY HASH("TIME_ID",  
                 "PRODUCT_NO")   
                   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY ; 



insert into G_S_04003_DAY_B20110504
select * from G_S_04003_DAY

select count(0) from    G_S_04003_DAY_B20110504
select count(0) from    G_S_04003_DAY


select * from    G_S_04003_DAY
where time_id = 20110503





select count(0) from    G_S_02007_MONTH
where time_id = 201104


select POINT_FEEDBACK_ID , count(0) 
--,  count(distinct POINT_FEEDBACK_ID ) 
from G_S_02007_MONTH 
group by  POINT_FEEDBACK_ID 
order by 1 


select count(0) from    G_S_02007_MONTH
where 
time_id = ''
POINT_FEEDBACK_ID
not in 
('2210','2220','2230','2240','2250'
,'1100','1200','1300','2100','2100')


                select count(0) 
                from    G_S_02007_MONTH
                where 
                time_id = 201104
                and POINT_FEEDBACK_ID   not in ('2210','2220','2230','2240','2250'
                                                                ,'1100','1200','1300','2100','2100')
                                                                
                                                                

	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(b.control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��10��ǰ'
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��10��ǰ'
	union all
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��10��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)	
	union all 
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��10��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)	
	

set flag = -2 
where control_code in 
(
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��10��ǰ'
)
and  flag = 0 

set flag = -2 
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
		(
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��8��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)
		and flag = 0
		and date(endtime) < current date
		and month(endtime)  = month(current timestamp)
)

set flag = -2 
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
		(
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��10��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)
		and flag = 0
		and date(endtime) < current date
		and month(endtime)  = month(current timestamp)
)

set flag = -2 
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��10��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)
		and flag = 0
		and date(endtime) <= current date 
		and month(endtime)  = month(current timestamp)
)
and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')

                                                                                                                  


where type = 'm'
and deadline = 10
and  interface_code  in 
(select substr(filename,16,5)
from 
(
select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
from APP.G_FILE_REPORT a
where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
and err_code='00'
and length(filename)=length('s_13100_201002_03014_01_001.dat')
) t where rn = 1)

--,  count(distinct time_id ) 
from G_S_21001_DAY 
group by  time_id 
order by 1  desc 

         time_id,
         case when rule_code='R159_1' then '�����ͻ���'
              when rule_code='R159_2' then '�ͻ�������'
              when rule_code='R159_3' then '�������ͻ���'
              when rule_code='R159_4' then '�����ͻ���'
         end,
         target1,
         target2,
         target3
from bass1.g_rule_check
where 
    rule_code in ('R159_1','R159_2','R159_3','R159_4')
    and time_id between 20110401 and 20110506





















1166203	110917

1262590	122586

1281704	119684

--,  count(distinct time_id ) 
from G_S_21001_DAY 
group by  time_id 
order by 1  desc 
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��15��ǰ'
	union all
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��15��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)	
	union all 
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��15��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)	
	
set flag = -2 
--select * from  app.sch_control_runlog 
(
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��15��ǰ'
)
and  flag = 0     
set flag = -2 
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
		(
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��15��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)
		and flag = 0
		and date(endtime) < current date
		and month(endtime)  = month(current timestamp)
)
where channel_id not in
(select distinct channel_id from bass1.g_s_22062_month where time_id =201104)
  and time_id =201104
  and channel_status='1'
select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.G_S_22062_MONTH_TEST where time_id =201103)
  and time_id =201103
  and channel_status='1'

where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month_test where time_id =201103)
  and time_id =201103
  and channel_type in ('2','3')
  and channel_status='1'
where time_id =201104
  and channel_status='1'
  and channel_type='1'
  and (longitude='0' or longitude='' or latitude='0' or latitude='')

where channel_id not in
(select distinct channel_id from bass1.g_s_22062_month where time_id =201104)
  and time_id =201104
  and channel_status='1'
                        where channel_id not in
                        (select distinct channel_id from bass1.g_s_22062_month where time_id =201103 )
                          and time_id =201103
                          and channel_status='1'
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month where time_id =201103)
  and time_id =201103
  and channel_type in ('2','3')
  and channel_status='1'
set flag = -2 
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��15��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)
		and flag = 0
		and date(endtime) <= current date 
		and month(endtime)  = month(current timestamp)
)
and control_code not in ('BASS1_EXP_G_S_05001_MONTH','BASS1_EXP_G_S_05002_MONTH')


									where a.control_code like 'BASS1%EXP%DAY%' 
									and date(a.begintime) =  date(current date) 
									and flag = 0 with ur 
RETURNS
TABLE (control_code varchar(128),before_control_code varchar(128))
RETURN
select control_code,before_control_code from app.sch_control_before where control_code = p_control_code
RETURNS
TABLE (control_code varchar(128),before_control_code varchar(128))
RETURN
select control_code,before_control_code from app.sch_control_before where before_control_code = p_before_control_code
select * from  table( bass1.get_after('BASS1_G_I_03007_MONTH.tcl')) a 

select * from  bass1.T_GS05001M where time_id = 201104


insert into bass1.g_s_05002_month
select * from  bass1.T_GS05002M where time_id = 201104
select * from  bass1.T_GS05001M where time_id = 201104


insert into bass1.g_s_05002_month
select * from  bass1.T_GS05002M where time_id = 201104
from   bass1.g_s_05001_month 
group by  time_id 
order by 1 desc 

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05002_month 
group by time_id 
order by 1 desc 


where
(
   control_code like 'BASS1%22080%'
or control_code like 'BASS1%22082%'
or control_code like 'BASS1%22084%'
or control_code like 'BASS1%22085%'
or control_code like 'BASS1%22081%'
or control_code like 'BASS1%02023%'
or control_code like 'BASS1%22083%'
or control_code like 'BASS1%02022%'
)
and function_desc not like '%����%'
and cc_flag = 1 
drop table T_INT_CHECK_USER_STATUS1003

drop table T_GPRS_SUM2
drop table T_GPRS_SUM
drop table T_GPRS_PROD_USER2
drop table T_GPRS_PROD_USER

drop table T_BASE_BILL_DURATION
drop table T_BASS1_STD_0113

drop table T_GRP_ID_OLD_NEW_MAP
drop table G_I_77780_DAY_DOWN20110415
drop view T_V_06021

drop table G_I_77780_DAY_DOWN20110422
drop table G_I_77780_DAY_DOWN20110414
drop table G_A_02055_DAY_DOWN20110321
drop table G_A_02059_DAY_DOWN20110321

drop table G_S_03018_MONTH_B20110414
drop table G_S_03017_MONTH_B20110414

drop table G_I_77780_DAY_MID5
drop table G_I_77780_DAY_MID4
drop table G_I_77780_DAY_MID_A2
drop table G_I_77780_DAY_MID_A

drop table G_I_77780_DAY_MID2
drop table G_I_77780_DAY_B20110421

 		sum(bigint(flowup)+bigint(flowdown))*1.00/1024/1024/1024 wlan_flow
 		,count(distinct product_no) user_cnt
        from   bass1.G_S_04003_DAY
		where time_id between  20110401 and 20110430
MON_INTERFACE_NOT_EMPTY
MON_USER_MOBILE
MONIT_SQL
MONITOR_CONFIG
MONITOR_LIST
MONTH_02006_MID1
MONTH_02006_MID2

('DIM_21003_IP_TYPE'
,'G_A_01002_DAY'
,'G_A_01004_DAY'
,'G_A_01004_TMP1'
,'G_A_01004_TMP10'
,'G_A_01004_TMP11'
,'G_A_01004_TMP12'
,'G_A_01004_TMP2'
,'G_A_01004_TMP3'
,'G_A_01004_TMP4'
,'G_A_01005_MONTH'
,'G_A_01006_DAY'
,'G_A_01007_DAY'
,'G_A_02004_DAY'
,'G_A_02004_DAY_create'
,'G_A_02004_DAY_CREATE'
,'G_A_02004_DAY_CREATE_2'
,'G_A_02004_DAY_create_2'
,'G_A_02004_DAY_T'
,'G_A_02004_DAY_TAKE'
,'G_A_02008_DAY'
,'G_A_02008_DAY_T'
,'G_A_02008_DAY_TAKE'
,'G_A_02011_DAY'
,'G_A_02013_DAY'
,'G_A_02052_MONTH'
,'G_A_02053_DAY'
,'G_A_02054_DAY'
,'G_A_02054_DAY_0317_1220REPAIR'
,'G_A_02054_DAY_BLACK'
,'G_A_02055_DAY'
,'G_A_02056_DAY'
,'G_A_02057_DAY'
,'G_A_02058_DAY'
,'G_A_02059_DAY'
,'G_A_02059_DAY_0315MODIFY'
,'G_A_02059_DAY_20110321FIX1340'
,'G_A_02060_DAY'
,'G_A_02061_DAY'
,'G_A_02061_DAY_0317REPAIR'
,'G_A_02062_DAY'
,'G_A_02062_DAY_20110317REPAIR'
,'G_A_02064_DAY'
,'G_A_06001_DAY'
,'G_I_01006_MONTH'
,'G_I_02005_MONTH'
,'G_I_02006_MONTH'
,'G_I_02014_MONTH'
,'G_I_02015_MONTH'
,'G_I_02016_MONTH'
,'G_I_02017_MONTH'
,'G_I_02018_MONTH'
,'G_I_02019_MONTH'
,'G_I_02019_MONTH_1'
,'G_I_02019_MONTH_2'
,'G_I_02019_MONTH_4'
,'G_I_02020_MONTH'
,'G_I_02021_MONTH'
,'G_I_02021_MONTH_TEMP1'
,'G_I_02021_MONTH_TEMP2'
,'G_I_02022_DAY'
,'G_I_02022_MONTH'
,'G_I_02023_DAY'
,'G_I_02023_DAY_1'
,'G_I_02023_MONTH'
,'G_I_02049_MONTH'
,'G_I_02053_MONTH'
,'G_I_02063_DAY'
,'G_I_03001_MONTH'
,'G_I_03002_MONTH'
,'G_I_03003_MONTH'
,'G_I_03007_MONTH'
,'G_I_06001_MONTH'
,'G_I_06002_MONTH'
,'G_I_06011_MONTH'
,'G_I_06012_MONTH'
,'G_I_06021_MONTH'
,'G_I_06022_MONTH'
,'G_I_06023_MONTH'
,'G_I_06029_MONTH'
,'G_I_06031_DAY'
,'G_I_06032_DAY'
,'G_I_21020_MONTH'
,'G_S_02007_DAY'
,'G_S_02007_MONTH'
,'G_S_02047_MONTH'
,'G_S_03002_MONTH'
,'G_S_03002_MONTH_TYM'
,'G_S_03004_MONTH'
,'G_S_03004_MONTH_TD'
,'G_S_03005_MONTH'
,'G_S_03012_MONTH'
,'G_S_03015_MONTH'
,'G_S_03016_MONTH'
,'G_S_03016_MONTH_LS'
,'G_S_03017_MONTH'
,'G_S_03018_MONTH'
,'G_S_04002_DAY'
,'G_S_04002_DAY_FLOWS'
,'G_S_04002_DAY_TD'
,'G_S_04002_DAY_TD1'
,'G_S_04002_DAY_TD2'
,'G_S_04002_DAY_TMP'
,'G_S_04003_DAY'
,'G_S_04004_DAY'
,'G_S_04005_DAY'
,'G_S_04006_DAY'
,'G_S_04007_DAY'
,'G_S_04008_DAY'
,'G_S_04009_DAY'
,'G_S_04010_DAY'
,'G_S_04011_DAY'
,'G_S_04012_DAY'
,'G_S_04014_DAY'
,'G_S_04015_DAY'
,'G_S_04016_DAY'
,'G_S_04017_DAY'
,'G_S_04017_DAY_TD'
,'G_S_04018_DAY'
,'G_S_04018_DAY_FLOWS'
,'G_S_05001_MONTH'
,'G_S_05002_MONTH'
,'G_S_05003_MONTH'
,'G_S_21001_DAY'
,'G_S_21002_DAY'
,'G_S_21003_MONTH'
,'G_S_21003_MONTH_TD'
,'G_S_21003_MONTH_TMP'
,'G_S_21003_TO_DAY'
,'G_S_21004_DAY'
,'G_S_21005_DAY'
,'G_S_21006_MONTH'
,'G_S_21006_TO_DAY'
,'G_S_21007_DAY'
,'G_S_21008_MONTH'
,'G_S_21008_TO_DAY'
,'G_S_21009_DAY'
,'G_S_21010_MONTH'
,'G_S_21011_MONTH'
,'G_S_21012_MONTH'
,'G_S_21013_MONTH'
,'G_S_21014_MONTH'
,'G_S_21015_MONTH'
,'G_S_21016_DAY'
,'G_S_21020_MONTH'
,'G_S_22009_MONTH'
,'G_S_22012_DAY'
,'G_S_22013_MONTH'
,'G_S_22021_MONTH'
,'G_S_22025_MONTH'
,'G_S_22032_MONTH'
,'G_S_22033_MONTH'
,'G_S_22036_MONTH'
,'G_S_22038_DAY'
,'G_S_22039_MONTH'
,'G_S_22040_MONTH'
,'G_S_22041_MONTH'
,'G_S_22042_MONTH'
,'G_S_22043_MONTH'
,'G_S_22049_MONTH'
,'G_S_22050_MONTH'
,'G_S_22052_MONTH'
,'G_S_22055_MONTH'
,'G_S_22056_MONTH'
,'G_S_22061_MONTH'
,'G_S_22062_MONTH'
,'G_S_22063_MONTH'
,'G_S_22064_MONTH'
,'G_S_22065_MONTH'
,'G_S_22072_MONTH'
,'G_S_22073_DAY'
,'G_S_22073_DAY_TEST'
,'G_S_22080_DAY'
,'G_S_22081_MONTH'
,'G_S_22081_MONTH_1'
,'G_S_22081_MONTH_2'
,'G_S_22082_DAY'
,'G_S_22082_DAY_1'
,'G_S_22082_DAY_2'
,'G_S_22083_MONTH'
,'G_S_22083_MONTH_1'
,'G_S_22083_MONTH_2'
,'G_S_22083_MONTH_3'
,'G_S_22083_MONTH_4'
,'G_S_22084_DAY'
,'G_S_22085_MONTH'
,'G_S_22101_MONTH'
,'G_S_22102_DAY'
,'G_S_22103_MONTH'
,'G_S_22104_DAY'
,'G_S_22105_MONTH'
,'G_S_22106_MONTH'
,'G_S_22201_DAY'
,'G_S_22202_DAY'
,'G_S_22203_DAY'
,'G_S_22204_MONTH'
,'G_S_22204_MONTH_TMP3'
,'G_S_22301_DAY'
,'G_S_22302_DAY'
,'G_S_22303_MONTH'
,'G_S_22304_MONTH'
,'G_S_22305_MONTH'
,'G_S_22306_MONTH'
,'G_S_22307_MONTH'
,'G_S_22401_MONTH'
,'INT_02011_SNAPSHOT')
from syscat.tables a,syscat.tablespaces b where a.tbspace = b.tbspace
group by a.TABNAME having sum(decimal(float(a.npages)/(1024/(b.pagesize/1024)),9,2)) > 1000
--,  count(distinct time_id ) 
from G_S_04002_DAY 
group by  time_id 
order by 1 
	  select * from  BASS1.G_S_04002_DAY_BAK
	  where time_id >= 20101101
    create table BASS1.G_S_04002_DAY like BASS1.G_S_04002_DAY_BAK 
     DISTRIBUTE BY HASH(TIME_ID,  
                 PRODUCT_NO,  
                 ROAM_LOCN,  
                 ROAM_TYPE_ID,  
                 APNNI,  
                 START_TIME)   
     IN TBS_APP_BASS1 INDEX IN TBS_INDEX 
                   
	  insert into BASS1.G_S_04002_DAY 
	  select * from  BASS1.G_S_04002_DAY_BAK
	  where time_id >= 20110101
	  --about 15min
	 db2 RUNSTATS ON TABLE BASS1.G_S_04002_DAY	with distribution and detailed indexes all  
	  drop table BASS1.G_S_04002_DAY_BAK


3554	3552	3151




    rename BASS1.G_S_04005_DAY to G_S_04005_DAY_BAK
    create table BASS1.G_S_04005_DAY like BASS1.G_S_04005_DAY_BAK 
                 DISTRIBUTE BY HASH(TIME_ID,  
                 PRODUCT_NO,  
                 SP_CODE,  
                 OPPOSITE_NO)   
                   IN TBS_APP_BASS1 INDEX IN TBS_INDEX NOT LOGGED INITIALLY ; 
                   
	  insert into BASS1.G_S_04005_DAY 
	  select * from  BASS1.G_S_04005_DAY_BAK
	  where time_id >= 20110101
	  --about 10min

RUNSTATS ON TABLE BASS1.G_S_04005_DAY	with distribution and detailed indexes all  

select tabname , card from syscat.tables where tabname in ('G_S_04005_DAY','G_S_04005_DAY_BAK')            
AND tabschema = 'BASS1'

G_S_04005_DAY	159753255
G_S_04005_DAY_BAK	433834695

drop table BASS1.G_S_04005_DAY_BAK

--,  count(distinct time_id ) 
from G_S_21003_TO_DAY 
group by  time_id 
order by 1 


where month_new_mark = 1
CREATE TABLE "BASS1"."G_I_02053_MONTH_B20110512"
 ("TIME_ID"       INTEGER,
  "PRODUCT_NO"    CHARACTER(15),
  "PRODUCT_NO_3"  CHARACTER(15),
  "BUSI_TYPE"     CHARACTER(2),
  "SP_CODE"       CHARACTER(12),
  "SERV_CODE"     CHARACTER(20),
  "STS"           CHARACTER(1),
  "VALID_DATE"    CHARACTER(8),
  "EXPIRE_DATE"   CHARACTER(8),
  "BILL_FLAG"     CHARACTER(1),
  "REMARK"        CHARACTER(1),
  "APPLY_TYPE"    CHARACTER(2)
 )
  DATA CAPTURE NONE
 IN "TBS_APP_BASS1"
 INDEX IN "TBS_INDEX"
  PARTITIONING KEY
   (TIME_ID,
    PRODUCT_NO
   ) USING HASHING;

ALTER TABLE "BASS1"."G_I_02053_MONTH_B20110512"
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
                 left outer join bass2.dw_product_20110430 b
                  on a.user_id=b.user_id
                 inner join (select distinct USER_ID  from BASS2.DW_I_USER_RADIUS_ORDER_201104) c
                  on a.user_id=c.USER_ID
                where a.busi_type in ('115','119','130')
                 left outer join bass2.dw_product_20110430 b
                  on a.user_id=b.user_id
                 --inner join (select distinct USER_ID  from BASS2.DW_I_USER_RADIUS_ORDER_201104) c
                  --on a.user_id=c.USER_ID
                where a.busi_type in ('115','119','130')
                  on a.user_id=c.USER_ID                 
                 left outer join bass2.dw_product_20110430 b
                  on a.user_id=b.user_id
                where a.busi_type in ('115','119','130')
                 left outer join bass2.dw_product_20110430 b
                  on a.user_id=b.user_id
                where a.busi_type in ('115','119','130')
                select count(0) from g_i_02053_month
                  where time_id = 201104
                 and 
                 VALID_DATE> EXPIRE_DATE
                 with ur
CREATE TABLE "BASS1"."G_A_02053_DAY_B20110512"
 ("TIME_ID"       INTEGER,
  "PRODUCT_NO"    CHARACTER(15),
  "PRODUCT_NO_3"  CHARACTER(15),
  "BUSI_TYPE"     CHARACTER(2),
  "SP_CODE"       CHARACTER(12),
  "SERV_CODE"     CHARACTER(20),
  "STS"           CHARACTER(1),
  "VALID_DATE"    CHARACTER(8),
  "EXPIRE_DATE"   CHARACTER(8),
  "BILL_FLAG"     CHARACTER(1),
  "REMARK"        CHARACTER(1),
  "APPLY_TYPE"    CHARACTER(2)
 )
  DATA CAPTURE NONE
 IN "TBS_APP_BASS1"
 INDEX IN "TBS_INDEX"
  PARTITIONING KEY
   (TIME_ID,
    PRODUCT_NO
   ) USING HASHING;

ALTER TABLE "BASS1"."G_A_02053_DAY_B20110512"
  LOCKSIZE ROW
  APPEND OFF
  NOT VOLATILE;
                 
BASS1_G_A_02053_DAY.tcl	BASS2_Dwd_i_user_radius_order_ds.tcl

select * from  table( bass1.get_after('BASS1_G_I_03007_MONTH.tcl')) a 
BASS1_G_A_02053_DAY.tcl	BASS2_Dwd_product_regsp_ds.tcl
BASS1_G_A_02053_DAY.tcl	BASS2_Dwd_i_user_radius_order_ds.tcl
BASS1_G_I_02053_MONTH.tcl	BASS2_Dwd_product_regsp_ds.tcl
BASS1_G_I_02053_MONTH.tcl	BASS2_Dw_product_ds.tcl
BASS1_G_I_02053_MONTH.tcl	BASS2_Dwd_i_user_radius_order_ds.tcl

       substr(TBSP_NAME,1,18) TBSP_NAME,
       substr(TBSP_TYPE,1,4) TYPE,
       -- TBSP_TOTAL_SIZE_KB / 1024 / 1024 as total_size,
       TBSP_USED_SIZE_KB / 1024 / 1024 as used,
       TBSP_FREE_SIZE_KB / 1024 / 1024 as free,
       TBSP_UTILIZATION_PERCENT as percent,
       substr(TBSP_STATE,1,12) STATE,
       DBPARTITIONNUM as par
  from SYSIBMADM.TBSP_UTILIZATION
 order by TBSP_ID,DBPARTITIONNUM
RETURNS
TABLE (control_code varchar(128),FUNCTION_DESC           VARCHAR(200))
RETURN
select control_code,FUNCTION_DESC 
from app.sch_control_task 
where  locate(upper(p_control_code),upper(control_code)) > 0


)
                    from bass2.dw_product_20110429  
                   where usertype_id in (1,2,9) 
                     and day_off_mark = 1 
                     and userstatus_id not in (1,2,3,6,8)
                     and test_mark<>1
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
                                              where time_id<=20110517 ) e
                                        inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
                                                                       from bass1.g_a_02008_day
                                                                       where time_id<=20110517 ) f on f.user_id=e.user_id
                                        where e.row_id=1 and f.row_id=1

                        where usertype_id IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id=20110429
                          and test_flag='0'
                          and time_id=20110429                       
from bass2.dw_product_20110429  
where usertype_id in (1,2,9) 
 and day_off_mark = 1 
 and userstatus_id not in (1,2,3,6,8)
 and test_mark<>1
89360000853341 
89360000853343 
89360000853342 
89260000852773 

18289031824    
18289031854    
18289032624    
18889020019    

where usertype_id in (1,2,9) 
 and day_off_mark = 1 
 and userstatus_id not in (1,2,3,6,8)
 and test_mark<>1
20110426	20110426	2476      	1625771     	24109824    	436466      	3755052     	41773     	279559      	20110426	R159_4	41773.00000	41775.00000	-0.00004	0.00000





R161_1 	65
R161_10	66
R161_13	155
R161_14	189
R161_15	12
R161_16	33
R161_17	71
R161_2 	3
R161_3 	63
R161_4 	2
R161_5 	20
R161_6 	10
R161_7 	45
R161_9 	41

GID VARCHAR(20)
,DID VARCHAR(20)
) RETURNS 
VARCHAR(32) 
DETERMINISTIC 
NO EXTERNAL 
ACTION LANGUAGE SQL 
BEGIN ATOMIC 
		RETURN 
		SELECT 
			BASS1_VALUE 
		FROM BASS1.ALL_DIM_LKP 
		WHERE BASS1_TBID = GID 
			AND XZBAS_VALUE = DID 
END
SET SCHEMA BASS1;

SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","BASS1";

CREATE FUNCTION "BASS1"."FN_GET_ALL_DIM"
 ("GID" VARCHAR(20),
  "DID" VARCHAR(20)
 ) 
  RETURNS VARCHAR(32)
  LANGUAGE SQL
  DETERMINISTIC
  READS SQL DATA
  STATIC DISPATCH
  CALLED ON NULL INPUT
  NO EXTERNAL ACTION
  INHERIT SPECIAL REGISTERS
  BEGIN ATOMIC
    RETURN
      SELECT BASS1_VALUE
        FROM BASS1.ALL_DIM_LKP
        WHERE BASS1_TBID = GID
          AND XZBAS_VALUE = DID;
  END;

select count(0) from    G_A_02053_DAY
                                                                
select * from   bass2.Dw_product_ins_off_ins_prod_ds where offer_id like '%QW_QQT_DJ%'

select xzbas_value  as offer_id 
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0115'
                                              and bass1_value like 'QW_QQT_DJ%'
                                              
                        select 
                                USER_ID
                                ,ADD_PKG_ID
                                ,VALID_DT
                        FROM (
                                SELECT
                                 a.PRODUCT_INSTANCE_ID as USER_ID
                                ,bass1.fn_get_all_dim('BASS_STD1_0115',char(a.offer_id)) as ADD_PKG_ID
                                ,replace(char(date(a.VALID_DATE)),'-','') as VALID_DT 
                                ,row_number()over(partition by a.PRODUCT_INSTANCE_ID,a.offer_id 
                                        order by EXPIRE_DATE desc ,VALID_DATE desc  ) rn 
                                from  bass2.Dw_product_ins_off_ins_prod_ds a 
                                     ,(select xzbas_value  as offer_id 
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0115'
                                              and bass1_value like 'QW_QQT_DJ%'
                                      ) b 
                                where char(a.offer_id) = b.offer_id 
                                and a.state=1
                                and a.valid_type = 1 
                                 and a.OP_TIME = '2011-05-17'
                                 and date(a.VALID_DATE)<='2011-05-17'
                            ) AS T where t.rn = 1 
                         with ur 
                                                                
SELECT a.OFFER_ID
                                from  bass2.Dw_product_ins_off_ins_prod_ds a 
                                     ,(select xzbas_value  as offer_id 
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0115'
                                              and bass1_value like 'QW_QQT_DJ%'
                                      ) b 
                                where char(a.offer_id) = b.offer_id                                and a.state=1
                                and a.valid_type = 1 
                                 and a.OP_TIME = '2011-05-17'
                                 and date(a.VALID_DATE)<='2011-05-17'                                                                 
                                 
select  *
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0115'
                                              and bass1_value like 'QW_QQT_DJ%'                                 
                                              
select xzbas_value  as offer_id 
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0115'
                                              and bass1_value like 'QW_QQT_DJ%'
except
SELECT a.OFFER_ID
                                from  bass2.Dw_product_ins_off_ins_prod_ds a 
                                     ,(select xzbas_value  as offer_id 
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0115'
                                              and bass1_value like 'QW_QQT_DJ%'
                                      ) b 
                                where char(a.offer_id) = b.offer_id                                and a.state=1
                                and a.valid_type = 1 
                                 and a.OP_TIME = '2011-05-17'
                                 and date(a.VALID_DATE)<='2011-05-17'
                                from  bass2.Dw_product_ins_off_ins_prod_ds a 
                                     ,(select xzbas_value  as offer_id 
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0115'
                                              and bass1_value like 'QW_QQT_DJ%'
                                      ) b 
                                where char(a.offer_id) = b.offer_id  
                                from  bass2.Dw_product_ins_off_ins_prod_ds a 
                                     ,(select xzbas_value  as offer_id 
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0115'
                                              and bass1_value like 'QW_QQT_DJ%'
                                      ) b 
                                where char(a.offer_id) = b.offer_id                                and a.state=1
                                and a.valid_type = 1 
                                 and a.OP_TIME = '2011-05-17'
                                 and date(a.VALID_DATE)<='2011-05-17'                                 
					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0114'
					      and bass1_value like 'QW_QQT_JC%'
                20110511 TIME_ID
                ,char(a.product_instance_id)  USER_ID
                ,bass1.fn_get_all_dim('BASS_STD1_0114',char(a.offer_id)) BASE_PKG_ID
                ,replace(char(date(a.create_date)),'-','') VALID_DT
        from  bass2.ODS_PRODUCT_INS_PROD_20110517 a
        ,(select xzbas_value  as offer_id 
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0114'
                                              and bass1_value like 'QW_QQT_JC%'
                                      ) c
        where  char(a.offer_id) = c.offer_id 
				SELECT
				 a.PRODUCT_INSTANCE_ID as USER_ID
				,bass1.fn_get_all_dim_ex('BASS_STD1_0115',char(a.offer_id)) as ADD_PKG_ID
				,replace(char(date(a.VALID_DATE)),'-','') as VALID_DT 
				,row_number()over(partition by a.PRODUCT_INSTANCE_ID,a.offer_id 
					order by EXPIRE_DATE desc ,VALID_DATE desc  ) rn 
				from  bass2.Dw_product_ins_off_ins_prod_ds a 
				     ,(select xzbas_value  as offer_id 
					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0115'
					      and bass1_value like 'QW_QQT_DJ%'
				      ) b 
				where char(a.offer_id) = b.offer_id 
				and a.state=1
				and a.valid_type = 1 
				 and a.OP_TIME = '$op_time'
				 and date(a.VALID_DATE)<='$op_time'
			    ) AS T where t.rn = 1 
			 with ur 
				USER_ID
				,ADD_PKG_ID
				,VALID_DT
			FROM (
				SELECT
				 a.PRODUCT_INSTANCE_ID as USER_ID
				,bass1.fn_get_all_dim_ex('BASS_STD1_0115',char(a.offer_id)) as ADD_PKG_ID
				,replace(char(date(a.VALID_DATE)),'-','') as VALID_DT 
				,row_number()over(partition by a.PRODUCT_INSTANCE_ID,a.offer_id 
					order by EXPIRE_DATE desc ,VALID_DATE desc  ) rn 
				from  bass2.Dw_product_ins_off_ins_prod_ds a 
				     ,(select xzbas_value  as offer_id 
					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0115'
					      and bass1_value like 'QW_QQT_DJ%'
				      ) b 
				where char(a.offer_id) = b.offer_id 
				and a.state=1
				and a.valid_type = 1 
				 and a.OP_TIME = '2011-05-17'
				 and date(a.VALID_DATE)<='2011-05-17'
			    ) AS T where t.rn = 1 
			 with ur 
			20110517 as TIME_ID
			,a.USER_ID
			,a.ADD_PKG_ID
			,a.VALID_DT
		from  bass1.G_I_02023_DAY_1 as a 
		      ,bass2.dw_product_20110517 as b
		    where a.user_id = b.user_id
		    and b.usertype_id in (1,2,9) 
		    and b.userstatus_id in (1,2,3,6,8)
		    and b.test_mark<>1
	with ur 
					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0114' )
                                
--,  count(distinct time_id ) 
from G_I_02023_DAY 
group by  time_id 
order by 1 
                                     ,(select xzbas_value  as offer_id ,BASS1_VALUE_DESC
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0114'
                                              and bass1_value like 'QW_QQT_JC%'
                                      ) b 
                                where char(a.offer_id) = b.offer_id 
				and a.valid_type = 1 
				 and a.OP_TIME = '2011-05-17'
				 and date(a.VALID_DATE)<='2011-05-17'

		20110517 TIME_ID
		,char(a.product_instance_id)  USER_ID
		,bass1.fn_get_all_dim_EX('BASS_STD1_0114',char(a.offer_id)) BASE_PKG_ID
		,replace(char(date(a.create_date)),'-','') VALID_DT
	from  bass2.Dw_product_ins_off_ins_prod_ds a
	,(select xzbas_value  as offer_id 
					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0114'
					      and bass1_value like 'QW_QQT_JC%'
				      ) c
	where  char(a.offer_id) = c.offer_id 
	  and a.state =1
	  and a.valid_type = 1
	  and a.OP_TIME = '2011-05-17'	  
	  and date(a.VALID_DATE)<='2011-05-17'	  
	  and not exists (	select 1 from bass2.dwd_product_test_phone_20110517 b 
				where a.product_instance_id = b.USER_ID  and b.sts = 1
	 with ur 
		20110517 TIME_ID
		,char(a.product_instance_id)  USER_ID
		,bass1.fn_get_all_dim_EX('BASS_STD1_0114',char(a.offer_id)) BASE_PKG_ID
		,replace(char(date(a.create_date)),'-','') VALID_DT
	from  bass2.Dw_product_ins_off_ins_prod_ds a
	,(select xzbas_value  as offer_id 
					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0114'
					      and bass1_value like 'QW_QQT_JC%'
				      ) c
	where  char(a.offer_id) = c.offer_id 
	  and a.state =1
	  and a.valid_type = 1
	  and a.OP_TIME = '2011-05-17'	  
	  and date(a.VALID_DATE)<='2011-05-17'	  
	  and not exists (	select 1 from bass2.dwd_product_test_phone_20110517 b 
				where a.product_instance_id = b.USER_ID  and b.sts = 1
	 with ur 
111090001335
111090001352

	from  bass2.Dw_product_ins_off_ins_prod_ds a
	,(select xzbas_value  as offer_id ,XZBAS_COLNAME
					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0114'
					      and bass1_value like 'QW_QQT_JC%'
				      ) c
	where  char(a.offer_id) = c.offer_id 
	  and a.state =1
	  and a.valid_type = 1
	  and a.OP_TIME = '2011-05-17'	  
	  and date(a.VALID_DATE)<='2011-05-17'	  
	  and not exists (	select 1 from bass2.dwd_product_test_phone_20110517 b 
				where a.product_instance_id = b.USER_ID  and b.sts = 1
	 with ur 
where time_id in (20110517)
and return_flag = 1
(
 89710018
,89510018
,89110018
,89410018
,89610018
,89210018
,89310018
,89710017
,89510017
,89110017
,89410017
,89610017
,89210017
,89310017
,89710016
,89510016
,89110016
,89410016
,89610016
,89210016
,89310016
) 
(
 89710018
,89510018
,89110018
,89410018
,89610018
,89210018
,89310018
,89710017
,89510017
,89110017
,89410017
,89610017
,89210017
,89310017
,89710016
,89510016
,89110016
,89410016
,89610016
,89210016
,89310016
) 
        from  bass2.Dw_product_ins_off_ins_prod_ds a
        ,(select xzbas_value  as offer_id 
                                        from  BASS1.ALL_DIM_LKP 
                                        where BASS1_TBID = 'BASS_STD1_0114'
                                              and bass1_value like 'QW_QQT_JC%'
                                              and XZBAS_COLNAME not like '�ײͼ���%'
                                      ) c
        where  char(a.offer_id) = c.offer_id 
          and a.OP_TIME = '2011-05-17'    
          and date(a.VALID_DATE)<='2011-05-17'    
          and a.valid_type = 1
          and not exists (      select 1 from bass2.dwd_product_test_phone_20110517 b 
                                where a.product_instance_id = b.USER_ID  and b.sts = 1
                         ) 
--,  count(distinct time_id ) 
from bass1.g_i_02022_day 
group by  time_id 
order by 1 
--,  count(distinct time_id ) 
from bass1.g_i_02023_day 
group by  time_id 
order by 1 
select VALID_DT , count(0) 
--,  count(distinct VALID_DT ) 
from bass1.g_i_02022_day 
group by  VALID_DT 
order by 1 


select substr(rule_code,6) seq
,case 
    when rule_code = 'R161_1' then '�����ͻ���'
    when rule_code = 'R161_2' then '�ͻ�������'
    when rule_code = 'R161_3' then '�����ͻ���'
    when rule_code = 'R161_4' then 'ͨ�ſͻ���'
    when rule_code = 'R161_5' then '�����ۼ�ͨ�ſͻ���'
    when rule_code = 'R161_6' then 'ʹ��TD����Ŀͻ���'
    when rule_code = 'R161_7' then '�����ۼ�ʹ��TD������ֻ��ͻ���'
    when rule_code = 'R161_8' then '�����ۼ�ʹ��TD�������Ϣ���ͻ���'
    when rule_code = 'R161_9' then '�����ۼ�ʹ��TD��������ݿ��ͻ���'
    when rule_code = 'R161_10' then '�����ۼ�ʹ��TD������������ͻ���'
    when rule_code = 'R161_11' then '��ͨ�ƶ��ͻ�����'
    when rule_code = 'R161_12' then '�����ƶ��ͻ�����'
    when rule_code = 'R161_13' then '��ͨ�ƶ������ͻ���'
    when rule_code = 'R161_14' then '�����ƶ������ͻ���'
    when rule_code = 'R161_15' then 'ʹ��TD����Ŀͻ���T���ϼƷ�ʱ��'
    when rule_code = 'R161_16' then 'ʹ��TD����Ŀͻ���T���ϵ���������'
    when rule_code = 'R161_17' then '�����ͻ���'
else '0'
end rule_name
,case 
    when rule_code = 'R161_1' and abs(target3*100) > 15 then '����'
    when rule_code = 'R161_2' and abs(target3*100) > 2 then '����'
    when rule_code = 'R161_3' and abs(target3*100) > 100 then '����'
    when rule_code = 'R161_4' and abs(target3*100) > 5 then '����'
    when rule_code = 'R161_5' and abs(target3*100) > 5 then '����'
    when rule_code = 'R161_6' and abs(target3*100) > 5 then '����'
    when rule_code = 'R161_7' and abs(target3*100) > 5 then '����'
    when rule_code = 'R161_8' and abs(target3*100) > 5 then '����'
    when rule_code = 'R161_9' and abs(target3*100) > 5 then '����'
    when rule_code = 'R161_10' and abs(target3*100) > 5 then '����'
    when rule_code = 'R161_11' and abs(target3*100) > 2 then '����'
    when rule_code = 'R161_12' and abs(target3*100) > 2 then '����'
    when rule_code = 'R161_13' and abs(target3*100) > 8 then '����'
    when rule_code = 'R161_14' and abs(target3*100) > 8 then '����'
    when rule_code = 'R161_15' and abs(target3*100) > 20 then '����'
    when rule_code = 'R161_16' and abs(target3*100) > 20 then '����'
    when rule_code = 'R161_17' and abs(target3*100) > 70 then '����'
else '0'
end IF_OK
 from 
bass1.g_rule_check a 
and time_id = int(replace(char(current date - 1 days),'-',''))
THEN 'S'||rule_code 
ELSE rule_code END 
asc 

where rule_code like 'R161_%'
and time_id = 20110517
,case 

/**
select * from  app.sch_control_alarm 
where alarmtime >=  timestamp('20110322'||'000000') 
--and flag = -1
and control_code like 'BASS1%'
order by alarmtime desc 
**/

                                                                
select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1. g_s_22063_month where time_id =201104)
  and time_id =201104
  and channel_type in ('2','3')
  and channel_status='1'
                                                                
                                                                
                                                                


           
MON_ALL_INTERFACE
MON_INTERFACE_NOT_EMPTY
MON_USER_MOBILE
MONIT_SQL
MONITOR_CONFIG
MONITOR_LIST
MONTH_02006_MID1
MONTH_02006_MID2

