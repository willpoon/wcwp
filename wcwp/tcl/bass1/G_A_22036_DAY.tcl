#=====================================================================*
#  G_A_22036_DAY.tcl                              *
#集团客户端口资源使用开展情况
#  bass2.dim_bs_enterprise_customer_code                               *
#  bass2.DW_ENTERPRISE_SUB_yyyymm                                      *
#	 bass2.dw_newbusi_ismg_yyyymm                                        *
#	 BASS2.DIM_NEWBUSI_SPINFO                                            *
#*规则类型：月                                                          *
#*规则描述：22036接口程序                                               *
#功能描述：记录各省的号码资源信息                                        *
#调用例程：int -s G_A_22036_DAY.tcl                                  *
#创建日期：2007-08-23                                                  *
#创 建 者：xiahuaxue                                                   *
#修改历史：1.5.3版本，22036改为集团行业应用业务量接口 2008-03-27         *
#	20111027 修改接口22036（集团客户端口资源使用开展情况）的名称为“集团客户端口资源使用情况”，
#增加字段"集团业务名称"、“开通日期”、“状态”。接口传输方式改为日增量接口。
#接口单元说明：本接口描述是统计支撑各省公司集团客户或SI占用或使用我公司端口号码资源或GPRS专用APN的启用和发送情况。各省公司上报的“行业应用代码全码”以及“APN网络标识”，应保持与相应话单中的服务代码或APN网络标识保持一致。
#======================================================================*

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
	    global env
      global handle

       

	     #当天 yyyymmdd
		##~   set op_time 2012-07-31
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]   

        
        ##~   set curr_month 201207

        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

			 #上月  yyyymm

			 set last_month [GetLastMonth [string range $op_month 0 5]]

			 #上上月 yyyymm

			 set last_last_month [GetLastMonth [string range $last_month 0 5]]

        global app_name
        set app_name "G_A_22036_DAY.tcl"



set curr_month [string range $op_time 0 3][string range $op_time 5 6]

	set sql_buff "delete from bass1.G_A_22036_DAY where TIME_ID < 20111101"
  exec_sql $sql_buff
  
  #删除本期数据
	set sql_buff "delete from bass1.G_A_22036_DAY where TIME_ID= $timestamp"
  exec_sql $sql_buff
  #--------------------------------------------------------------------------------------------------------------
  


  
  set sql_buff "insert into BASS1.G_A_22036_DAY
                select a.TIME_ID
				--, a.BILL_MONTH
				, a.CUST_TYPE, a.EC_CODE, a.SINAME, 
                       a.OPERATE_TYPE, a.APP_LENCODE, a.APNCODE ,service_name ,open_date,sts
                from
                (
select distinct 
$timestamp   			TIME_ID
--,'$curr_month'        BILL_MONTH
,'0'                  	CUST_TYPE
,cust_id            	EC_CODE
,''                   	SINAME    
,case   when ATTR_3 like 'QXZ%' then '1'
		when ATTR_3 like 'M%' then '1' else '2' end as OPERATE_TYPE
,ATTR_2      			APP_LENCODE
,'' 						APNCODE
,name 					SERVICE_NAME
,replace(char(date(VALID_DATE)),'-','') open_date
,case when replace(char(date(EXPIRE_DATE)),'-','') > '$timestamp' then '1' else '2' end sts
from 
(select a.*,b.name,c.cust_id
,row_number()over(partition by a.ATTR_2 order by a.EXPIRE_DATE desc , a.VALID_DATE desc ) rn 
from bass2.Dw_product_ins_srv_ds  a 
	  ,(
			select product_item_id ,name from   bass2.dim_prod_up_product_item
			where platform_id = 2
			and item_type = 'SERVICE'
	   ) b
	  ,bass2.dw_product_$timestamp c
where  a.attr_2 like '106%'
and a.VALID_DATE <='$op_time'
and a.EXPIRE_DATE >='$op_time'
and a.service_id = b.product_item_id
and a.product_instance_id = c.user_id 
) t where t.rn =1 
) a with ur
"  
    exec_sql $sql_buff      


##~   20120627
source /bassapp/bass1/tcl/INT_FIX_TMP.tcl
Deal_fix22036 $op_time $optime_month
  


##~   20120801
##~   经咨询齐红霞，本次1.8.2割接不用报全量





##~   --~   R265	月	03_话单日志	行业网关短信话单中的“服务代码”都在集团客户端口资源使用情况接口的“行业应用代码全码”	"04016 行业网关短信话单
##~   --~   22036 集团客户端口资源使用情况"	行业网关短信话单中的“服务代码”都在集团客户端口资源使用情况接口的“行业应用代码全码”中	0.05	

##~   "Step1.04016（行业网关短信话单）接口中发送状态为0“成功”的“服务代码”集合；
##~   Step2.22036（集团客户端口资源使用情况）接口中截止到统计周期末业务类型=1“行业网关短信”的“行业应用代码全码”集合；
##~   Step3.集合Step1是否均在集合Step2中。"



set sql_buff "


select count(0)
from table(

select distinct  SERV_CODE from G_S_04016_DAY where time_id / 100 = $curr_month and SEND_STATUS = '0'
except
                        select distinct APP_LENCODE from 
                        (
                                        select t.*
                                        ,row_number()over(partition by EC_CODE,APP_LENCODE,APNCODE,BUSI_NAME order by time_id desc ) rn 
                                        from 
                                        G_A_22036_DAY  t
										where  time_id / 100 <= $curr_month
                          ) a
                        where rn = 1    and OPERATE_TYPE = '1'
										And bigint(OPEN_DATE)/100 <= $curr_month
                        
) a
with ur
"

chkzero2 $sql_buff "R265 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R265',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  




  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_A_22036_DAY"
        set pk                  "APP_LENCODE||APNCODE"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #
  ##   new
  set tabname "G_A_22036_DAY"
        set pk                  "EC_CODE||APP_LENCODE||APNCODE||BUSI_NAME"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #




return 0

    
}

##~   update(
##~   select *from app.g_runlog where unit_code = '22036' and time_id = 20120731
##~   ) t set RETURN_FLAG = 0

##~   /bassapp/backapp/bin/bass1_export/bass1_export bass1.G_A_22036_DAY 2012-07-31 &
##~   /bassapp/backapp/bin/bass1_export/bass1_export bass1.G_A_22036_DAY 2012-08-01 &





##~   CREATE TABLE "BASS1   "."G_A_22036_DAY_FIX20120731"  (
                  ##~   "TIME_ID" INTEGER , 
                  ##~   "CUST_TYPE" CHAR(1) , 
                  ##~   "EC_CODE" CHAR(20) , 
                  ##~   "SINAME" CHAR(60) , 
                  ##~   "OPERATE_TYPE" CHAR(1) , 
                  ##~   "APP_LENCODE" CHAR(21) , 
                  ##~   "APNCODE" CHAR(63) , 
                  ##~   "BUSI_NAME" CHAR(60) , 
                  ##~   "OPEN_DATE" CHAR(8) , 
                  ##~   "STS" CHAR(1) )   
                 ##~   DISTRIBUTE BY HASH("TIME_ID",  
                 ##~   "APP_LENCODE")   
                   ##~   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" NOT LOGGED INITIALLY


##~   ALTER TABLE bass1.G_S_04016_DAY_TMP_SERV_CODE ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE

##~   insert into bass1.G_S_04016_DAY_TMP_SERV_CODE
##~   select 20120731 time_id,t.* from 
 ##~   table(
##~   select distinct  SERV_CODE  SERV_CODE  
##~   from G_S_04016_DAY where time_id / 100 = 201207 and SEND_STATUS = '0'
##~   except
##~   select distinct APP_LENCODE from 
##~   (
			##~   select t.*
			##~   ,row_number()over(partition by EC_CODE,APP_LENCODE,APNCODE,BUSI_NAME order by time_id desc ) rn 
			##~   from 
			##~   G_A_22036_DAY  t
													##~   where  time_id / 100 <= 201207
##~   ) a
##~   where rn = 1    and OPERATE_TYPE = '1'
													##~   And bigint(OPEN_DATE)/100 <= 201207
##~   ) t
##~   with ur

