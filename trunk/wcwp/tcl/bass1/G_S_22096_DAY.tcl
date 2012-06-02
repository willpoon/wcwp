
######################################################################################################		
#接口名称: 电子渠道重点增值业务办理日汇总                                                               
#接口编码：22096                                                                                          
#接口说明：记录电子渠道29项重点增值业办理日汇总信息。
#程序名称: G_S_22096_DAY.tcl                                                                            
#功能描述: 生成22096的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120529
#问题记录：
#修改历史: 1. panzw 20120529	中国移动一级经营分析系统省级数据接口规范 (V1.8.1) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
      
    #上个月 yyyymm
    set last_month [GetLastMonth [string range $op_month 0 5]]
    puts $last_month
	set curr_month [string range $op_time 0 3][string range $op_time 5 6]
        #程序名
        global app_name
        set app_name "G_S_22096_DAY.tcl"
	
  #删除本期数据
	set sql_buff "delete from bass1.G_S_22096_DAY where time_id=$timestamp"
	exec_sql $sql_buff


 set sql_buff "alter table bass1.G_S_22096_DAY_1 activate not logged initially with empty table"
	exec_sql $sql_buff

#########################################################################################################

##~   网站：
	set sql_buff "
			insert into G_S_22096_DAY_1
			select 
			'01' ECHNL_TYPE
			,case 
			when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
			when INDEX_ID1 = 23 then '02'
			when INDEX_ID1 = 10 then '03'
			when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
			when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
			when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
			when INDEX_ID1 = 22 then '08'
			when INDEX_ID1 = 24 then '09'
			--when INDEX_ID1 = 24 then '10' 无该业务
			when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
			when INDEX_ID1 = 25 then '12'
			--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 关联另表
			--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 关联另表
			when INDEX_ID1 = 25 then '15'
			when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
			when INDEX_ID1 = 27 then '17'
			when INDEX_ID1 = 13 then '18'
			when INDEX_ID1 = 27 then '19'
			when INDEX_ID1 = 17 then '20'
			when INDEX_ID1 = 8  then '21'
			when INDEX_ID1 = 9  then '22'
			when INDEX_ID1 = 7  then '23'
			when INDEX_ID1 = 28 then '24'
			when INDEX_ID1 = 11 then '25'
			when INDEX_ID1 = 19 then '26'
			--when INDEX_ID1 = 24 then '27'  无该业务
			--when INDEX_ID1 = 24 then '28'  无该业务
			when INDEX_ID1 = 21 then '29'
			else '99' end IMP_VAL_TYPE
			,char(count(0)) OPEN_CNT
			from bass2.dw_product_ord_cust_dm_$curr_month a
			, bass2.dw_product_$timestamp b
			, bass2.dw_product_ord_offer_dm_$curr_month c
			, bass2.DIM_DATA_OFFER d
			where a.product_instance_id = b.user_id 
			and a.ORDER_STATE = 11
			and a.CUSTOMER_ORDER_ID = c.CUSTOMER_ORDER_ID
			and c.OFFER_ID = d.OFFER_ID
			and upper(a.channel_type) = 'B' and    a.op_time = '$op_time' 
			group by case 
			when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
			when INDEX_ID1 = 23 then '02'
			when INDEX_ID1 = 10 then '03'
			when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
			when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
			when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
			when INDEX_ID1 = 22 then '08'
			when INDEX_ID1 = 24 then '09'
			--when INDEX_ID1 = 24 then '10' 无该业务
			when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
			when INDEX_ID1 = 25 then '12'
			--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 关联另表
			--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 关联另表
			when INDEX_ID1 = 25 then '15'
			when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
			when INDEX_ID1 = 27 then '17'
			when INDEX_ID1 = 13 then '18'
			when INDEX_ID1 = 27 then '19'
			when INDEX_ID1 = 17 then '20'
			when INDEX_ID1 = 8  then '21'
			when INDEX_ID1 = 9  then '22'
			when INDEX_ID1 = 7  then '23'
			when INDEX_ID1 = 28 then '24'
			when INDEX_ID1 = 11 then '25'
			when INDEX_ID1 = 19 then '26'
			when INDEX_ID1 = 21 then '29'
			else '99' end
			"
	exec_sql $sql_buff


	set sql_buff "
			insert into G_S_22096_DAY_1
		select 
		'01' ECHNL_TYPE
		,case 
		when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
		when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
		else '99' end IMP_VAL_TYPE
		,char(count(0)) OPEN_CNT
		from bass2.dw_product_ord_cust_dm_$curr_month a
		, bass2.dw_product_$timestamp b
		, bass2.dw_product_ord_offer_dm_$curr_month c
		, bass2.DIM_DATA_SRVPKG d
		where a.product_instance_id = b.user_id 
		and a.ORDER_STATE = 11
		and a.CUSTOMER_ORDER_ID = c.CUSTOMER_ORDER_ID
		and c.OFFER_ID = bigint('11'||substr(char(d.SRVPKG_ID),3))
		and upper(a.channel_type) = 'B' and    a.op_time = '$op_time' 
		group by case 
		when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
		when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
		else '99' end
			"
	exec_sql $sql_buff

#########################################################################################################
##~   10086
	set sql_buff "
			insert into G_S_22096_DAY_1
select 
'02' ECHNL_TYPE
,case 
when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
when INDEX_ID1 = 23 then '02'
when INDEX_ID1 = 10 then '03'
when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
when INDEX_ID1 = 22 then '08'
when INDEX_ID1 = 24 then '09'
--when INDEX_ID1 = 24 then '10' 无该业务
when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
when INDEX_ID1 = 25 then '12'
--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 关联另表
--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 关联另表
when INDEX_ID1 = 25 then '15'
when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
when INDEX_ID1 = 27 then '17'
when INDEX_ID1 = 13 then '18'
when INDEX_ID1 = 27 then '19'
when INDEX_ID1 = 17 then '20'
when INDEX_ID1 = 8  then '21'
when INDEX_ID1 = 9  then '22'
when INDEX_ID1 = 7  then '23'
when INDEX_ID1 = 28 then '24'
when INDEX_ID1 = 11 then '25'
when INDEX_ID1 = 19 then '26'
--when INDEX_ID1 = 24 then '27'  无该业务
--when INDEX_ID1 = 24 then '28'  无该业务
when INDEX_ID1 = 21 then '29'
else '99' end IMP_VAL_TYPE
,char(count(0)) OPEN_CNT
from bass2.dw_product_ins_off_ins_prod_ds a
/**,
(
  SELECT * FROM bass2.DIM_BOSS_STAFF
  WHERE OP_NAME LIKE '%客服%'
 ) b
 **/
,bass2.dw_product_$timestamp c
, bass2.DIM_DATA_OFFER d
where  ( a.op_id =  91000038 or a.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%客服%'))
and a.product_instance_id = c.user_id
and a.offer_id = d.offer_id
and date(a.DONE_DATE) = '$op_time'
group by case 
when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
when INDEX_ID1 = 23 then '02'
when INDEX_ID1 = 10 then '03'
when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
when INDEX_ID1 = 22 then '08'
when INDEX_ID1 = 24 then '09'
--when INDEX_ID1 = 24 then '10' 无该业务
when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
when INDEX_ID1 = 25 then '12'
--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 关联另表
--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 关联另表
when INDEX_ID1 = 25 then '15'
when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
when INDEX_ID1 = 27 then '17'
when INDEX_ID1 = 13 then '18'
when INDEX_ID1 = 27 then '19'
when INDEX_ID1 = 17 then '20'
when INDEX_ID1 = 8  then '21'
when INDEX_ID1 = 9  then '22'
when INDEX_ID1 = 7  then '23'
when INDEX_ID1 = 28 then '24'
when INDEX_ID1 = 11 then '25'
when INDEX_ID1 = 19 then '26'
when INDEX_ID1 = 21 then '29'
else '99' end
			"
	exec_sql $sql_buff

 	set sql_buff "
			insert into G_S_22096_DAY_1
select 
'02' ECHNL_TYPE
,case 
when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
else '99' end IMP_VAL_TYPE
,char(count(0)) OPEN_CNT
from bass2.dw_product_ins_off_ins_prod_ds a
/**,
(
  SELECT * FROM bass2.DIM_BOSS_STAFF
  WHERE OP_NAME LIKE '%客服%'
 ) b **/
,bass2.dw_product_$timestamp c
, bass2.DIM_DATA_SRVPKG d
where    ( a.op_id =  91000038 or a.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%客服%'))
and a.product_instance_id = c.user_id
and a.offer_id = bigint('11'||substr(char(d.SRVPKG_ID),3))
and date(a.DONE_DATE) = '$op_time'
group by case 
when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
else '99' end
			"
	exec_sql $sql_buff


#########################################################################################################

#########################################################################################################
##~  短信营业厅
	set sql_buff "
			insert into G_S_22096_DAY_1
select 
'03' ECHNL_TYPE
,case 
when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
when INDEX_ID1 = 23 then '02'
when INDEX_ID1 = 10 then '03'
when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
when INDEX_ID1 = 22 then '08'
when INDEX_ID1 = 24 then '09'
--when INDEX_ID1 = 24 then '10' 无该业务
when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
when INDEX_ID1 = 25 then '12'
--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 关联另表
--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 关联另表
when INDEX_ID1 = 25 then '15'
when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
when INDEX_ID1 = 27 then '17'
when INDEX_ID1 = 13 then '18'
when INDEX_ID1 = 27 then '19'
when INDEX_ID1 = 17 then '20'
when INDEX_ID1 = 8  then '21'
when INDEX_ID1 = 9  then '22'
when INDEX_ID1 = 7  then '23'
when INDEX_ID1 = 28 then '24'
when INDEX_ID1 = 11 then '25'
when INDEX_ID1 = 19 then '26'
--when INDEX_ID1 = 24 then '27'  无该业务
--when INDEX_ID1 = 24 then '28'  无该业务
when INDEX_ID1 = 21 then '29'
else '99' end IMP_VAL_TYPE
,char(count(0)) OPEN_CNT
from bass2.dw_product_ins_off_ins_prod_ds a
/**,
(
  SELECT * FROM bass2.DIM_BOSS_STAFF
  WHERE OP_NAME LIKE '%客服%'
 ) b
 **/
,bass2.dw_product_$timestamp c
, bass2.DIM_DATA_OFFER d
where   ( a.op_id =  10000047 or a.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%自助%'))
and a.product_instance_id = c.user_id
and a.offer_id = d.offer_id
and date(a.DONE_DATE) = '$op_time'
group by case 
when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
when INDEX_ID1 = 23 then '02'
when INDEX_ID1 = 10 then '03'
when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
when INDEX_ID1 = 22 then '08'
when INDEX_ID1 = 24 then '09'
--when INDEX_ID1 = 24 then '10' 无该业务
when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
when INDEX_ID1 = 25 then '12'
--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 关联另表
--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 关联另表
when INDEX_ID1 = 25 then '15'
when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
when INDEX_ID1 = 27 then '17'
when INDEX_ID1 = 13 then '18'
when INDEX_ID1 = 27 then '19'
when INDEX_ID1 = 17 then '20'
when INDEX_ID1 = 8  then '21'
when INDEX_ID1 = 9  then '22'
when INDEX_ID1 = 7  then '23'
when INDEX_ID1 = 28 then '24'
when INDEX_ID1 = 11 then '25'
when INDEX_ID1 = 19 then '26'
when INDEX_ID1 = 21 then '29'
else '99' end
			"
	exec_sql $sql_buff

 	set sql_buff "
			insert into G_S_22096_DAY_1
select 
'03' ECHNL_TYPE
,case 
when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
else '99' end IMP_VAL_TYPE
,char(count(0)) OPEN_CNT
from bass2.dw_product_ins_off_ins_prod_ds a
/**,
(
  SELECT * FROM bass2.DIM_BOSS_STAFF
  WHERE OP_NAME LIKE '%客服%'
 ) b **/
,bass2.dw_product_$timestamp c
, bass2.DIM_DATA_SRVPKG d
where    ( a.op_id =  10000047 or a.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%自助%'))
and a.product_instance_id = c.user_id
and a.offer_id = bigint('11'||substr(char(d.SRVPKG_ID),3))
and date(a.DONE_DATE) = '$op_time'
group by case 
when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
else '99' end
			"
	exec_sql $sql_buff
#########################################################################################################
##~   wap　无

#########################################################################################################
##~   05:自助终端
	set sql_buff "
insert into G_S_22096_DAY_1
select 
'05' ECHNL_TYPE
,case 
when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
when INDEX_ID1 = 23 then '02'
when INDEX_ID1 = 10 then '03'
when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
when INDEX_ID1 = 22 then '08'
when INDEX_ID1 = 24 then '09'
--when INDEX_ID1 = 24 then '10' 无该业务
when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
when INDEX_ID1 = 25 then '12'
--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 关联另表
--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 关联另表
when INDEX_ID1 = 25 then '15'
when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
when INDEX_ID1 = 27 then '17'
when INDEX_ID1 = 13 then '18'
when INDEX_ID1 = 27 then '19'
when INDEX_ID1 = 17 then '20'
when INDEX_ID1 = 8  then '21'
when INDEX_ID1 = 9  then '22'
when INDEX_ID1 = 7  then '23'
when INDEX_ID1 = 28 then '24'
when INDEX_ID1 = 11 then '25'
when INDEX_ID1 = 19 then '26'
--when INDEX_ID1 = 24 then '27'  无该业务
--when INDEX_ID1 = 24 then '28'  无该业务
when INDEX_ID1 = 21 then '29'
else '99' end IMP_VAL_TYPE
,char(count(0)) OPEN_CNT
from bass2.dw_product_ins_off_ins_prod_ds a
/**,
(
  SELECT * FROM bass2.DIM_BOSS_STAFF
  WHERE OP_NAME LIKE '%客服%'
 ) b
 **/
,bass2.dw_product_$timestamp c
, bass2.DIM_DATA_OFFER d
where ( a.op_id =  10100101 or a.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%自助%'))
and a.product_instance_id = c.user_id
and a.offer_id = d.offer_id
and date(a.DONE_DATE) = '$op_time'
group by case 
when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
when INDEX_ID1 = 23 then '02'
when INDEX_ID1 = 10 then '03'
when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
when INDEX_ID1 = 22 then '08'
when INDEX_ID1 = 24 then '09'
--when INDEX_ID1 = 24 then '10' 无该业务
when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
when INDEX_ID1 = 25 then '12'
--when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 关联另表
--when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 关联另表
when INDEX_ID1 = 25 then '15'
when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
when INDEX_ID1 = 27 then '17'
when INDEX_ID1 = 13 then '18'
when INDEX_ID1 = 27 then '19'
when INDEX_ID1 = 17 then '20'
when INDEX_ID1 = 8  then '21'
when INDEX_ID1 = 9  then '22'
when INDEX_ID1 = 7  then '23'
when INDEX_ID1 = 28 then '24'
when INDEX_ID1 = 11 then '25'
when INDEX_ID1 = 19 then '26'
when INDEX_ID1 = 21 then '29'
else '99' end
			"
	exec_sql $sql_buff

 	set sql_buff "
insert into G_S_22096_DAY_1
select 
'05' ECHNL_TYPE
,case 
when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
else '99' end IMP_VAL_TYPE
,char(count(0)) OPEN_CNT
from bass2.dw_product_ins_off_ins_prod_ds a
/**,
(
  SELECT * FROM bass2.DIM_BOSS_STAFF
  WHERE OP_NAME LIKE '%客服%'
 ) b **/
,bass2.dw_product_$timestamp c
, bass2.DIM_DATA_SRVPKG d
where   ( a.op_id =  10100101 or a.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%自助%'))
and a.product_instance_id = c.user_id
and a.offer_id = bigint('11'||substr(char(d.SRVPKG_ID),3))
and date(a.DONE_DATE) = '$op_time'
group by case 
when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
else '99' end
			"
	exec_sql $sql_buff



##~  自助终端：
	##~   set sql_buff "
			##~   insert into G_S_22096_DAY_1
			##~   select 
			##~   '05' ECHNL_TYPE
			##~   ,case 
			##~   when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
			##~   when INDEX_ID1 = 23 then '02'
			##~   when INDEX_ID1 = 10 then '03'
			##~   when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
			##~   when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
			##~   when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
			##~   when INDEX_ID1 = 22 then '08'
			##~   when INDEX_ID1 = 24 then '09'
			##~   --when INDEX_ID1 = 24 then '10' 无该业务
			##~   when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
			##~   when INDEX_ID1 = 25 then '12'
			##~   --when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 关联另表
			##~   --when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 关联另表
			##~   when INDEX_ID1 = 25 then '15'
			##~   when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
			##~   when INDEX_ID1 = 27 then '17'
			##~   when INDEX_ID1 = 13 then '18'
			##~   when INDEX_ID1 = 27 then '19'
			##~   when INDEX_ID1 = 17 then '20'
			##~   when INDEX_ID1 = 8  then '21'
			##~   when INDEX_ID1 = 9  then '22'
			##~   when INDEX_ID1 = 7  then '23'
			##~   when INDEX_ID1 = 28 then '24'
			##~   when INDEX_ID1 = 11 then '25'
			##~   when INDEX_ID1 = 19 then '26'
			##~   --when INDEX_ID1 = 24 then '27'  无该业务
			##~   --when INDEX_ID1 = 24 then '28'  无该业务
			##~   when INDEX_ID1 = 21 then '29'
			##~   else '99' end IMP_VAL_TYPE
			##~   ,char(count(0)) OPEN_CNT
			##~   from bass2.dw_product_ord_cust_dm_$curr_month a
			##~   , bass2.dw_product_$timestamp b
			##~   , bass2.dw_product_ord_offer_dm_$curr_month c
			##~   , bass2.DIM_DATA_OFFER d
			##~   where a.product_instance_id = b.user_id 
			##~   and a.ORDER_STATE = 11
			##~   and a.CUSTOMER_ORDER_ID = c.CUSTOMER_ORDER_ID
			##~   and c.OFFER_ID = d.OFFER_ID
			##~   and upper(a.channel_type) = '9' and    a.op_time = '$op_time' 
			##~   group by case 
			##~   when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
			##~   when INDEX_ID1 = 23 then '02'
			##~   when INDEX_ID1 = 10 then '03'
			##~   when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
			##~   when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
			##~   when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
			##~   when INDEX_ID1 = 22 then '08'
			##~   when INDEX_ID1 = 24 then '09'
			##~   --when INDEX_ID1 = 24 then '10' 无该业务
			##~   when INDEX_ID1 = 6 and INDEX_ID2 = 26 then '11'
			##~   when INDEX_ID1 = 25 then '12'
			##~   --when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 关联另表
			##~   --when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 关联另表
			##~   when INDEX_ID1 = 25 then '15'
			##~   when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
			##~   when INDEX_ID1 = 27 then '17'
			##~   when INDEX_ID1 = 13 then '18'
			##~   when INDEX_ID1 = 27 then '19'
			##~   when INDEX_ID1 = 17 then '20'
			##~   when INDEX_ID1 = 8  then '21'
			##~   when INDEX_ID1 = 9  then '22'
			##~   when INDEX_ID1 = 7  then '23'
			##~   when INDEX_ID1 = 28 then '24'
			##~   when INDEX_ID1 = 11 then '25'
			##~   when INDEX_ID1 = 19 then '26'
			##~   when INDEX_ID1 = 21 then '29'
			##~   else '99' end
			##~   "
	##~   exec_sql $sql_buff


	##~   set sql_buff "
			##~   insert into G_S_22096_DAY_1
		##~   select 
		##~   '05' ECHNL_TYPE
		##~   ,case 
		##~   when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
		##~   when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
		##~   else '99' end IMP_VAL_TYPE
		##~   ,char(count(0)) OPEN_CNT
		##~   from bass2.dw_product_ord_cust_dm_$curr_month a
		##~   , bass2.dw_product_$timestamp b
		##~   , bass2.dw_product_ord_offer_dm_$curr_month c
		##~   , bass2.DIM_DATA_SRVPKG d
		##~   where a.product_instance_id = b.user_id 
		##~   and a.ORDER_STATE = 11
		##~   and a.CUSTOMER_ORDER_ID = c.CUSTOMER_ORDER_ID
		##~   and c.OFFER_ID = bigint('11'||substr(char(d.SRVPKG_ID),3))
		##~   and upper(a.channel_type) = '9' and    a.op_time = '$op_time' 
		##~   group by case 
		##~   when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 
		##~   when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 
		##~   else '99' end
			##~   "
	##~   exec_sql $sql_buff


####################################################################################################


 	set sql_buff "
		insert into G_S_22096_DAY
		(
				 TIME_ID
				,OP_TIME
				,ECHNL_TYPE
				,IMP_VAL_TYPE
				,OPEN_CNT
		)
		select 
				$timestamp TIME_ID
				,'$timestamp' OP_TIME
				,ECHNL_TYPE
				,IMP_VAL_TYPE
				,OPEN_CNT
		from G_S_22096_DAY_1
		where  IMP_VAL_TYPE <> '99'
		with ur
"

	exec_sql $sql_buff


  #1.检查chkpkunique
  set tabname "G_S_22096_DAY"
  set pk   "OP_TIME||ECHNL_TYPE||IMP_VAL_TYPE"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #
  aidb_runstats bass1.G_S_22066_DAY 3


	return 0
}

##~   此字段仅取以下分类：
##~   01:门户网站
##~   02:10086电话营业厅
##~   03: 短信营业厅
##~   04:WAP网站
##~   05:自助终端（包括所有的自助终端，即包括实体渠道和24小时营业厅内布放的自助终端，还包括商场等场所独立摆放的自助终端。）



					
##~   TIME_ID                        SYSIBM    INTEGER                   4     0 Yes   
##~   OP_TIME                        SYSIBM    CHARACTER                 8     0 Yes   
##~   ECHNL_TYPE                     SYSIBM    CHARACTER                 2     0 Yes   
##~   IMP_VAL_TYPE                   SYSIBM    CHARACTER                 2     0 Yes   
##~   OPEN_CNT                       SYSIBM    CHARACTER                10     0 Yes   

					
					
					 
##~   case 
##~   when INDEX_ID1 = 15 and INDEX_ID2 = 71 then '01'
##~   when INDEX_ID1 = 23 then '02'
##~   when INDEX_ID1 = 10 then '03'
##~   when INDEX_ID1 = 15 and INDEX_ID2 = 75 then '04'
##~   when INDEX_ID1 = 20 and INDEX_ID2 = 88 then '06'
##~   when INDEX_ID1 = 12 and INDEX_ID2 in (58,93) then '07'
##~   when INDEX_ID1 = 22 then '08'
##~   when INDEX_ID1 = 24 then '09'
##~   --when INDEX_ID1 = 24 then '10' 无该业务
##~   when INDEX_ID1 = 24 then '11'
##~   when INDEX_ID1 = 25 then '12'
##~   --when INDEX_ID1 = 4 and INDEX_ID2 = 19 then '13' 关联另表
##~   --when INDEX_ID1 = 4 and INDEX_ID2 = 20 then '14' 关联另表
##~   when INDEX_ID1 = 25 then '15'
##~   when INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29) then '16'
##~   when INDEX_ID1 = 27 then '17'
##~   when INDEX_ID1 = 13 then '18'
##~   when INDEX_ID1 = 27 then '19'
##~   when INDEX_ID1 = 17 then '20'
##~   when INDEX_ID1 = 8  then '21'
##~   when INDEX_ID1 = 9  then '22'
##~   when INDEX_ID1 = 7  then '23'
##~   when INDEX_ID1 = 28 then '24'
##~   when INDEX_ID1 = 11 then '25'
##~   when INDEX_ID1 = 19 then '26'
##~   --when INDEX_ID1 = 24 then '27'  无该业务
##~   --when INDEX_ID1 = 24 then '28'  无该业务
##~   when INDEX_ID1 = 21 then '29'
##~   else '99' end 


##~   01:来电提醒  INDEX_ID1 = 15 and INDEX_ID2 = 71
##~   02:语音信箱  INDEX_ID1 = 23
##~   03:号簿管家  INDEX_ID1 = 10  
##~   04:短信回执  INDEX_ID1 = 15 and INDEX_ID2 = 75
##~   05:信息管家  -
##~   06:飞信会员  INDEX_ID1 = 20 and INDEX_ID2 = 88
##~   07:139邮箱收费版 INDEX_ID1 = 12 and INDEX_ID2 in (58,93)
##~   08:手机证券  INDEX_ID1 = 22 
##~   09:手机商界  INDEX_ID1 = 24
##~   10:blackberry -
##~   11:无线音乐俱乐部 INDEX_ID1 = 6 and INDEX_ID2 = 26
##~   12:手机动漫  INDEX_ID1 = 25
##~   --13:彩铃		- bass2.DIM_DATA_SRVPKG  INDEX_ID1 = 4 and INDEX_ID2 = 19
##~   --14:多媒体彩铃 - bass2.DIM_DATA_SRVPKG  INDEX_ID1 = 4 and INDEX_ID2 = 20
##~   15:音乐随身听 INDEX_ID1 = 25
##~   16:歌曲下载   INDEX_ID1 = 6 and INDEX_ID2 in (27,28,29)
##~   17:手机电视(数字广播方式)    INDEX_ID1 = 15 and INDEX_ID2 = 27
##~   18:手机医疗    INDEX_ID1 = 13
##~   19:手机地图	   INDEX_ID1 = 27
##~   20:手机导航		INDEX_ID1 = 17
##~   21:快讯			INDEX_ID1 = 8
##~   22:手机阅读		INDEX_ID1 = 9
##~   23:手机报		INDEX_ID1 = 7
##~   24:无线体育俱乐部 INDEX_ID1 = 28
##~   25:手机视频		INDEX_ID1 = 11
##~   26:手机游戏		INDEX_ID1 = 19
##~   27:彩像		-
##~   28:移动应用商城  -
##~   29:12580生活播报  INDEX_ID1 = 21





##~   CREATE TABLE "BASS1   "."G_S_22096_DAY_1"  (
                  ##~   "TIME_ID" INTEGER , 
                  ##~   "OP_TIME" CHAR(8) , 
                  ##~   "ECHNL_TYPE" CHAR(2) , 
                  ##~   "IMP_VAL_TYPE" CHAR(2) , 
                  ##~   "OPEN_CNT" CHAR(10) )   
                 ##~   DISTRIBUTE BY HASH("OP_TIME",  
                 ##~   "ECHNL_TYPE",  
                 ##~   "IMP_VAL_TYPE")   
                   ##~   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 

