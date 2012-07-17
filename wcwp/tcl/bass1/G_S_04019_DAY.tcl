
######################################################################################################		
#接口名称: "国际长途语音话单"                                                               
#接口编码：04019                                                                                          
#接口说明："记录批价后的国际长途语音，包括直拨国际长途、国际IP、12593业务话单，不包括国际漫游语音。"
#程序名称: G_S_04019_DAY.tcl                                                                            
#功能描述: 生成04019的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110727
#问题记录：
#修改历史: 1. panzw 20110727	1.7.4 newly added
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {




##~   # 通过 while 循环
##~   # set i 0 设置重跑日期上限 0 为 昨日
	##~   set i 1
##~   # 设置重跑日期下限 , $i<= n   ,  n 越大，越久远
	##~   while { $i<=60 } {
	        ##~   set sql_buff "select char(current date - ( 1+$i ) days) from bass2.dual"
	        ##~   set op_time [get_single $sql_buff]
	
	##~   if { $op_time >= "2012-06-01" && $op_time <= "2012-07-10" } {
		##~   puts $op_time
		##~   Deal_proc04019 $op_time $optime_month	
	##~   }
		##~   incr i
	##~   }
	
		##~   set op_time 2012-07-11
		Deal_proc04019 $op_time $optime_month	



      ##~   set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      ##~   puts $timestamp
    ##~   #上个月 yyyymm
    
            ##~   global app_name

		##~   set app_name "G_S_04019_DAY.tcl"        

  ##~   #删除本期数据
	##~   set sql_buff "delete from bass1.G_S_04019_DAY where time_id=$timestamp"
	##~   exec_sql $sql_buff


##~   #仅报维表中的
##~   #040	港澳台
##~   #050	国际
##~   #051	美加日韩澳
##~   #052	其它国家

##~   #2100	    IP语音
##~   #2101	17950
##~   #2102	17951
##~   #2103	12593
##~   #2199	        其它

##~   #01	主叫/移动沙龙上行	
##~   #02	被叫/移动沙龙下行	
##~   #03	呼转	
##~   #

##~   #CALLTYPE_ID	CALLTYPE_NAME
##~   #0	主叫
##~   #1	被叫
##~   #2	有条件呼转
##~   #3	无条件呼转
##~   #4	短信发送
##~   #5	短信接受


##~   #ROAMTYPE_ID	ROAMTYPE_NAME
##~   #0	本地
##~   #1	省内漫游
##~   #2	省际漫入
##~   #3	国际漫入
##~   #4	省际漫出
##~   #5	港澳台国际漫出
##~   #6	省内边界漫游
##~   #7	省际边界漫入
##~   #8	省际边界漫出
##~   #9	非港澳台国际漫出
##~   #100	国际漫出(国际点对点短信)
##~   #101	国际漫入(国际点对点短信)
##~   #

#20120710 add: when substr(opp_number,1,2) = '00'    then '1000' 	
	##~   set sql_buff "
	##~   insert into G_S_04019_DAY
	##~   select 
	##~   $timestamp time_id
	##~   ,a.product_no
	##~   ,value(a.imei,'0') imei
	##~   ,case 
		##~   when tolltype_id in (3,4,5,103,104,105) then '040'
		##~   when tolltype_id in (6,7,12,13,106,107,112,113) then '050'
		##~   when tolltype_id in (8,9,10,11,108,109,110,111) then '051'
		##~   when tolltype_id in (99,999) then '052'
	##~   end TOLL_TYPE
	##~   ,case 
	##~   when substr(opp_number,1,2) = '00'    then '1000' 	
	##~   when substr(opp_number,1,5) = '17950' then '2101' 
	##~   when substr(opp_number,1,5) = '17951' then '2102' 
	##~   when substr(opp_number,1,5) = '12593' then '2103' 
	##~   else '2199' end	IP_TYPE
	##~   ,opp_city_id  B_AREA_CD
	##~   ,case 
		##~   when calltype_id  = 0 then '01'
		##~   when calltype_id  = 1 then '02'
		##~   when calltype_id  in (2,3) then '03' 
	##~   end  CALL_TYPE_CD
	##~   ,replace(char(date(START_TIME)),'-','') BEGIN_DT 
	##~   ,replace(char(time(START_TIME)),'.','') BEGIN_TIME
	##~   ,char(call_duration_m) BILL_DUR
	##~   ,char(call_duration) CALL_DUR
	##~   ,char(int(basecall_fee*100)) BASE_CALL_FEE
	##~   ,char(int(toll_fee*100)) TOLL_FEE
	##~   from bass2.Cdr_call_dtl_$timestamp a 
	##~   where roamtype_id in (0,1,2,4,6,7,8)
	##~   and tolltype_id in (3,4,5,103,104,105,6,7,12,13,106,107,112,113,8,9,10,11,108,109,110,111,99,999)
	##~   and calltype_id in (0,1,2,3)
  ##~   "
	##~   exec_sql $sql_buff

##~   #20110828
	##~   set sql_buff "
	##~   update (select * from G_S_04019_DAY where time_id = $timestamp) a 
	##~   set B_AREA_CD = '1209'
	##~   where time_id = $timestamp
	##~   and B_AREA_CD  in (select distinct B_AREA_CD
		##~   from G_S_04019_DAY where time_id = $timestamp
		##~   except 
		##~   select country_code from  bass1.dim_country_city_code
		##~   )
  ##~   "
	##~   exec_sql $sql_buff

  ##~   #2.检查 b_area_cd
	##~   set sql_buff "
	##~   select count(0) from (
		##~   select distinct B_AREA_CD
		##~   from G_S_04019_DAY where time_id = $timestamp
		##~   except 
		##~   select country_code from  bass1.dim_country_city_code 
		##~   ) a with ur
	            ##~   "
##~   chkzero2 $sql_buff "invalid B_AREA_CD!"

	return 0
}



proc Deal_proc04019 { op_time optime_month } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
    #上个月 yyyymm
    
            global app_name

		set app_name "G_S_04019_DAY.tcl"        

  #删除本期数据
	set sql_buff "delete from bass1.G_S_04019_DAY where time_id=$timestamp"
	exec_sql $sql_buff


#仅报维表中的
#040	港澳台
#050	国际
#051	美加日韩澳
#052	其它国家

#2100	    IP语音
#2101	17950
#2102	17951
#2103	12593
#2199	        其它

#01	主叫/移动沙龙上行	
#02	被叫/移动沙龙下行	
#03	呼转	
#

#CALLTYPE_ID	CALLTYPE_NAME
#0	主叫
#1	被叫
#2	有条件呼转
#3	无条件呼转
#4	短信发送
#5	短信接受


#ROAMTYPE_ID	ROAMTYPE_NAME
#0	本地
#1	省内漫游
#2	省际漫入
#3	国际漫入
#4	省际漫出
#5	港澳台国际漫出
#6	省内边界漫游
#7	省际边界漫入
#8	省际边界漫出
#9	非港澳台国际漫出
#100	国际漫出(国际点对点短信)
#101	国际漫入(国际点对点短信)
#

##~   20120710 add: when substr(opp_number,1,2) = '00'    then '1000' 	
	set sql_buff "
	insert into G_S_04019_DAY
	select 
	$timestamp time_id
	,a.product_no
	,value(a.imei,'0') imei
	,case 
		when tolltype_id in (3,4,5,103,104,105) then '040'
		when tolltype_id in (6,7,12,13,106,107,112,113) then '050'
		when tolltype_id in (8,9,10,11,108,109,110,111) then '051'
		when tolltype_id in (99,999) then '052'
	end TOLL_TYPE
	,case 
	when substr(opp_number,1,2) = '00'    then '1000' 	
	when substr(opp_number,1,5) = '17950' then '2101' 
	when substr(opp_number,1,5) = '17951' then '2102' 
	when substr(opp_number,1,5) = '12593' then '2103' 
	else '2199' end	IP_TYPE
	,opp_city_id  B_AREA_CD
	,case 
		when calltype_id  = 0 then '01'
		when calltype_id  = 1 then '02'
		when calltype_id  in (2,3) then '03' 
	end  CALL_TYPE_CD
	,replace(char(date(START_TIME)),'-','') BEGIN_DT 
	,replace(char(time(START_TIME)),'.','') BEGIN_TIME
	,char(call_duration_m) BILL_DUR
	,char(call_duration) CALL_DUR
	,char(int(basecall_fee*100)) BASE_CALL_FEE
	,char(int(toll_fee*100)) TOLL_FEE
	from bass2.Cdr_call_dtl_$timestamp a 
	where roamtype_id in (0,1,2,4,6,7,8)
	and tolltype_id in (3,4,5,103,104,105,6,7,12,13,106,107,112,113,8,9,10,11,108,109,110,111,99,999)
	and calltype_id in (0,1,2,3)
with ur
  "
	exec_sql $sql_buff


##~   仅报维表中的
##~   040	港澳台
##~   050	国际
##~   051	美加日韩澳
##~   052	其它国家

#来话
	set sql_buff "
	insert into G_S_04019_DAY
	select 
	$timestamp time_id
	,a.product_no
	,value(a.imei,'0') imei
	,case 
		when b.COUNTRY_CODE in ('852','853','886') then '040'
		when b.COUNTRY_CODE in ('1','61','81','82') then '051'
		else '052'
	end TOLL_TYPE
	,case 
	when substr(opp_number,1,2) = '00'    then '1000' 	
	when substr(opp_number,1,5) = '17950' then '2101' 
	when substr(opp_number,1,5) = '17951' then '2102' 
	when substr(opp_number,1,5) = '12593' then '2103' 
	else '2199' end	IP_TYPE
	,opp_city_id  B_AREA_CD
	,case 
		when calltype_id  = 0 then '01'
		when calltype_id  = 1 then '02'
		when calltype_id  in (2,3) then '03' 
	end  CALL_TYPE_CD
	,replace(char(date(START_TIME)),'-','') BEGIN_DT 
	,replace(char(time(START_TIME)),'.','') BEGIN_TIME
	,char(call_duration_m) BILL_DUR
	,char(call_duration) CALL_DUR
	,char(int(basecall_fee*100)) BASE_CALL_FEE
	,char(int(toll_fee*100)) TOLL_FEE
	from bass2.Cdr_call_dtl_$timestamp a
	left join (select * from  bass1.dim_country_city_code)	 b on char(a.opp_city_id) = b.COUNTRY_CODE
	where roamtype_id in (0,1,2,4,6,7,8)
	and substr(a.opp_number,1,2) = '00'
	and tolltype_id in (0,1,2)
	and calltype_id in (1)
with ur
"
	exec_sql $sql_buff



#20110828
	set sql_buff "
	update (select * from G_S_04019_DAY where time_id = $timestamp) a 
	set B_AREA_CD = '1209'
	where time_id = $timestamp
	and B_AREA_CD  in (select distinct B_AREA_CD
		from G_S_04019_DAY where time_id = $timestamp
		except 
		select country_code from  bass1.dim_country_city_code
		)
  "
	exec_sql $sql_buff

  #2.检查 b_area_cd
	set sql_buff "
	select count(0) from (
		select distinct B_AREA_CD
		from G_S_04019_DAY where time_id = $timestamp
		except 
		select country_code from  bass1.dim_country_city_code 
		) a with ur
	            "
chkzero2 $sql_buff "invalid B_AREA_CD!"

	return 0
}