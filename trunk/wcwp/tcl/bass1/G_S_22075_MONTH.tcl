
######################################################################################################		
#接口名称: 语音业务KPI                                                               
#接口编码：22075                                                                                          
#接口说明：记录语音业务月KPI信息。
#程序名称: G_S_22075_MONTH.tcl                                                                            
#功能描述: 生成22075的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110922
#问题记录：
#修改历史: 1. panzw 20110922	1.7.5 newly added
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#set optime_month 2011-08
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      


  #删除本期数据
	set sql_buff "delete from bass1.G_S_22075_MONTH where time_id=$op_month"
	exec_sql $sql_buff
	


		set sql_buff "
			INSERT INTO  BASS1.G_S_22075_MONTH
			( 
         TIME_ID
        ,OP_MONTH
        ,ROAMIN_CALL_CNT
        ,ROAMIN_CALL_DUR
        ,ROAMIN_BILL_DUR
        ,ROAMIN_OCALL_CNT
        ,ROAMIN_OCALL_DUR
        ,ROAMIN_OBILL_DUR
        ,ROAMIN_BYCALL_CNT
        ,ROAMIN_BYCALL_DUR
        ,ROAMIN_BYBILL_DUR
			)
			 

select	$op_month,'$op_month'
				    ,char(sum(case
 	                                 when OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) 
					 and  CALLTYPE_ID in (0,2,3) then CALL_COUNTS else 0 end 
					)) ROAMIN_CALL_CNT
 	                            ,char(sum(case
 	                                 when OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) 
					 and  CALLTYPE_ID in (0,2,3) then call_duration_m else 0 end 
					)) ROAMIN_CALL_DUR
 	                            ,char(sum(case
 	                                 when OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) 
					 and  CALLTYPE_ID in (0,2,3) and BASECALL_FEE+TOLL_FEE+INFO_FEE > 0 then call_duration_m else 0 end 
					)) ROAMIN_BILL_DUR
				    ,char(sum(case when OPPOSITE_ID in (3,13,115,2,1,4,116,14) and  CALLTYPE_ID in (0,2,3) then CALL_COUNTS else 0 end )) ROAMIN_OCALL_CNT
				    ,char(sum(case when OPPOSITE_ID in (3,13,115,2,1,4,116,14) and  CALLTYPE_ID in (0,2,3) then call_duration_m else 0 end )) ROAMIN_OCALL_DUR
				    ,char(sum(case when OPPOSITE_ID in (3,13,115,2,1,4,116,14) and  CALLTYPE_ID in (0,2,3) and BASECALL_FEE+TOLL_FEE+INFO_FEE > 0 then call_duration_m else 0 end )) ROAMIN_OBILL_DUR
				    ,char(sum(case
 	                                 when  CALLTYPE_ID in (1) then CALL_COUNTS else 0 end 
					)) ROAMIN_BYCALL_CNT
 	                            ,char(sum(case
 	                                 when CALLTYPE_ID in (1) then call_duration_m else 0 end 
					)) ROAMIN_BYCALL_DUR
 	                            ,char(sum(case
 	                                 when CALLTYPE_ID in (1) and BASECALL_FEE+TOLL_FEE+INFO_FEE > 0  then call_duration_m else 0 end 
					)) ROAMIN_BYCALL_DUR					
                 from bass2.DW_CALL_ROAMIN_DM_$op_month
                 where  roamtype_id in (2,7)  and TOLLTYPE_ID < 100
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_22075_MONTH 3

  #1.检查chkpkunique
	set tabname "G_S_22075_MONTH"
	set pk 			"OP_MONTH"
	chkpkunique ${tabname} ${pk} ${op_month}
	
	
	return 0
}


## 接口单元名称：语音业务KPI
## 接口单元编码： 22075
## 接口单元说明：记录语音业务月KPI信息。
## 以下指标口径说明参见 “2011年定期统计报表制度”：
## 省际漫游来访-主叫
## 省际漫游来访-被叫
## 
## 接口单元属性列表：
## 属性编码	属性名称	属性描述	属性类型	备注
## 00	记录行号	唯一标识记录在接口数据文件中的行号。	number(8)	
## 01	月份	格式：YYYYMM	char(6)	主键
#BT5301 ## 02	省际漫游来访-网内-主叫-通话次数	统计周期内外省移动用户漫游至本省时拨打移动用户的所有通话。	number(15)	单位：次
#xxxxxx ## 03	省际漫游来访-网内-主叫-通话分钟数	统计周期内外省移动用户漫游至本省时拨打移动用户的所有通话。	number(15)	单位：分钟
#BT5302 ## 04	省际漫游来访-网内-主叫-计费分钟数	统计周期内外省移动用户漫游至本省时拨打移动用户的所有通话。	number(15)	单位：分钟
#xxxxxx ## 05	省际漫游来访-网间-主叫-通话次数	统计周期内外省移动用户漫游至本省时拨打其他运营商用户的所有通话。	number(15)	单位：次
#xxxxxx ## 06	省际漫游来访-网间-主叫-通话分钟数	统计周期内外省移动用户漫游至本省时拨打其他运营商用户的所有通话。	number(15)	单位：分钟
#xxxxxx ## 07	省际漫游来访-网间-主叫-计费分钟数	统计周期内外省移动用户漫游至本省时拨打其他运营商用户的所有通话。	number(15)	单位：分钟
#xxxxxx ## 08	省际漫游来访-被叫-通话次数	统计周期内外省移动用户漫游至本省时接听的所有通话。	number(15)	单位：次
#xxxxxx ## 09	省际漫游来访-被叫-通话分钟数	统计周期内外省移动用户漫游至本省时接听的所有通话。	number(15)	单位：分钟
#xxxxxx ## 10	省际漫游来访-被叫-计费分钟数	统计周期内外省移动用户漫游至本省时接听的所有通话。	number(15)	单位：分钟
## 	0x0D0A	回车换行符		
## 

## 网内  (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) 
##  主叫 CALLTYPE_ID in (0,2,3) 
## 省际漫入 roamtype_id in (2,7)

 #bass2.DW_CALL_ROAMIN_DM_201109
# 
#select
# 	                             sum(case
# 	                                 when OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) 
#					 and  CALLTYPE_ID in (0,2,3) then CALL_COUNTS else 0 end 
#					) ROAMIN_CALL_CNT
# 	                            ,sum(case
# 	                                 when OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) 
#					 and  CALLTYPE_ID in (0,2,3) then call_duration_m else 0 end 
#					) ROAMIN_CALL_DUR
#				    ,sum(case when OPPOSITE_ID in (3,13,115,2,1,4,116,14) and  CALLTYPE_ID in (0,2,3) then CALL_COUNTS else 0 end ) ROAMIN_OCALL_CNT
#				    ,sum(case when OPPOSITE_ID in (3,13,115,2,1,4,116,14) and  CALLTYPE_ID in (0,2,3) then call_duration_m else 0 end ) ROAMIN_OCALL_DUR
#				    ,sum(case
# 	                                 when  CALLTYPE_ID in (1) then CALL_COUNTS else 0 end 
#					) ROAMIN_BYCALL_CNT
# 	                            ,sum(case
# 	                                 when CALLTYPE_ID in (1) then call_duration_m else 0 end 
#					) ROAMIN_BYCALL_DUR
#					
#                 from bass2.DW_CALL_ROAMIN_DM_201109
#                 where  roamtype_id in (2,7)  and TOLLTYPE_ID < 100
#                
#			      
#			      
#
#
#
#select
# 	                            case
# 	                                 when OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
#                                   when opposite_id in (3)           then 2
#                                   when opposite_id in (13)          then 3
#                                   when opposite_id in (115,2)       then 4
#                                   when opposite_id in (1,4,116)     then 5
#                                   when opposite_id in (14)          then 6
#                                   else 0
#                              end,
#                              case when TOLLTYPE_ID in (0) then 1
#                                   when TOLLTYPE_ID in (1) then 2
#                                   when TOLLTYPE_ID in (2) then 3
#                              else 0 end,
#                  	          sum(CALL_COUNTS),
#                  	          sum(call_duration_m),
#                  	          sum(case when mns_type = 1 then call_counts else 0 end),
#                  	          sum(case when mns_type = 1 then call_duration_m else 0 end)
#                 from bass2.DW_CALL_ROAMIN_DM_$op_month
#                 where  roamtype_id in (2,7) and CALLTYPE_ID in (0,2,3) and TOLLTYPE_ID < 100
#                 group by     case
# 	                                 when OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
#                                   when opposite_id in (3)           then 2
#                                   when opposite_id in (13)          then 3
#                                   when opposite_id in (115,2)       then 4
#                                   when opposite_id in (1,4,116)     then 5
#                                   when opposite_id in (14)          then 6
#                                   else 0
#                              end,
#                              case when TOLLTYPE_ID in (0) then 1
#                                   when TOLLTYPE_ID in (1) then 2
#                                   when TOLLTYPE_ID in (2) then 3
#                              else 0 end
			      