######################################################################################################
#接口名称：SP结算
#接口编码：05003
#接口说明：记录中国移动与中国移动服务提供商的结算信息。
#程序名称: G_S_05003_MONTH.tcl
#功能描述: 生成05003的数据
#运行粒度: 月
#源    表：1.bass2.dw_newbusi_ismg_yyyymm(移动梦网)
#          2.bass2.dim_js_rule(结算规则)
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.目前只能从移动梦网里才能关联结算规则表
#          2.SP业务类型编码无法细化到满足集团公司需要
#修改历史: 1.20090420zhanght彩铃结算业务剔除公免测试用户
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
  
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        set thismonthdays [string range $optime_month 7 8]
        puts $thismonthdays


        #删除本期数据
	set sql_buff "delete from bass1.g_s_05003_month where time_id=$op_month"
  puts $sql_buff
  exec_sql $sql_buff
       
             
	#--梦网短信结算
	set sql_buff "insert into bass1.g_s_05003_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,a.ser_code
                        ,a.sp_code
                        ,case when a.svcitem_id=300010 then '09' 
                              when a.svcitem_id=300011 then '08' 
                              when a.svcitem_id=300012 then '13' 
                              when a.svcitem_id=300013 then '17' 
                      	      when a.svcitem_id=300016 then '05' 
                      	      when a.svcitem_id=300017 then '14' 
                      	      when a.svcitem_id=400006 then '15'
                      	      when a.svcitem_id in (300001,300002,300003,300004) then '01' 
                      	   end
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')
                        ,'0'
                        ,char((
                          sum(case when b.bjh_flag='1' and a.calltype_id=1 then 1 else 0 end )-
                          sum(case when b.bjh_flag='1' and a.calltype_id=0 then 1 else 0 end ))*50) 
                        ,value(char(bigint(sum((a.info_fee + a.month_fee)*(100-b.rate)*10))),'0') 
                        ,char(bigint(sum((a.info_fee + a.month_fee)*1000)))
                      from 
                        bass2.dw_newbusi_ismg_$op_month a,
                        (select distinct sp_code,rate,bjh_flag from bass2.dim_js_rule) b
                      where 
                        a.sp_code=b.sp_code and a.bill_mark=1 and a.svcitem_id in (300001,300002,300003,300004,300010,300011,300012,300013,300016,300017)
                      group by
                        a.ser_code
                        ,a.sp_code
                        ,case when a.svcitem_id=300010 then '09' 
                              when a.svcitem_id=300011 then '08' 
                              when a.svcitem_id=300012 then '13' 
                              when a.svcitem_id=300013 then '17' 
                      	      when a.svcitem_id=300016 then '05' 
                      	      when a.svcitem_id=300017 then '14' 
                      	      when a.svcitem_id=400006 then '15'
                      	      when a.svcitem_id in (300001,300002,300003,300004) then '01' 
                      	   end
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')"
  puts $sql_buff
  exec_sql $sql_buff



	#--16	彩铃   有结算规则的
	set sql_buff "insert into bass1.g_s_05003_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,a.ser_code
                        ,a.sp_code
                        ,'16'
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')
                        ,'0'
                        ,char((
                          sum(case when b.bjh_flag='1' and a.calltype_id=1 then 1 else 0 end )-
                          sum(case when b.bjh_flag='1' and a.calltype_id=0 then 1 else 0 end ))*50) 
                        ,value(char(bigint(sum((a.info_fee + a.month_fee)*(100-b.rate)*10))) ,'0')
                        ,char(bigint(sum((a.info_fee + a.month_fee)*1000)))
                      from 
                        bass2.dw_newbusi_ismg_$op_month a,
                        (select distinct sp_code,rate,bjh_flag from bass2.dim_js_rule) b,
                        bass2.dw_product_$op_month c
                      where 
                        a.sp_code=b.sp_code and a.bill_mark=1 and a.svcitem_id in (300007)
                        and a.user_id=c.user_id
                        and c.FREE_MARK=0
                        and c.test_mark=0
                      group by
                        a.ser_code
                        ,a.sp_code
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')"
  puts $sql_buff
  exec_sql $sql_buff
  


	#--16	彩铃   没有结算规则的
	set sql_buff "insert into bass1.g_s_05003_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,a.ser_code
                        ,a.sp_code
                        ,'16'
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')
                        ,'0'
                        ,'0' 
                        ,value(char(bigint(sum((a.info_fee + a.month_fee)*850))) ,'0')
                        ,char(bigint(sum((a.info_fee + a.month_fee)*1000)))
                      from 
                        bass2.dw_newbusi_ismg_$op_month a,
                        bass2.dw_product_$op_month b
                      where 
                        a.sp_code not in (select distinct sp_code from bass2.dim_js_rule)
                        and a.bill_mark=1 and a.svcitem_id in (300007)
                        and a.user_id=b.user_id
                        and b.FREE_MARK=0
                        and b.test_mark=0
                      group by
                        a.ser_code
                        ,a.sp_code
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')"
  puts $sql_buff
  exec_sql $sql_buff





	#--百宝箱结算
	set sql_buff "insert into bass1.g_s_05003_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,a.ser_code
                        ,a.sp_code
                        ,case when a.sp_code = '600902' then '16' else '04' end
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')
                        ,'0'
                        ,'0'
                        ,char(bigint(sum((a.info_fee + a.month_fee)*15*10))) 
                        ,char(bigint(sum((a.info_fee + a.month_fee)*1000)))
                      from 
                        bass2.dw_newbusi_kj_$op_month a
                      where 
                        a.svcitem_id in (800001,800002)
                      group by
                        a.ser_code
                        ,a.sp_code
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')"
  puts $sql_buff
  exec_sql $sql_buff
	

	#--WAP结算
	set sql_buff "insert into bass1.g_s_05003_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,a.ser_code
                        ,a.sp_code
                        ,'03'
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')
                        ,'0'
                        ,'0'
                        ,char(bigint(sum((a.info_fee + a.month_fee)*15*10))) 
                        ,char(bigint(sum((a.info_fee + a.month_fee)*1000)))
                      from 
                        bass2.dw_newbusi_wap_$op_month a
                      group by
                        a.ser_code
                        ,a.sp_code
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')"
  puts $sql_buff
  exec_sql $sql_buff
	


	set sql_buff "insert into bass1.g_s_05003_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,value(a.ser_code,'')
                        ,value(a.sp_code,'')
                        ,'02'
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')
                        ,'0'
                        ,'0'
                        ,char(bigint(sum((a.info_fee + a.month_fee)*15*10))) 
                        ,char(bigint(sum((a.info_fee + a.month_fee)*1000)))
                      from 
                        bass2.dw_newbusi_mms_$op_month a
                      where
                      	sp_code is not null
                      group by
                        a.ser_code
                        ,a.sp_code
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',CHAR(a.city_id)),'13101')"

  puts $sql_buff
  exec_sql $sql_buff
					      				
		return 0
}
########reference
#(300001,'全球通梦网短信(上行话单)',3,'梦网短信',2,'数据业务');  
#(300002,'全球通梦网短信(下行话单)',3,'梦网短信',2,'数据业务');  
#(300003,'国际漫游问候短信',3,'梦网短信',2,'数据业务');          
#(300004,'梦网下发m文件',3,'梦网短信',2,'数据业务');             
#(300005,'儿基会短信业务',3,'梦网短信',2,'数据业务');            
#(300006,'本地USSD业务信息费',3,'梦网短信',2,'数据业务');        
#(300007,'彩铃下载铃声话单',3,'梦网短信',2,'数据业务');          
#(300008,'语音杂志(本地信息费)',3,'梦网短信',2,'数据业务');      
#(300009,'集团客户短信(梦网下行话单)',3,'梦网短信',2,'数据业务');
#(300010,'飞信(梦网下发m文件)',3,'梦网短信',2,'数据业务');       
#(300011,'移动气象站(梦网下行话单)',3,'梦网短信',2,'数据业务');  
#(300012,'话簿管家',3,'梦网短信',2,'数据业务');                  
#(300013,'语音短信(部分梦网下行话单)',3,'梦网短信',2,'数据业务');
#(300014,'本地USSD业务通信费',3,'梦网短信',2,'数据业务');        
#(300015,'全网USSD业务话单',3,'梦网短信',2,'数据业务');          
#(300016,'PDA业务MISC下发的话单',3,'梦网短信',2,'数据业务');     
#(300017,'手机邮箱',3,'梦网短信',2,'数据业 务');   


#06	语音杂志
#07	移动沙龙
#11	移动证券
#12	手机钱包
#15	本地基础邮箱



#01	梦网短信     1
#02	彩信
#03	WAP
#04	百宝箱       1
#05	PDA         1
#06	语音杂志
#07	移动沙龙
#08	移动气象站   1
#09	161移动聊天  1
#11	移动证券
#12	手机钱包
#13	号簿管家     1
#14	基础邮箱     1
#15	本地基础邮箱
#16	彩铃         1
#17	语音短信     1
#99	其他

#SP结算接口修改。保证百宝箱、彩信、梦网短信、语音杂志的数据质量
##################################################


#内部函数部分	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------


