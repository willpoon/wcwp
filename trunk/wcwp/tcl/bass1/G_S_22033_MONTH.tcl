######################################################################################################
#接口名称：SP业务量情况
#接口编码：22033
#接口说明：记录用户使用sp业务的业务量汇总信息，主要来自梦网或者其它和sp业务类型相关的详单。
#程序名称: G_S_22033_MONTH.tcl
#功能描述: 生成22033的数据
#运行粒度: 月
#源    表：1.bass2.dw_newbusi_ismg_yyyymm
#          2.bass2.dw_newbusi_call_yyyymm
#          3.bass2.dw_newbusi_wap_yyyymm
#          4.bass2.dw_newbusi_mms_yyyymm
#          5.bass2.dw_newbusi_kj_yyyymm
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.因为bass2.dw_product_func_200702没有数据，所以流失注册用户数不能统计出。
#          2.因为bass2.dw_acct_shoulditem_200703没有数据，所以功能费/包月费不能统计出。
#          3.要跟BOSS核对帐目科目后才能哪些帐目科目属于包月费/功能费。
#          4.确认好帐目科目后，完整程序
#          5.将bill_counts计费量 修改成 counts业务量  夏华学 20080523
#修改历史: 1.
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
        set db_user $env(DB_USER)
        #月接口使用
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        
        
        set handle [aidb_open $conn]
	set sql_buff "\
		DELETE FROM $db_user.G_S_22033_MONTH where time_id=${op_month}"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       
       
       
          ##---1 生成梦网相关-----#       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_22033_MONTH
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,case when svcitem_id=300007 then '16' 
                              when svcitem_id=300010 then '09' 
                              when svcitem_id=300011 then '08' 
                              when svcitem_id=300012 then '13' 
                              when svcitem_id=300013 then '17' 
                      	      when svcitem_id=300016 then '05' 
                      	      when svcitem_id=300017 then '14' 
                      	   else '01' 
                             end
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee +func_fee +month_fee else 0 end)*100))
                         ,char(count(distinct product_no))
                         ,char(int(sum(case when calltype_id=0 then counts else 0 end)))
                         ,char(int(sum(case when calltype_id=1 then counts else 0 end)))
                         ,'0'
                         ,'0'
                         ,'0'
                         ,char(int(sum(info_fee + month_fee)*100))
                         ,char(int(sum(base_fee )*100))
                         from bass2.dw_newbusi_ismg_${op_month}
                         group by
                         case when svcitem_id=300007 then '16'  
                              when svcitem_id=300010 then '09'  
                              when svcitem_id=300011 then '08'  
                              when svcitem_id=300012 then '13'  
                              when svcitem_id=300013 then '17'  
                      	      when svcitem_id=300016 then '05'  
                      	      when svcitem_id=300017 then '14'  
                      	   else '01' 
                      	   end
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
   "
        puts $sql_buff 
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        
	aidb_commit $conn

    #-----2 生成移动沙龙，语音杂志-----------#
    set handle [aidb_open $conn]

	set sql_buff "insert into G_S_22033_MONTH
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,case when svcitem_id=100001 then '07' 
                              when svcitem_id=100002 then '06' 
                              when svcitem_id=100022 then '12' 
                              end
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee + toll_fee else 0 end)*100))
                         ,char(count(distinct product_no))
                         ,char(int(sum(case when calltype_id=0 then counts else 0 end)))
                         ,char(int(sum(case when calltype_id=1 then counts else 0 end)))
                         ,'0'
                         ,'0'
                         ,'0'
                         ,char(int(sum(info_fee )*100))
                         ,char(int(sum(base_fee )*100))
                         from bass2.dw_newbusi_call_${op_month} 
                         where svcitem_id in (100001,100002,100022)
                         group by
                         case when svcitem_id=100001 then '07' 
                              when svcitem_id=100002 then '06' 
                              when svcitem_id=100022 then '12' 
                              end
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
        puts $sql_buff 
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        
	aidb_commit $conn
	
	#----3 生成wap----#
	
	set handle [aidb_open $conn]

	set sql_buff "insert into G_S_22033_MONTH
                     select
                         ${op_month}
                        ,'${op_month}'
                        ,'03' 
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee +func_fee +month_fee else 0 end)*100))
                         ,char(count(distinct product_no))
                         ,'0'
                         ,'0'
                         ,'0'
                         ,'0'
                         ,'0'
                         ,char(int(sum(info_fee + month_fee)*100))
                         ,char(int(sum(base_fee )*100))
                         from bass2.dw_newbusi_wap_${op_month}
                         group by
                          COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
        puts $sql_buff 
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        
	aidb_commit $conn	
	
	#-----4 生成彩信 -----#
	set handle [aidb_open $conn]

	set sql_buff "insert into G_S_22033_MONTH
                      select
                         ${op_month}
                         ,'${op_month}'
                         ,'02'
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee + month_fee + func_fee else 0 end)*100))
                         ,char(count(distinct product_no))
                         ,char(int(sum(case when calltype_id=0 then counts else 0 end)))
                         ,char(int(sum(case when calltype_id=1 then counts else 0 end)))
                         ,'0'
                         ,'0'
                         ,'0'
                         ,char(int(sum(info_fee + month_fee )*100))
                         ,char(int(sum(base_fee )*100))
                         from bass2.dw_newbusi_mms_${op_month} where svcitem_id in (400003,400004,400006)
                         group by
                           COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
        puts $sql_buff 
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	
	#-----5 生成百宝箱 -----#
	set handle [aidb_open $conn]

	set sql_buff "insert into G_S_22033_MONTH
                      select
                         ${op_month}
                         ,'${op_month}'
                         ,'05'
                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee + month_fee + func_fee else 0 end)*100))
                         ,char(count(distinct product_no))
                         ,'0'
                         ,char(sum(counts))
                         ,'0'
                         ,char(sum(int(data_size/1024/1024)))
                         ,char(sum(dnload_duration))
                         ,char(int(sum(info_fee + month_fee )*100))
                         ,char(int(sum(base_fee )*100))
                         from bass2.dw_newbusi_kj_${op_month} 
                         group by
                           COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
        puts $sql_buff 
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn	
	aidb_close $handle

	return 0
}
########################备份
#         ##---1 生成梦网相关-----#       
#        set handle [aidb_open $conn]
#	set sql_buff "insert into bass1.G_S_22033_MONTH
#                      select
#                         ${op_month}
#                        ,'${op_month}'
#                        ,case when svcitem_id=300007 then '16'  ---彩玲
#                              when svcitem_id=300010 then '09'  ---161移动聊天--
#                              when svcitem_id=300011 then '08'  ---移动气象站
#                              when svcitem_id=300012 then '13'  ---号簿管家
#                              when svcitem_id=300013 then '17'  ---语音短信
#                      	      when svcitem_id=300016 then '05'  ----PDA
#                      	      when svcitem_id=300017 then '14'  ----基础邮箱
#                      	   else '01'  --梦网短信
#                             end
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
#                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
#                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee +func_fee +month_fee else 0 end)*100))
#                         ,char(count(distinct product_no))
#                         ,char(int(sum(case when calltype_id=0 then counts else 0 end)))
#                         ,char(int(sum(case when calltype_id=1 then counts else 0 end)))
#                         ,'0'
#                         ,'0'
#                         ,'0'
#                         ,char(int(sum(info_fee + month_fee)*100))
#                         ,char(int(sum(base_fee )*100))
#                         from bass2.dw_newbusi_ismg_${op_month}
#                         group by
#                         case when svcitem_id=300007 then '16'  ---彩玲
#                              when svcitem_id=300010 then '09'  ---161移动聊天--
#                              when svcitem_id=300011 then '08'  ---移动气象站
#                              when svcitem_id=300012 then '13'  ---号簿管家
#                              when svcitem_id=300013 then '17'  ---语音短信
#                      	      when svcitem_id=300016 then '05'  ----PDA
#                      	      when svcitem_id=300017 then '14'  ----基础邮箱
#                      	   else '01'  --梦网短信
#                      	   end
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
#   "
#        puts $sql_buff 
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#        
#	aidb_commit $conn
#
#    #-----2 生成移动沙龙，语音杂志-----------#
#    set handle [aidb_open $conn]
#
#	set sql_buff "insert into G_S_22033_MONTH
#                      select
#                         ${op_month}
#                        ,'${op_month}'
#                        ,case when svcitem_id=100001 then '07'  ---移动沙龙
#                              when svcitem_id=100002 then '06'  ---音信互动--
#                              when svcitem_id=100022 then '12'  ---手机钱包--
#                              end
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
#                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
#                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee + toll_fee else 0 end)*100))
#                         ,char(count(distinct product_no))
#                         ,char(int(sum(case when calltype_id=0 then counts else 0 end)))
#                         ,char(int(sum(case when calltype_id=1 then counts else 0 end)))
#                         ,'0'
#                         ,'0'
#                         ,'0'
#                         ,char(int(sum(info_fee )*100))
#                         ,char(int(sum(base_fee )*100))
#                         from bass2.dw_newbusi_call_${op_month} 
#                         where svcitem_id in (100001,100002,100022)
#                         group by
#                         case when svcitem_id=100001 then '07'  ---移动沙龙
#                              when svcitem_id=100002 then '06'  ---音信互动--
#                              when svcitem_id=100022 then '12'  ---手机钱包--
#                              end
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
#        puts $sql_buff 
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#        
#	aidb_commit $conn
#	
#	#----3 生成wap----#
#	
#	set handle [aidb_open $conn]
#
#	set sql_buff "insert into G_S_22033_MONTH
#                     select
#                         ${op_month}
#                        ,'${op_month}'
#                        ,'03' ---生成wap
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
#                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
#                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee +func_fee +month_fee else 0 end)*100))
#                         ,char(count(distinct product_no))
#                         ,'0'
#                         ,'0'
#                         ,'0'
#                         ,'0'
#                         ,'0'
#                         ,char(int(sum(info_fee + month_fee)*100))
#                         ,char(int(sum(base_fee )*100))
#                         from bass2.dw_newbusi_wap_${op_month}
#                         group by
#                          COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
#        puts $sql_buff 
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#        
#	aidb_commit $conn	
#	
#	#-----4 生成彩信 -----#
#	set handle [aidb_open $conn]
#
#	set sql_buff "insert into G_S_22033_MONTH
#                      select
#                         ${op_month}
#                         ,'${op_month}'
#                         ,'02'
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
#                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
#                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee + month_fee + func_fee else 0 end)*100))
#                         ,char(count(distinct product_no))
#                         ,char(int(sum(case when calltype_id=0 then counts else 0 end)))
#                         ,char(int(sum(case when calltype_id=1 then counts else 0 end)))
#                         ,'0'
#                         ,'0'
#                         ,'0'
#                         ,char(int(sum(info_fee + month_fee )*100))
#                         ,char(int(sum(base_fee )*100))
#                         from bass2.dw_newbusi_mms_${op_month} where svcitem_id in (400003,400004,400006)
#                         group by
#                           COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
#        puts $sql_buff 
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	
#	#-----5 生成百宝箱 -----#
#	set handle [aidb_open $conn]
#
#	set sql_buff "insert into G_S_22033_MONTH
#                      select
#                         ${op_month}
#                         ,'${op_month}'
#                         ,'05'
#                         ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')
#                         ,char(count(distinct(case when bill_mark=1 then product_no end)))
#                         ,char(int(sum(case when bill_mark=1 then base_fee+info_fee + month_fee + func_fee else 0 end)*100))
#                         ,char(count(distinct product_no))
#                         ,'0'
#                         ,char(sum(counts))
#                         ,'0'
#                         ,char(sum(data_size))
#                         ,char(sum(dnload_duration))
#                         ,char(int(sum(info_fee + month_fee )*100))
#                         ,char(int(sum(base_fee )*100))
#                         from bass2.dw_newbusi_kj_${op_month} 
#                         group by
#                           COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0055',CHAR(brand_id)),'2')"
#        puts $sql_buff 
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn	
#	aidb_close $handle
#
#	return 0
#}
###############################