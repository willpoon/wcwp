
######################################################################################################		
#接口名称: 重点自有业务收入                                                               
#接口编码：03019                                                                                          
#接口说明：重点自有业务收入是中国移动“用户”使用中国移动自有业务服务后，在指定“帐务周期”产生的费用细项信息，且经过了帐务级优惠处理。
#程序名称: G_S_03019_MONTH.tcl                                                                            
#功能描述: 生成03019的数据
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

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]      
        #上月 YYYYMM
        set last_month [GetLastMonth [string range $op_month 0 5]]
            
 
        #删除本期数据
	set sql_buff "delete from bass1.G_S_03019_MONTH where time_id=$op_month"
  exec_sql $sql_buff
  
  
	set sql_buff "
	insert into bass1.G_S_03019_MONTH
                        select
                          $op_month
                          ,a.user_id
                          ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0117',char(a.item_id)),'0901')
                          ,'$op_month'
                          ,char(bigint(sum(a.fact_fee)*100))
                          ,substr(char(bigint(sum(case when a.fav_fee >0 then a.fav_fee else 0 end)*-100)),1,8) 
                        from   bass2.dw_acct_shoulditem_$op_month a
                        inner join 
	                        (
	                         select user_id from bass2.dw_product_$op_month where userstatus_id<>0 and usertype_id in (1,2,9)
	                         except
	                         select user_id from bass2.dw_product_${last_month} where userstatus_id in (0,4,5,7,9)
	                         ) b on a.user_id=b.user_id
                        where
                        a.item_id  in (select  bigint(XZBAS_VALUE) from BASS1.ALL_DIM_LKP   where  bass1_tbid = 'BASS_STD1_0117')
                        group by
                          a.user_id                       
                          ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0117',char(a.item_id)),'0901') 
                          with ur
			  "
  exec_sql $sql_buff

	return 0
}
