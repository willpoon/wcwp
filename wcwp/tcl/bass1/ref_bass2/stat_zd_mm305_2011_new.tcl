#=======================================================================================================#
#	  名称：stat_zd_mm305_2011.tcl                                                                      #
#                                                                                                   	#
#	  功能描述： 移动增值业务情况统计表                           编号:综定604,605,606,674             	#
#                                                                                                   	#
#	  编写人：AsiaInfo	heys                                  	  日期:2011年1月                    	#
#=======================================================================================================#
proc deal {p_optime p_timestamp} {

	global conn
	global handle

	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1000
		return -1
	}

	if {[stat_zd_mm305 $p_optime]  != 0} {
		aidb_roll $conn
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	aidb_close $handle

	return 0
}

proc stat_zd_mm305 {p_optime} {
	global conn
	global handle

  source  stat_insert_index.tcl
  source  report.cfg
  set     date_optime   [ai_to_date $p_optime]

  scan   $p_optime "%04s-%02s-%02s" year month day
  #set    next_month [GetNextMonth [string range $year$month 0 5]]01

	scan $p_optime "%04s-%02s-%02s" year month day
	scan $p_optime "%04d-%02d-%02d" year2 month2 day2
#=========== 09年新增指标 =========
#移动增值业务情况统计表（一）
#CU9215	发送联通网间短消息使用客户数	户
#CU9216	发送电信网间短消息使用客户数	户
#CU9230	点对点短信套餐用户数	户
#CU9231	其中：全球通	户
#CU9232	神州行 	户
#CU9233	动感地带	户
#CU9234	点对点短信套餐使用用户数	户
#CU9235	其中：全球通	户
#CU9236	神州行	户
#CU9237	动感地带	户
#CU9223	其中：全球通	户
#CU9224	神州行 	户
#CU9225	动感地带	户
#CU9226	其他用户	户
#移动增值业务情况统计表（二）
#CU9249	其中：使用TD网络的GPRS客户数
#CU9240	GPRS套餐用户数
#CU9241	其中：全球通
#CU9242	      神州行
#CU9243	动感地带
#CU9244	GPRS套餐使用用户数
#CU9245	其中：全球通
#CU9246	      神州行
#CU9247	动感地带
#CU9359	其中：使用TD网络的CMWAP客户数
#CU9360	CMNET用户数
#CU9361	其中：全球通
#CU9362	      神州行
#CU9363	      动感地带
#移动增值业务情况统计表（三）
#CU9369	其中：使用TD网络的CMNET客户数	户
#CU9370	点对点彩信套餐用户数	户
#CU9371	其中：全球通	户
#CU9372	神州行 	户
#CU9373	动感地带	户
#CU9374	点对点彩信套餐使用用户数	户
#CU9375	其中：全球通	户
#CU9376	神州行 	户
#CU9377	动感地带	户
#CU9431	手机游戏业务使用客户数	户
#CU9432	游戏业务包活跃用户数	户
#CU9910	可视电话使用客户数	户
#CU9911	多媒体彩铃注册客户数	户
#CU9912	多媒体彩铃下载客户数	户
#CU9913	视频点播使用客户数	户
#CU9914	视频共享使用客户数	户
#CU9915	视频会议预约客户数	户
#CU9916	视频留言注册客户数	户

#	=========== 初始化需要计算的简单指标值 ===========

  set   name_list1   "CU9110 CU9111 CU9120 CU9130 CU9140 CU9510 CU9210 CU9211 CU9213 CU9212 CU9214 CU9215 CU9216 CU9217 CU9230 CU9231 CU9232 CU9233 CU9234 CU9235 CU9236 CU9237 CU9277 CU9270 CU9274 CU9276 CU9275 CU9220 CU3410 CU3420 CU3430 CU3431 CU3432 CU3433 CU9180 CU9181 CU9420 CU9421 CU9331 CU9332 CU9333 CU9370 CU9371 CU9372 CU9373 CU9374 CU9375 CU9376 CU9377 CU9422 CU9334 CU9335 CU9336 CU9423 CU9337 CU9338 CU9339 CU3600 CU3601 CU3602 CU3603 CU3610 CU3611"
  set   name_list2   "CU3612 CU3613 CU3614 CU3810 CU3820 CU9340 CU9345 CU9347 CU9346 CU9249 CU9240 CU9241 CU9242 CU9243 CU9244 CU9245 CU9246 CU9247 CU9351 CU9355 CU9356 CU9357 CU9359 CU9360 CU9361 CU9362 CU9363 CU9369 CU9352 CU9478 CU9477 CU1141 CU1142 CU9479 CU9463 CU9460 CU9461 CU9462 CU9520 CU9521 CU9522 CU3510 CU9560 CU9561 CU9562 CU9540 CU3300 CU3301 CU3302 CU3303 CU3320 CU3321 CU3322 CU3323 CU3324 CU3310 CU3311 CU3312 CU3313 CU3314 CU3700 CU3710 CU3711 CU3712 CU3713 CU3720"
  set   name_list3   "CU3721 CU3722 CU3723 CU3730 CU3731 CU3732 CU3740 CU3741 CU3742 CU3743 CU3744 CU3745 CU3746 CU9260 CU9281 CU9282 CU9283 CU9263 CU9291 CU9292 CU9293 CU9971 CU9972 CU9973 CU9974 CU9975 CU9976 CU9977 CU9265 CU9285 CU9286 CU9287 CU9266 CU9267 CU9268 CU9269 CU3750 CU3751 CU3752 CU3754 CU9436 CU9433 CU9434 CU9435 CU9221 CU9222 CU9227 CU9228 CU9910 CU9911 CU9912 CU9913 CU9914 CU9915 CU9916 CU9251 CU9252 CU9253 CU9254 CU9321 CU9322 CU9323 CU9440 CU9441 CU9431 CU9442"
  set   name_list4   "CU9443 CU9444 CU9445 CU9432 CU9920 CU9882 CU9530 CU9531 CU9450 CU9451 CU9452 CU9455 CU9456 CU9457 CU9551 CU9552 CU9490 CU9491 CU9492 CU9493 CU9494 JT3450 JT3420 JT3460 CU9500 CU9501 CU9464 CU9465 CU9880 CU9881"

  foreach index_name $name_list1 {
	      set	value_list(890,$index_name)	"0"
        }
  foreach index_name $name_list2 {
	      set	value_list(890,$index_name)	"0"
	      }
  foreach index_name $name_list3 {
	      set	value_list(890,$index_name)	"0"
	      }
  foreach index_name $name_list4 {
	      set	value_list(890,$index_name)	"0"
	      }
#	=========== 日期处理 ===========
#依次为上月/下月/本月天数/截至本月末本年天数/本年天数/月份(整数)
	set sql_buf "select $date_optime - 1 month,
	                    $date_optime + 1 month,
	                    days($date_optime + 1 month) - days('$p_optime'),
	                    days($date_optime + 1 month) - days('$year-01-01'),
	                    days('$year-12-31') - days('$year-01-01')+1,
	                    int(month('$p_optime'))
	             from dual"
	puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1100
		return -1
	}
  while {[set this_row [aidb_fetch $handle]] != ""} {
	           set	last_month	      [lindex $this_row 0]
	           set	next_month	      [lindex $this_row 1]
	           set	days_of_month     [lindex $this_row 2]
	           set  days_of_untilyear [lindex $this_row 3]
	           set  days_of_year      [lindex $this_row 4]
	           set  int_month         [lindex $this_row 5]

	           puts $last_month
             puts $next_month
             puts $days_of_month
             puts $days_of_untilyear
             puts $days_of_year
             puts $int_month

	           set	tmp_date	        [lindex $this_row 0]
	           set  year1             [string range $tmp_date 0 3]
	           set  month1            [string range $tmp_date 5 6]
	}
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
#CU9110	来电显示客户到达数	    户			    305011
#CU9120	主叫号码隐藏客户到达数	户			    305015
#CU9130	语音信箱客户到达数	    户			    305018

 	set sql_buf01 "select
	                      case when a.service_id in (305011) then 1
	                           when a.service_id in (305015) then 2
	                           when a.service_id in (305018) then 3
	                      else 0 end,
				          		  count(distinct a.user_id)
				         from	DW_PRODUCT_FUNC_$year$month a , dw_product_$year$month b
				         where a.valid_date < date('$next_month')
				         and a.expire_date >= date('$next_month')
				         and b.usertype_id in (1,2,9) and a.user_id=b.user_id
				         and a.SERVICE_ID in (305011,305015,305018)
				         and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1 and a.STS in (1)
				         group by
				                 case when a.service_id in (305011) then 1
	                            when a.service_id in (305015) then 2
	                            when a.service_id in (305018) then 3
	                       else 0 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_service_id          [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

#合计
           if {$tmp_service_id == 1 } {
                   set value_list(890,CU9110)  [expr $value_list(890,CU9110) + $tmp_user_nums    ]
           }
           if {$tmp_service_id == 2 } {
                   set value_list(890,CU9120)  [expr $value_list(890,CU9120) + $tmp_user_nums    ]
           }
           if {$tmp_service_id == 3 } {
                   set value_list(890,CU9130)  [expr $value_list(890,CU9130) + $tmp_user_nums    ]
           }
#End Of It
}
#====================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#CU9111　	来电显示付费客户数	户		  501
 	set sql_buf01 "select count(distinct a.user_id)
                 from dw_acct_should_extdtl_$year$month a , dw_product_$year$month b
                 where a.fact_fee > 0
                   and a.feetype_id = 501
                   and a.user_id=b.user_id
                   and b.usertype_id in (1,2,9)"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

          set value_list(890,CU9111)  [expr $value_list(890,CU9111) + $tmp_user_nums    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
#CU9140	  呼叫转移使用客户数	    户	  100015,100016,100017,100018
#CU9150	  主叫付费客户到达数	    户	  100023                       
#CU9251	  移动沙龙使用客户数	    户	  100001
#CU9220   移动秘书客户到达数	    户	  100003
#CU9352　 17266 CSD使用客户数	    户	  100019

 	set sql_buf01 "select
				          	    case when a.SVCITEM_ID in (100015,100016,100017,100018) then 1
				          	         when a.SVCITEM_ID in (100023) then 2
				          	         when a.SVCITEM_ID in (100001) then 4
				          	         when a.SVCITEM_ID in (100003) then 5
				          	         when a.SVCITEM_ID in (100019) then 6
				          	    else 0 end,
				          		count(distinct a.user_id)
				         from	DW_NEWBUSI_CALL_$year$month a , dw_product_$year$month b
				         where b.usertype_id in (1,2,9)
				           and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1
				           and a.user_id=b.user_id
				           and a.SVCITEM_ID in (100015,100016,100017,100018,100023,100001,100003,100019)
				         group by
				               case  when a.SVCITEM_ID in (100015,100016,100017,100018) then 1
				          	         when a.SVCITEM_ID in (100023) then 2
				          	         when a.SVCITEM_ID in (100001) then 4
				          	         when a.SVCITEM_ID in (100003) then 5
				          	         when a.SVCITEM_ID in (100019) then 6
				          	   else 0 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set     tmp_service_id          [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

        if {$tmp_service_id == 1 }  {
                     set value_list(890,CU9140)    [expr $value_list(890,CU9140) + $tmp_user_nums     ]
           }
        if {$tmp_service_id == 2 }  {
                     set value_list(890,CU9150)    [expr $value_list(890,CU9150) + $tmp_user_nums     ]
           }
        if {$tmp_service_id == 4 }  {
                     set value_list(890,CU9251)    [expr $value_list(890,CU9251) + $tmp_user_nums     ]
           }
        if {$tmp_service_id == 5 }  {
                     set value_list(890,CU9220)    [expr $value_list(890,CU9220) + $tmp_user_nums     ]
           }
        if {$tmp_service_id == 6 }  {
                     set value_list(890,CU9352)    [expr $value_list(890,CU9352) + $tmp_user_nums     ]
           }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
#CU9510	会议电话使用客户数	    户			    50019

 	set sql_buf01 "select
				          		  value(count(distinct a.user_id),0)
				         from	DW_ACCT_SHOULD_EXTDTL_${year}${month} a , dw_product_$year$month b
				         where b.usertype_id in (1,2,9) and a.user_id=b.user_id
				         and a.feetype_id in (509)"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

#合计
                   set value_list(890,CU9510)  [expr $value_list(890,CU9510) + $tmp_user_nums    ]
#End Of It
}
#====================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
#CU9170	    语音短信使用客户数	     户	     100027
 	set sql_buf01 "select
                            count(distinct a.user_id) as numbs1
                 from    dw_newbusi_call_$year$month a , dw_product_$year$month b
                 where b.usertype_id in (1,2,9)
                 and a.user_id=b.user_id
                 and a.svcitem_id in (100027)
                 group by
                                b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

#		                  set value_list(890,CU9170)  [expr $value_list(890,CU9170) + $tmp_user_nums   ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
#CU9170	    语音短信使用客户数	     户	    300013
#CU9180	    移动气象站使用客户数	   户	    300011

 	set sql_buf01 "select
                            case when a.svcitem_id in (300013) then 1
                                 when a.svcitem_id in (300011) then 2
                            else 0 end,
                            count(distinct a.user_id) as numbs1
                 from    dw_newbusi_ismg_$year$month a , dw_product_$year$month b
                 where b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.svcitem_id in (300013,300011)
                 group by
                           case when a.svcitem_id in (300013) then 1
                                when a.svcitem_id in (300011) then 2
                           else 0 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_svcitem_id          [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

		    if {$tmp_svcitem_id == 1 } {
#		                  set value_list(890,CU9170)  [expr $value_list(890,CU9170) + $tmp_user_nums   ]
		       }
		    if {$tmp_svcitem_id == 2 } {
		                  set value_list(890,CU9180)  [expr $value_list(890,CU9180) + $tmp_user_nums   ]
		       }

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
	
#CU9181	集团短信使用客户数         户	

 	set sql_buf01 "select
                         count(distinct a.enterprise_id)
                 from   dw_enterprise_extsub_rela_$year$month a
                       dw_enterprise_msg_$year$month b
                 where a.enterprise_id = b.enterprise_id
                  and a.service_id = '910' "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {

		      set sms_ent  [lindex $this_row 0 ]
		      
          set value_list(890,CU9181) [expr $value_list(890,CU9181) + $sms_ent ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
	
	
#====================================移动增值业务情况统计表（一）==================================
#CU9210	  点对点短消息使用客户数	                户	200001,200002,200003,200004,200005
#CU9211	  其中：全球通	                          户
#CU9213	        神州行 	                          户
#CU9212	  　　　动感地带	                        户
#CU9214	  其中：网内点对点短消息使用客户数	      户	200004
#CU9215	        发送联通网间短消息使用客户数	    户	200005
#CU9216	        发送电信网间短消息使用客户数	    户	200001,200002
#CU9217         发送国际短消息使用客户数                200003

 	set sql_buf01 "select
                            b.crm_brand_id3,
                       count(distinct b.user_id)
                 from  dw_newbusi_sms_$year$month a , dw_product_$year$month b
                 where a.user_id=b.user_id and b.usertype_id in (1,2,9) and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1
                    and a.svcitem_id in (200001,200002,200003,200004,200005) and a.calltype_id = 0
                 group by b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id            [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                       set value_list(890,CU9210)  [expr $value_list(890,CU9210) + $tmp_user_nums    ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                          set value_list(890,CU9211)  [expr $value_list(890,CU9211) + $tmp_user_nums   ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                          set value_list(890,CU9213)  [expr $value_list(890,CU9213) + $tmp_user_nums   ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                          set value_list(890,CU9213)  [expr $value_list(890,CU9213) + $tmp_user_nums   ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                          set value_list(890,CU9212)  [expr $value_list(890,CU9212) + $tmp_user_nums   ]
                 }
          }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
  set sql_buf01 "select
                            case when a.svcitem_id=200004 then 4
                                 when a.svcitem_id=200005 then 5
                                 when a.svcitem_id=200003 then 7
                            else 6 end,
                       count(distinct b.user_id)
                 from  dw_newbusi_sms_$year$month a , dw_product_$year$month b
                 where a.user_id=b.user_id and b.usertype_id in (1,2,9) and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1
                    and a.svcitem_id in (200001,200002,200003,200004,200005) and a.calltype_id = 0
                 group by case when a.svcitem_id=200004 then 4
                               when a.svcitem_id=200005 then 5
                               when a.svcitem_id=200003 then 7
                            else 6 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_svcitem_id          [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

		      set value_list(890,CU921${tmp_svcitem_id}) [expr $value_list(890,CU921${tmp_svcitem_id}) + $tmp_user_nums   ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
#CU3300	    飞信业务客户到达数	     户	    300010
#CU3301	    其中：全球通	           户
#CU3302	          神州行	           户
#CU3303	          动感地带	         户

  	set sql_buf01 "select
                        b.crm_brand_id3,
                        count(distinct a.user_id)
                   from DW_PRODUCT_REGSP_${year}${month} a ,dw_product_${year}${month} b
                   where  a.busi_type='115' and a.SP_CODE <> '0'
                          and a.user_id=b.user_id
                   group by
                           b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id            [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                set value_list(890,CU3300)  [expr $value_list(890,CU3300) + $tmp_user_nums    ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                set value_list(890,CU3301)  [expr $value_list(890,CU3301) + $tmp_user_nums    ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                set value_list(890,CU3302)  [expr $value_list(890,CU3302) + $tmp_user_nums    ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                set value_list(890,CU3302)  [expr $value_list(890,CU3302) + $tmp_user_nums    ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                set value_list(890,CU3303)  [expr $value_list(890,CU3303) + $tmp_user_nums    ]
                 }
          }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}

##====================================移动增值业务情况统计表（一）==================================
#CU3310	    飞信业务付费客户数
#CU3311	    其中：飞信交友付费客户数

 	set sql_buf01 "select
                         count(distinct (case when a.BILL_COUNTS > 0 then a.user_id else null end))
                 from    dw_newbusi_ismg_$year$month a , dw_product_$year$month b
                 where b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.svcitem_id in (300010)"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

#合计
          set value_list(890,CU3310)  [expr $value_list(890,CU3310) + $tmp_user_nums     ]
          set value_list(890,CU3311)  [expr $value_list(890,CU3311) + $tmp_user_nums     ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
	
#CU3312	 飞信位置服务付费客户数   户
#CU3313	 飞信QQ付费客户数    户
#CU3314	 飞信泡泡付费客户数  户

 	set sql_buf01 " 
 	          select 
 	               count(distinct case when item_id in (80000505,80000689) then user_id else null end),
 	               count(distinct case when item_id in (80000176) then user_id else null end),
 	               count(distinct case when item_id in (80000688,80000687) then user_id else null end)
 	          from 
 	              dw_acct_shoulditem_${year}${month}
 	"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]
		      set     tmp_qq_nums             [lindex $this_row 1 ]
		      set     tmp_pp_nums             [lindex $this_row 2 ]

#合计
          set value_list(890,CU3310)  [expr $value_list(890,CU3310) + $tmp_user_nums + $tmp_qq_nums + $tmp_pp_nums   ]
          set value_list(890,CU3312)  [expr $value_list(890,CU3312) + $tmp_user_nums     ]
          set value_list(890,CU3313)  [expr $value_list(890,CU3313) + $tmp_qq_nums     ]
          set value_list(890,CU3314)  [expr $value_list(890,CU3314) + $tmp_pp_nums     ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}


#====================================移动增值业务情况统计表（一）==================================
#  CU3410 	来电提醒包月客户到达数           	户
 	set sql_buf01 "select
                       count(distinct user_id)
                   from dw_product_sprom_$year$month
                   where ACTIVE_MARK = 1 and SPROM_ID in (30505101,90002601,30505102,90002603)"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

#合计
          set value_list(890,CU3410)  [expr $value_list(890,CU3410) + $tmp_user_nums    ]
#End Of It
}
##===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
# CU3420	来电提醒按次使用客户数	户      100021,100025
# CU3430	来电提醒包月付费客户数	户
# CU3431	      其中：全球通	    户
# CU3432	      神州行	          户
# CU3433	      动感地带	        户
#
      set sql_buf01 "select
                        count(distinct a.user_id)
                     from DW_CALL_OPPOSITE_$year$month a,dw_product_${year}${month} b
                     where a.OPP_NUMBER = '13800891309' and a.user_id=b.user_id"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

#合计
                        set value_list(890,CU3420)  [expr $value_list(890,CU3420) + $tmp_user_nums    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
# CU3430	来电提醒包月付费客户数	户
# CU3431	      其中：全球通	    户
# CU3432	      神州行	          户
# CU3433	      动感地带	        户


  	set sql_buf01 "select
  	                      case when b.crm_brand_id3 in ($rep_global_brand_id) then 1
  	                           when b.crm_brand_id3 in ($rep_mzone_brand_id)  then 3
  	                      else 2 end,
                         count(distinct a.user_id)
                  from DW_ACCT_SHOULDITEM_${year}${month} a,dw_product_${year}${month} b
                  where a.ITEM_ID in (80000146,80000177,80000187,80000188,80000194,80000469) and a.user_id=b.user_id
                     and a.fact_fee > 0
                  group by
                          case when b.crm_brand_id3 in ($rep_global_brand_id) then 1
  	                           when b.crm_brand_id3 in ($rep_mzone_brand_id)  then 3
  	                      else 2 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set	    tmp_brand_id            [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

#合计
                   if {$tmp_brand_id == 1 } {
                            set value_list(890,CU3431)  [expr $value_list(890,CU3431) + $tmp_user_nums    ]
                      }
                   if {$tmp_brand_id == 2 } {
                            set value_list(890,CU3432)  [expr $value_list(890,CU3432) + $tmp_user_nums    ]
                      }
                   if {$tmp_brand_id == 3 } {
                            set value_list(890,CU3433)  [expr $value_list(890,CU3433) + $tmp_user_nums    ]
                      }
                   set value_list(890,CU3430)  [expr $value_list(890,CU3430) + $tmp_user_nums    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
#CU3510	PUSHMAIL业务客户到达数	    户
#CU9221	12580综合信息门户查询客户数	户
#CU9223	其中：全球通	              户
#CU9224	      神州行 	              户
#CU9225	      动感地带	            户
#CU9226	      其他用户	            户

#CU9222	   拨打12580的客户数        户     100045

 	set sql_buf01 "select
                       count(distinct user_id)
                 from
                       dw_product_regsp_$year$month
                 where busi_type = '125' and 
                      valid_date < date('$next_month')
                     and expire_date >= date('$next_month') "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

#合计
                                set value_list(890,CU3510)  [expr $value_list(890,CU3510) + $tmp_user_nums    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
	

#CU9222	拨打12580的客户数
 	set sql_buf01 "select
                       count(distinct user_id)
                 from
                       dw_newbusi_call_$year$month
                 where svcitem_id in (100045)"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

#合计
                                set value_list(890,CU9222)  [expr $value_list(890,CU9222) + $tmp_user_nums    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
#CU9252   	语音杂志业务使用客户数              	户
#CU9253   	其中：全网业务使用客户数            	户   100002
#CU9254   	　　　本地业务使用客户数            	户   100026

 	   set sql_buf01 "select
                            b.crm_brand_id3 as crm_brand_id3,
                      case
                           when a.svcitem_id  in (100002)        then 1
                           when a.svcitem_id  in (100026)        then 2
                       else 0 end as svcitem_id,
                      		count(distinct a.user_id) as numbs1
                     from	dw_newbusi_call_$year$month a , dw_product_$year$month b
                     where b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.svcitem_id in (100002,100026)
                     group by
                             b.crm_brand_id3,
                          case
                               when a.svcitem_id  in (100002)        then 1
                               when a.svcitem_id  in (100026)        then 2
                           else 0 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id            [lindex $this_row 0 ]
	        set     tmp_svcitem_id          [lindex $this_row 1 ]
		      set	    tmp_user_nums		        [lindex $this_row 2 ]


          if {$tmp_svcitem_id == 1 } {
                         set value_list(890,CU9253)  [expr $value_list(890,CU9253) + $tmp_user_nums    ]
                         set value_list(890,CU9252)  [expr $value_list(890,CU9252) + $tmp_user_nums    ]
             }
          if {$tmp_svcitem_id == 2 } {
                         set value_list(890,CU9254)  [expr $value_list(890,CU9254) + $tmp_user_nums    ]
                         set value_list(890,CU9252)  [expr $value_list(890,CU9252) + $tmp_user_nums    ]
             }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
#CU9260	    彩铃业务客户到达数	      户	   305120
#CU9281	    其中：全球通	            户
#CU9282	          神州行	            户
#CU9283	          动感地带	          户

 	set sql_buf01 "select
                            b.crm_brand_id3 as crm_brand_id3,
                            count(distinct a.user_id) as numbs1
                 from    dw_product_func_$year$month  a , dw_product_$year$month b
                 where b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.service_id in (305120)
                 and date(a.valid_date) <date('$next_month') and date(a.expire_date) >=date('$next_month')
                 and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1 and a.STS in (1)
                 group by
                            b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id            [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                set value_list(890,CU9260)  [expr $value_list(890,CU9260) + $tmp_user_nums    ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                set value_list(890,CU9281)  [expr $value_list(890,CU9281) + $tmp_user_nums   ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                set value_list(890,CU9282)  [expr $value_list(890,CU9282) + $tmp_user_nums   ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                set value_list(890,CU9282)  [expr $value_list(890,CU9282) + $tmp_user_nums   ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                set value_list(890,CU9283)  [expr $value_list(890,CU9283) + $tmp_user_nums   ]
                 }
          }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
#CU9263	彩铃业务付费客户数	      户	           300007
#CU9291	其中：全球通	            户
#CU9292	      神州行	            户
#CU9293	      动感地带	          户
#FEETYPE_ID FEETYPE_NAME
#---------- ----------------------
#       629 彩铃通信费
#       706 彩铃信息费

 	#set sql_buf01 "select
  #                        b.crm_brand_id3,
  #                        count(distinct (case when a.fact_fee > 0  then a.user_id else null end))
  #               from
  #                   dw_acct_should_extdtl_$year$month a ,dw_product_$year$month b
  #               where b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.FEETYPE_ID in (629,706)
  #               group by
  #                         b.crm_brand_id3"
 	set sql_buf01 "select
                          b.crm_brand_id3,
                          count(distinct a.user_id)
                 from (select user_id from dw_acct_shoulditem_$year$month
                       where item_id in (80000012,80000132,80000139,80000143,82000011,80000167,80000170,80000175) and fact_fee>0
                       group by user_id having sum(fact_fee)>0
                      )a,dw_product_$year$month b
                 where a.user_id=b.user_id and b.usertype_id in (1,2,9)and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1
                 group by
                           b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id            [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                set value_list(890,CU9263)  [expr $value_list(890,CU9263) + $tmp_user_nums    ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                set value_list(890,CU9291)  [expr $value_list(890,CU9291) + $tmp_user_nums   ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                set value_list(890,CU9292)  [expr $value_list(890,CU9292) + $tmp_user_nums   ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                set value_list(890,CU9292)  [expr $value_list(890,CU9292) + $tmp_user_nums   ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                set value_list(890,CU9293)  [expr $value_list(890,CU9293) + $tmp_user_nums   ]
                 }
          }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
##=====================================移动增值业务情况统计表（二）=================================
#CU9971	本月新增彩铃客户数	户	  305120

 	set sql_buf01 "select
	                      count(distinct case when a.service_id = 305120 and a.valid_date  >= $date_optime then a.user_id else null end)
				         from	dw_product_func_$year$month a , dw_product_$year$month b
				         where a.valid_date < date('$next_month') and a.valid_date >= $date_optime
				         and a.expire_date >= date('$next_month')
				         and b.usertype_id in (1,2,9) and a.user_id=b.user_id
				         and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1 and a.STS in (1)
				         and a.SERVICE_ID in (305120)"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set	    tmp_user_nums 		        [lindex $this_row 0 ]

#合计
          set value_list(890,CU9971)  [expr $value_list(890,CU9971) + $tmp_user_nums 	    ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
##=====================================移动增值业务情况统计表（二）=================================
#CU9972	上月彩铃客户到达数中本月流失客户数	户
#CU9973	其中：全球通	户
#CU9974	      神州行	户
#CU9975	      动感地带	户
#CU9976	其中：被动取消业务客户数	户
#CU9977	      主动取消业务客户数	户

 	set sql_buf01 "select  a.crm_brand_id3,
                         case when b.userstatus_id in (1,2,3,6,8) then 1
                         else 2 end,
                         count(distinct a.user_id)
                 from
                        (select
                                  a.user_id,
                                  b.crm_brand_id3
                         from    DW_PRODUCT_FUNC_$year1$month1  a , dw_product_$year1$month1 b
                         where b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.service_id in (305120)
                         and a.valid_date <date('$last_month') and a.expire_date >=date('$last_month')
                         and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1 and a.STS in (1)
                         group by
                                  a.user_id,
                                  b.crm_brand_id3
                         except
                         select
                                  a.user_id,
                                  b.crm_brand_id3
                         from    DW_PRODUCT_FUNC_$year$month  a , dw_product_$year$month b
                         where b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.service_id in (305120)
                         and a.valid_date <$date_optime and a.expire_date >=$date_optime
                         and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1 and a.STS in (1)
                         group by
                                  a.user_id,
                                  b.crm_brand_id3
                        ) a ,dw_product_$year$month b
                 where a.user_id=b.user_id
                 group by
                               a.crm_brand_id3,
                         case when b.userstatus_id in (1,2,3,6,8) then 1
                         else 2 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id            [lindex $this_row 0 ]
	        set     tmp_active_id           [lindex $this_row 1 ]
		      set	    tmp_user_nums		        [lindex $this_row 2 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                set value_list(890,CU9972)  [expr $value_list(890,CU9972) + $tmp_user_nums    ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                set value_list(890,CU9973)  [expr $value_list(890,CU9973) + $tmp_user_nums   ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                set value_list(890,CU9974)  [expr $value_list(890,CU9974) + $tmp_user_nums   ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                set value_list(890,CU9974)  [expr $value_list(890,CU9974) + $tmp_user_nums   ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                set value_list(890,CU9975)  [expr $value_list(890,CU9975) + $tmp_user_nums   ]
                 }
          }

        if {$tmp_active_id == 1 } {
                        set value_list(890,CU9976)  [expr $value_list(890,CU9976) + $tmp_user_nums   ]
           }
        if {$tmp_active_id == 2 } {
                        set value_list(890,CU9977)  [expr $value_list(890,CU9977) + $tmp_user_nums   ]
           }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
##=====================================移动增值业务情况统计表（二）=================================
#CU9265	彩铃业务下载客户数	      户	300007      a.svcitem_id=300018 and a.sp_code='600902' and a.ser_code='12530'
#CU9285	其中：全球通	            户
#CU9286	      神州行	            户
#CU9287	      动感地带	          户

 	   set sql_buf01 "select
                        b.crm_brand_id3,
                        count(distinct a.user_id)
                    from     (select user_id from dw_mr_down_cdr_dm_${year}${month} group by user_id
                              union all
                              select user_id
 	                            from DW_ACCT_SHOULDITEM_$year$month
 	                            where item_id= 80000144
 	                            group by user_id
 	                            having sum(fact_fee)>0
                    )a,dw_product_${year}${month} b
                    where a.user_id=b.user_id
                    group by
                            b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id            [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                set value_list(890,CU9265)  [expr $value_list(890,CU9265) + $tmp_user_nums    ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                set value_list(890,CU9285)  [expr $value_list(890,CU9285) + $tmp_user_nums   ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                set value_list(890,CU9286)  [expr $value_list(890,CU9286) + $tmp_user_nums   ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                set value_list(890,CU9286)  [expr $value_list(890,CU9286) + $tmp_user_nums   ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                set value_list(890,CU9287)  [expr $value_list(890,CU9287) + $tmp_user_nums   ]
                 }
          }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
##=====================================移动增值业务情况统计表（二）=================================
#CU9266	其中：12530语音下载客户数	             户
#CU9267	      12530短信下载客户数	             户
#CU9268	      WWW网站下载客户数	               户
#CU9269	      WAP下载客户数	                   户
#CU3751	      彩铃铃音盒下载客户数	           户
#CU3750	      其他方式下载客户数	             户
#CU3752	      全网音乐搜索下载客户数	         户
#全网音乐搜索下载客户数：是指用户通过短信、WAP、WWW以及音乐随身听客户端四种使用方式查找全网音乐资源,并下载彩铃的客户数。（全网音乐搜索为统一规划、统一运营的一类产品）。

 	   set sql_buf01 "select
                          count(distinct case when DOWN_MODE in ('1') then user_id else null end) as CU9266,
                          0 as CU9267,
                          count(distinct case when DOWN_MODE in ('2') then user_id else null end) as CU9268,
                          count(distinct case when DOWN_MODE in ('3') then user_id else null end) as CU9269,
                          0 as CU3751,
                          count(distinct case when DOWN_MODE in ('6') then user_id else null end) as CU3750,
                          count(distinct case when DOWN_MODE in ('2','3','4') then user_id else null end) as CU3752
                    from dw_mr_down_cdr_dm_$year$month"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums1		        [lindex $this_row 0 ]
		      set	    tmp_user_nums2		        [lindex $this_row 1 ]
		      set	    tmp_user_nums3		        [lindex $this_row 2 ]
		      set	    tmp_user_nums4		        [lindex $this_row 3 ]
		      set	    tmp_user_nums5		        [lindex $this_row 4 ]
		      set	    tmp_user_nums6		        [lindex $this_row 5 ]
		      set	    tmp_user_nums7		        [lindex $this_row 6 ]

		      set value_list(890,CU9266)  [expr $value_list(890,CU9266) + $tmp_user_nums1   ]
		      set value_list(890,CU9268)  [expr $value_list(890,CU9268) + $tmp_user_nums3   ]
		      set value_list(890,CU9269)  [expr $value_list(890,CU9269) + $tmp_user_nums4   ]
		      set value_list(890,CU3751)  [expr $value_list(890,CU3751) + $tmp_user_nums5   ]
		      set value_list(890,CU3750)  [expr $value_list(890,CU3750) + $tmp_user_nums6   ]
		      set value_list(890,CU3752)  [expr $value_list(890,CU3752) + $tmp_user_nums7   ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（二）==================================
#CU3700	       无线音乐俱乐部客户到达数	户	      800002
#CU3710	       其中：普通会员	户		              50001402 IVR（普通会员）
#CU3711	             其中：全球通	                户
#CU3712	                   神州行	                户
#CU3713	                   动感地带	              户
#CU3720	       其中：高级会员	户		              50001403 IVR（高级会员）
#CU3721	             其中：全球通	                户
#CU3722	                   神州行	                户
#CU3723	                   动感地带	              户
#CU3730	       本月新增无线音乐俱乐部客户数	      户	800002
#CU3731	       其中：普通会员	                    户
#CU3732	             高级会员	                    户

 	set sql_buf01 "select
                         case when a.sprom_id in (50001402) then 1
                              when a.sprom_id in (50001403) then 2
                         else 1 end,
                         b.crm_brand_id3,
                         count(distinct a.user_id),
                         count(distinct (case when a.valid_date >= $date_optime then a.user_id else null end))
                 from DW_PRODUCT_SPROM_$year$month a , dw_product_$year$month b
                 where a.user_id=b.user_id and a.valid_date < date('$next_month')
                    and a.expire_date >= date('$next_month') and b.usertype_id in (1,2,9)
                    and a.sprom_id in (50001402,50001403) and a.active_mark = 1 and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1
                 group by
                         b.crm_brand_id3,
                         case when a.sprom_id in (50001402) then 1
                              when a.sprom_id in (50001403) then 2
                         else 1 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set     tmp_sprom_id            [lindex $this_row 0 ]
		      set     tmp_brand_id            [lindex $this_row 1 ]
		      set	    tmp_user_nums		        [lindex $this_row 2 ]
		      set	    tmp_user_nums1		      [lindex $this_row 3 ]

#本月新增无线音乐俱乐部客户数
		      set value_list(890,CU3730)  [expr $value_list(890,CU3730) + $tmp_user_nums1    ]
		      if {$tmp_sprom_id == 1 } {
                 set value_list(890,CU3731)  [expr $value_list(890,CU3731) + $tmp_user_nums1    ]
             }
          if {$tmp_sprom_id == 2 } {
                 set value_list(890,CU3732)  [expr $value_list(890,CU3732) + $tmp_user_nums1    ]
             }

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                set value_list(890,CU3700)  [expr $value_list(890,CU3700) + $tmp_user_nums    ]
                         if {$tmp_sprom_id == 1 } {
                                set value_list(890,CU3710)  [expr $value_list(890,CU3710) + $tmp_user_nums    ]
                            }
                         if {$tmp_sprom_id == 2 } {
                                set value_list(890,CU3720)  [expr $value_list(890,CU3720) + $tmp_user_nums    ]
                            }
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                         if {$tmp_sprom_id == 1 } {
                                set value_list(890,CU3711)  [expr $value_list(890,CU3711) + $tmp_user_nums    ]
                            }
                         if {$tmp_sprom_id == 2 } {
                                set value_list(890,CU3721)  [expr $value_list(890,CU3721) + $tmp_user_nums    ]
                            }
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                         if {$tmp_sprom_id == 1 } {
                                set value_list(890,CU3712)  [expr $value_list(890,CU3712) + $tmp_user_nums    ]
                            }
                         if {$tmp_sprom_id == 2 } {
                                set value_list(890,CU3722)  [expr $value_list(890,CU3722) + $tmp_user_nums    ]
                            }
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                         if {$tmp_sprom_id == 1 } {
                                set value_list(890,CU3712)  [expr $value_list(890,CU3712) + $tmp_user_nums    ]
                            }
                         if {$tmp_sprom_id == 2 } {
                                set value_list(890,CU3722)  [expr $value_list(890,CU3722) + $tmp_user_nums    ]
                            }
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                         if {$tmp_sprom_id == 1 } {
                                set value_list(890,CU3713)  [expr $value_list(890,CU3713) + $tmp_user_nums    ]
                            }
                         if {$tmp_sprom_id == 2 } {
                                set value_list(890,CU3723)  [expr $value_list(890,CU3723) + $tmp_user_nums    ]
                            }
                 }
          }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（二）==================================
#CU3740	上月无线音乐俱乐部客户到达数中本月流失客户数	户
#CU3741	其中：被动取消业务客户数	                    户
#CU3742	       其中：普通会员	                        户
#CU3743	             高级会员	                        户
#CU3744	      主动取消业务客户数	                    户
#CU3745	       其中：普通会员	                        户
#CU3746	             高级会员	                        户

 	set sql_buf01 "select a.sprom_id,
                        case when b.userstatus_id in (1,2,3,6,8) then 1
                        else 0 end,
                        count(distinct a.user_id)
                 from
                       (select
                                case when a.sprom_id in (50001402) then 1
                                     when a.sprom_id in (50001403) then 2
                                else 1 end as sprom_id,
                                a.user_id
                        from DW_PRODUCT_SPROM_$year1$month1 a , dw_product_$year1$month1 b
                        where a.user_id=b.user_id and a.valid_date < $date_optime and a.expire_date >= $date_optime and b.usertype_id in (1,2,9)
                        and a.sprom_id in (50001402,50001403) and a.active_mark = 1 and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1
                        group by
                                case when a.sprom_id in (50001402) then 1
                                     when a.sprom_id in (50001403) then 2
                                else 1 end,
                                a.user_id
                        except
                        select
                                 case when a.sprom_id in (50001402) then 1
                                      when a.sprom_id in (50001403) then 2
                                 else 1 end as sprom_id,
                                 a.user_id
                         from DW_PRODUCT_SPROM_$year$month a , dw_product_$year$month b
                         where a.user_id=b.user_id and a.valid_date < date('$next_month') and a.expire_date >= date('$next_month') and b.usertype_id in (1,2,9)
                         and a.sprom_id in (50001402,50001403) and a.active_mark = 1
                         group by
                                 case when a.sprom_id in (50001402) then 1
                                      when a.sprom_id in (50001403) then 2
                                 else 1 end,
                                 a.user_id )a ,dw_product_$year$month b
                       where a.user_id=b.user_id
                 group by
                          a.sprom_id,
                          case when b.userstatus_id in (1,2,3,6,8) then 1
                          else 0 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set     tmp_sprom_id            [lindex $this_row 0 ]
		      set	    tmp_status_id	          [lindex $this_row 1 ]
		      set	    tmp_user_nums1		      [lindex $this_row 2 ]

		   if {$tmp_status_id == 1	} {
		              if {$tmp_sprom_id == 1 } {
		                         set     value_list(890,CU3742)  [expr $value_list(890,CU3742) + $tmp_user_nums1      ]
		                 }
		              if {$tmp_sprom_id == 2 } {
		                         set     value_list(890,CU3743)  [expr $value_list(890,CU3743) + $tmp_user_nums1      ]
		                 }
		                         set     value_list(890,CU3741)  [expr $value_list(890,CU3741) + $tmp_user_nums1      ]
		      }
		   if {$tmp_status_id == 0	} {
		              if {$tmp_sprom_id == 1 } {
		                         set     value_list(890,CU3745)  [expr $value_list(890,CU3745) + $tmp_user_nums1      ]
		                 }
		              if {$tmp_sprom_id == 2 } {
		                         set     value_list(890,CU3746)  [expr $value_list(890,CU3746) + $tmp_user_nums1      ]
		                 }
		                         set     value_list(890,CU3744)  [expr $value_list(890,CU3744) + $tmp_user_nums1      ]
		      }

		                         set     value_list(890,CU3740)  [expr $value_list(890,CU3740) + $tmp_user_nums1      ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（二）==================================
#CU9270	梦网短信使用客户数	户	300001,300002,300003,300004
#CU9274	其中：全球通	      户
#CU9276	      神州行 	      户
#CU9275	　　　动感地带	    户

 	set sql_buf01 "select
                          b.crm_brand_id3,
                          count(distinct a.user_id)
                 from
                     dw_newbusi_ismg_$year$month a ,dw_product_$year$month b
                 where b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.svcitem_id in (300001,300002,300003,300004)
                 group by
                          b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id            [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                set value_list(890,CU9270)  [expr $value_list(890,CU9270) + $tmp_user_nums    ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                set value_list(890,CU9274)  [expr $value_list(890,CU9274) + $tmp_user_nums    ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                set value_list(890,CU9276)  [expr $value_list(890,CU9276) + $tmp_user_nums    ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                set value_list(890,CU9276)  [expr $value_list(890,CU9276) + $tmp_user_nums    ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                set value_list(890,CU9275)  [expr $value_list(890,CU9275) + $tmp_user_nums    ]
                 }
          }
#End Of It
}
#====================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（二）==================================
#CU9277	信息点播使用客户数	户
#CU9340	GPRS使用客户数	    户	500001,500002,500003
#CU9345	其中：全球通	      户
#CU9347	      神州行 	      户
#CU9346	　　　动感地带	    户
#CU9249	其中：使用TD网络的GPRS客户数	户

 	set sql_buf01 "select b.crm_brand_id3, a.mns_type, count(distinct a.user_id)
                 	from DW_NEWBUSI_GPRS_$year$month a, dw_product_$year$month b
                 	where a.user_id = b.user_id
                 	and a.svcitem_id in (500001,500002,500003) and b.usertype_id in (1,2,9)
                 	group by b.crm_brand_id3, a.mns_type"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id       [lindex $this_row 0 ]
	        set     tmp_td_mark        [lindex $this_row 1 ]
		   set	 tmp_user_nums		[lindex $this_row 2 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                set value_list(890,CU9340)  [expr $value_list(890,CU9340) + $tmp_user_nums    ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                set value_list(890,CU9345)  [expr $value_list(890,CU9345) + $tmp_user_nums    ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                set value_list(890,CU9347)  [expr $value_list(890,CU9347) + $tmp_user_nums    ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                set value_list(890,CU9347)  [expr $value_list(890,CU9347) + $tmp_user_nums    ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                set value_list(890,CU9346)  [expr $value_list(890,CU9346) + $tmp_user_nums    ]
                 }
          }

#其中：TD
         foreach  td_all  {1} {
                 if {$tmp_td_mark == $td_all } {
                                set value_list(890,CU9249)  [expr $value_list(890,CU9249) + $tmp_user_nums    ]
                 }
          }

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（二）==================================
#CU9240	GPRS套餐用户数	户
#CU9241	其中：全球通	  户
#CU9242	      神州行 	  户
#CU9243	      动感地带	户
set sql_buf01   "select  b.crm_brand_id3,
                      count(distinct a.user_id)
                 from (select sprom_id,user_id from dw_product_sprom_${year}${month} a
                       where a.valid_date >= '$year-$month-$day' and a.expire_date >= '$next_month'
                          and a.active_mark = 1 group by user_id,sprom_id
                      )a,dw_product_${year}${month} b,
                      (select prod_id from dim_product_item_tmp
                       where prod_name like '%手机上网%' or prod_name like '%GPRS%' and is_prom=1
                      )c
                 where  a.user_id=b.user_id and a.sprom_id=c.prod_id
                 group by
                           b.crm_brand_id3"
	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}
	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id  	        [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,CU9240)  [expr $value_list(890,CU9240) + $tmp_user_nums     ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,CU9241)  [expr $value_list(890,CU9241) + $tmp_user_nums     ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,CU9242)  [expr $value_list(890,CU9242) + $tmp_user_nums     ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,CU9242)  [expr $value_list(890,CU9242) + $tmp_user_nums     ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,CU9243)  [expr $value_list(890,CU9243) + $tmp_user_nums     ]
                 }
          }
#End Of It
}
  	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（二）==================================
#CU9244	GPRS套餐使用用户数	户
#CU9245	其中：全球通	      户
#CU9246	      神州行 	      户
#CU9247	      动感地带	    户
  set sql_buf01   "select    b.crm_brand_id3 as crm_brand_id3,
                             count(distinct a.user_id)
                   from (select user_id,sprom_id
                         from dw_product_sprom_${year}${month} a
                         where a.valid_date < '$next_month' and a.expire_date >= '$next_month'
                            and a.active_mark = 1  group by user_id,sprom_id
                        )a, dw_product_${year}${month} b,
                        (select user_id,count(1) from dw_newbusi_gprs_${year}${month} c
                         where c.UPFLOW1+c.UPFLOW2+c.DOWNFLOW1+c.DOWNFLOW2 >0
                         group by user_id
                        )c,
                        (select prod_id from dim_product_item_tmp
                         where prod_name like '%手机上网%' or prod_name like '%GPRS%'  and is_prom=1
                        )d
                   where  a.user_id=b.user_id and a.user_id=c.user_id and a.sprom_id=d.prod_id
                   group by
                            b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}
	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id  	        [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]
#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,CU9244)  [expr $value_list(890,CU9244) + $tmp_user_nums     ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,CU9245)  [expr $value_list(890,CU9245) + $tmp_user_nums     ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,CU9246)  [expr $value_list(890,CU9246) + $tmp_user_nums     ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,CU9246)  [expr $value_list(890,CU9246) + $tmp_user_nums     ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,CU9247)  [expr $value_list(890,CU9247) + $tmp_user_nums     ]
                 }
          }
#End Of It
}
  	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（二）==================================
#CU9351	    CMWAP使用客户数	              		户	500002
#CU9355	    其中：全球通	              		户
#CU9356	          神州行	              		户
#CU9357	          动感地带	              		户
#CU9359	    其中：使用TD网络的CMWAP客户数 		户
#CU9360	    CMNET用户数            	    		户	500001
#CU9361	    其中：全球通	                    户
#CU9362	          神州行	                    户
#CU9363	          动感地带	                    户
#CU9369	    其中：使用TD网络的CMNET客户数	     户

 	set sql_buf01 "select b.crm_brand_id3, a.mns_type,
                         count(distinct (case when a.svcitem_id =500002 then a.user_id end)),
                         count(distinct (case when a.svcitem_id =500001 then a.user_id end))
                 from DW_NEWBUSI_GPRS_$year$month a, dw_product_$year$month b
                 where a.user_id = b.user_id
                 and a.svcitem_id in (500001,500002) and b.usertype_id in (1,2,9)
                 group by b.crm_brand_id3, a.mns_type"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id            [lindex $this_row 0 ]
	        set     tmp_td_mark            	[lindex $this_row 1 ]
		   set	 tmp_user_nums1          [lindex $this_row 2 ]
		   set	 tmp_user_nums2		[lindex $this_row 3 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                          set value_list(890,CU9351)  [expr $value_list(890,CU9351) + $tmp_user_nums1    ]

                          set value_list(890,CU9360)  [expr $value_list(890,CU9360) + $tmp_user_nums2    ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                          set value_list(890,CU9355)  [expr $value_list(890,CU9355) + $tmp_user_nums1    ]

                          set value_list(890,CU9361)  [expr $value_list(890,CU9361) + $tmp_user_nums2    ]
                 }
          }
#其中：神州行
          foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                          set value_list(890,CU9356)  [expr $value_list(890,CU9356) + $tmp_user_nums1    ]

                          set value_list(890,CU9362)  [expr $value_list(890,CU9362) + $tmp_user_nums2    ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                          set value_list(890,CU9356)  [expr $value_list(890,CU9356) + $tmp_user_nums1    ]

                          set value_list(890,CU9362)  [expr $value_list(890,CU9362) + $tmp_user_nums2    ]
                 }
         }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                          set value_list(890,CU9357)  [expr $value_list(890,CU9357) + $tmp_user_nums1    ]

                          set value_list(890,CU9363)  [expr $value_list(890,CU9363) + $tmp_user_nums2    ]
                 }
        }

#其中：TD
         foreach  td_all  {1} {
                 if {$tmp_td_mark == $td_all } {
                          set value_list(890,CU9359)  [expr $value_list(890,CU9359) + $tmp_user_nums1    ]

                          set value_list(890,CU9369)  [expr $value_list(890,CU9369) + $tmp_user_nums2    ]
                 }
        }

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（三）==================================
#CU9470	   随e行业务使用客户数	  户	  700
#500001    cmnet
#100020    usd
#随e行用户 700
#
# 	set sql_buf01  "select
#                        count(distinct a.user_id)
#                  from
#                  (select user_id
#                   from dw_newbusi_gprs_$year$month
#                   where  svcitem_id = 500001
#                   group by user_id
#                   union all
#                   select user_id
#                   from dw_newbusi_call_$year$month
#                   where  svcitem_id = 100020
#                   group by user_id
#                   union all
#                   select user_id
#                   from dw_product_$year$month
#                   where  usertype_id in (1,2,9) and crm_brand_id3 = 700 and userstatus_id in (1,2,3,6,8) and test_mark <> 1
#                   group by user_id
#                  ) a,dw_product_$year$month b
#                  where a.user_id=b.user_id and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1"
#
#	  puts $sql_buf01
#	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
#		trace_sql $errmsg 1301
#		return -1
#	}
#
#	while {[set this_row [aidb_fetch $handle]] != ""} {
#		      set	    tmp_user_nums		        [lindex $this_row 0 ]
#
##合计
#          foreach  all  $brandid_all   {
#                 if {$tmp_brand_id == $all } {
#                                set value_list(890,CU9470)  [expr $value_list(890,CU9470) + $tmp_user_nums    ]
#                 }
#          }
##End Of It
#}
##===================================================================================================
#	aidb_close $handle
#	if [catch {set handle [aidb_open $conn]} errmsg] {
#		trace_sql $errmsg 1302
#		return -1
#	}
#====================================移动增值业务情况统计表（三）==================================
#CU9477	其中：数据卡使用客户数	户

 	set sql_buf01 "select
                          count(distinct a.user_id)
                 from
                        dw_newbusi_gprs_$year$month a ,dw_product_$year$month b
                 where  crm_brand_id3 in (700)
                        and b.usertype_id in (1,2,9)
                        and a.user_id=b.user_id
                        and a.svcitem_id in (500001)"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

		      set value_list(890,CU9477)  [expr $value_list(890,CU9477) + $tmp_user_nums   ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（三）==================================
#CU9478	数据卡到达数	        户


 	set sql_buf01 "select
                          count(distinct user_id)
                 from dw_product_${year}${month}
                 where usertype_id in (1,2,9) and userstatus_id in (1,2,3,6,8) and crm_brand_id3 in (700)
                "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

		      set value_list(890,CU9478)  [expr $value_list(890,CU9478) + $tmp_user_nums   ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（三）==================================
# CU9460	随e行WLAN业务使用客户数	  户
# CU9479	WLAN注册客户数	          户
# 50015   wlan服务
# 4126    数据业务充值卡缴费
# 	   set sql_buf01 "select
#                           count(distinct a.user_id)
#                 from  DW_PRODUCT_FUNC_$year$month a , dw_product_$year$month b
#                 where a.valid_date < '$next_month' and a.expire_date >= '$next_month'
#                    and b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.SERVICE_ID in (50015)
#                    and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1 and a.STS in (1)"
#
#	  puts $sql_buf01
#	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
#		trace_sql $errmsg 1301
#		return -1
#	}
#
#	while {[set this_row [aidb_fetch $handle]] != ""} {
#		      set	    tmp_user_nums1		        [lindex $this_row 0 ]
#
#               set value_list(890,CU9479)  [expr $value_list(890,CU9479) + $tmp_user_nums1   ]
#
##End Of It
#}
##===================================================================================================
#	aidb_close $handle
#	if [catch {set handle [aidb_open $conn]} errmsg] {
#		trace_sql $errmsg 1302
#		return -1
#	}
   set sql_buf01 "select count(distinct user_id)
 	               from DW_PRODUCT_REGSP_${year}${month}
 	               where busi_type='6'"

	 puts $sql_buf01
	 if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	 }

	 while {[set this_row [aidb_fetch $handle]] != ""} {
		       set	    tmp_user_nums		        [lindex $this_row 0 ]

		       set value_list(890,CU9479)  [expr $value_list(890,CU9479) + $tmp_user_nums   ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#===================================================================================================
# CU9460	随e行WLAN业务使用客户数	  户
# CU9461	其中：高校WLAN使用客户数  户
   set sql_buf01 "select count(distinct user_id),
                         count(distinct case when b.product_instance_id is not null then a.user_id else null end)
 	                from DW_newbusi_wlan_${year}${month} a
 	                  left join 
					        (select distinct product_instance_id
						      from dw_product_ins_off_ins_prod_$(year)$(month)
						      where offer_id in (115000000254,115000000253,115000000252)
						      ) b on  a.user_id = b.product_instance_id"

	 puts $sql_buf01
	 if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	 }

	 while {[set this_row [aidb_fetch $handle]] != ""} {
		       set	    tmp_user_nums		        [lindex $this_row 0 ]
		       set      tmp_user_school         [lindex $this_row 1 ]

		       set value_list(890,CU9460)  [expr $value_list(890,CU9460) + $tmp_user_nums   ]
		       set value_list(890,CU9461)  [expr $value_list(890,CU9461) + $tmp_user_school   ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（三）==================================
#CU9530	      号簿管家（PIM）业务客户到达数	      户	300012
#CU9540	      精品门户（USSD信息服务）使用客户数	户	300006,300014,300015

 	set sql_buf01 "select
                          case when a.svcitem_id in (300012) then 1
                               when a.svcitem_id in (300006,300014,300015) then 2
                          else 0 end,
                          count(distinct a.user_id)
                 from
                     dw_newbusi_ismg_$year$month a ,dw_product_$year$month b
                 where b.usertype_id in (1,2,9) and a.user_id=b.user_id
                    and a.svcitem_id in (300012,300006,300014,300015)
                 group by
                          case  when a.svcitem_id in (300012) then 1
                                when a.svcitem_id in (300006,300014,300015) then 2
                          else 0 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_svcitem_id          [lindex $this_row 0  ]
		      set	    tmp_user_nums		        [lindex $this_row 1  ]

#合计
                 if {$tmp_svcitem_id == 1 } {
                             set value_list(890,CU9530)  [expr $value_list(890,CU9530) + $tmp_user_nums    ]
                    }
                 if {$tmp_svcitem_id == 2 } {
                             set value_list(890,CU9540)  [expr $value_list(890,CU9540) + $tmp_user_nums    ]
                    }

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
##====================================移动增值业务情况统计表（三）==================================
##CU9551	手机支付类业务使用用户数	户
##手机支付类业务使用用户数：当月使用了通过银行账户或话费进行远程支付的使用用户数，
##以及注册了现场支付（含通过银行账户、公交账户、其他集团行业账户等）的累计注册
##用户数之和。注：需要剔重；不含B-B方式的用户数。
#
# 	   set sql_buf01 "select count(distinct user_id)
# 	                   from dw_product_func_${year}${month} where busi_type = 140
# 	                   and  valid_date < '$next_month'
#                     and  expire_date >= '$next_month'
#                     and  STS in (1)
#                     union all
#                     "
#
#	  puts $sql_buf01
#	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
#		trace_sql $errmsg 1301
#		return -1
#	}
#
#	while {[set this_row [aidb_fetch $handle]] != ""} {
#		      set	    tmp_user_nums		        [lindex $this_row 0 ]
#
#          set value_list(890,CU9551)  [expr $value_list(890,CU9551) + $tmp_user_nums   ]
#
##End Of It
#}
##===================================================================================================
#	   aidb_close $handle
#	if [catch {set handle [aidb_open $conn]} errmsg] {
#		 trace_sql $errmsg 1302
#		 return -1
#	}
#====================================移动增值业务情况统计表（三）==================================
#CU9560	手机证券使用客户数	户		305041
#CU9561	手机证券（客户端产品）使用客户数

 	set sql_buf01 "select
                        count(distinct a.user_id)
                     from dw_newbusi_wap_${year}${month} a,dw_product_${year}${month} b
                     where a.user_id = b.user_id
                     and sp_code in ('900137','900140')"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set	    tmp_user_nums 		        [lindex $this_row 0 ]

#合计
               set value_list(890,CU9560)  [expr $value_list(890,CU9560) + $tmp_user_nums	    ]
               set value_list(890,CU9561)  [expr $value_list(890,CU9561) + $tmp_user_nums	    ]

#End Of It
}

#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
	
#CU9562	手机证券(短彩信产品)使用客户数	

 	set sql_buf01 "
 	        select count(distinct product_instance_id) 
 	        from dw_product_ins_off_ins_prod_${year}${month}
          where offer_id = 113110198228
          and valid_date < '$next_month' 
          and expire_date >= '$next_month'
          "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set	    tmp_user_nums 		        [lindex $this_row 0 ]

#合计            
               set value_list(890,CU9562)  [expr $value_list(890,CU9562) + $tmp_user_nums	    ]

#End Of It
}

#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
	
#====================================移动增值业务情况统计表（三）==================================
#CU9420	彩信（MMS）业务使用客户数	户	400001,400002,400003,400004,400005,400006
#CU9421	其中：点对点彩信	        户	400001,400002,400005
#CU9331	      其中：全球通	      户
#CU9332	              神州行   	  户
#CU9333	　　　        动感地带	  户
#CU9422	      梦网彩信	          户	400004
#CU9334	      其中：全球通	      户
#CU9335	              神州行 	    户
#CU9336	　　　        动感地带	  户
#CU9423	      邮箱彩信	          户	400003
#CU9337	      其中：全球通	      户
#CU9338	              神州行 	    户
#CU9339	　　　        动感地带	  户
#CU3610	全网彩信手机报客户到达数	户	400006
#CU3611	其中：全球通	            户
#CU3612	      神州行	            户
#CU3613	      动感地带	          户
#彩信业务使用客户数    400001,400002,400003,400004,400005,400006		129
#　　其中：点对点彩信  400001,400002,400005	                        130     上行
#　　　　　梦网彩信    400004,400006			                          131     下行
#　　　　　邮箱彩信    400003		                                    132     上行

 	set sql_buf01 "select
                         a.crm_brand_id3,
                         sum(a.numbs1),
                         sum(a.numbs2),
                         sum(a.numbs3),
                         sum(a.numbs4),
                         sum(a.numbs5)
                 from
                     (select
               	          b.crm_brand_id3 as crm_brand_id3,
               					  count(distinct a.user_id) as numbs1,
               					  0 as numbs2,
               					  0 as numbs3,
               					  0 as numbs4,
               					  0 as numbs5
               				 from	DW_NEWBUSI_MMS_$year$month a , dw_product_$year$month b
               				 where a.SEND_STATUS in (0,1,2,3) and  b.usertype_id in (1,2,9) and a.user_id=b.user_id
               				    and a.SVCITEM_ID in  (400001,400002,400005)
               				 and a.calltype_id in (0)
               				 group by
               				          	        b.crm_brand_id3
                       union all
                       select
               	            b.crm_brand_id3 as crm_brand_id3,
               				 	  0 as numbs1 ,
               				 	  count(distinct a.user_id) as numbs2,
               				 	  0 as numbs3,
               				 	  0 as numbs4,
               				     0 as numbs5
               				 from	DW_NEWBUSI_MMS_$year$month a , dw_product_$year$month b
               				 where a.SEND_STATUS in (0,1,2,3) and b.usertype_id in (1,2,9) and a.user_id=b.user_id
               				    and a.SVCITEM_ID in  (400004)
               				 group by
               				          	        b.crm_brand_id3
                       union all
                       select
               	            b.crm_brand_id3 as crm_brand_id3,
               				 	    0 as numbs1,
               				 	    0 as numbs2,
               				 	    count(distinct a.user_id) as numbs3,
               				 	    0 as numbs4,
               				      0 as numbs5
               				 from	DW_NEWBUSI_MMS_$year$month a , dw_product_$year$month b
               				 where a.SEND_STATUS in (0,1,2,3) and b.usertype_id in (1,2,9) and a.user_id=b.user_id
               				    and a.SVCITEM_ID in  (400003) and a.calltype_id in (0)
               				 group by
               				         b.crm_brand_id3
               				 union all
               				 select    b.crm_brand_id3 as crm_brand_id3,
               				           0 as numbs1,
               				           0 as numbs2,
               				           0 as numbs3,
               				        	 count(distinct a.user_id) as numbs4,
               				           0 as numbs5
                      from  DW_PRODUCT_REGSP_${year}${month}  a , dw_product_${year}${month} b
                      where  a.user_id=b.user_id and a.sp_code='801234'
                      group by
                                b.crm_brand_id3
                     )a
               group by
                         a.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id  		      [lindex $this_row 0 ]
	        set     tmp_user_nums1 		      [lindex $this_row 1 ]
	        set     tmp_user_nums2 		      [lindex $this_row 2 ]
	        set     tmp_user_nums3 		      [lindex $this_row 3 ]
	        set     tmp_user_nums4 		      [lindex $this_row 4 ]
	        set     tmp_user_nums5 		      [lindex $this_row 5 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                      set value_list(890,CU9421)  [expr $value_list(890,CU9421) + $tmp_user_nums1    ]
                      set value_list(890,CU9422)  [expr $value_list(890,CU9422) + $tmp_user_nums2    ]
                      set value_list(890,CU9423)  [expr $value_list(890,CU9423) + $tmp_user_nums3    ]
                      set value_list(890,CU3610)  [expr $value_list(890,CU3610) + $tmp_user_nums4    ]
#                     set value_list(890,CU9420)  [expr $value_list(890,CU9420) + $tmp_user_nums5    ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                      set value_list(890,CU9331)  [expr $value_list(890,CU9331) + $tmp_user_nums1    ]
                      set value_list(890,CU9334)  [expr $value_list(890,CU9334) + $tmp_user_nums2    ]
                      set value_list(890,CU9337)  [expr $value_list(890,CU9337) + $tmp_user_nums3    ]
                      set value_list(890,CU3611)  [expr $value_list(890,CU3611) + $tmp_user_nums4    ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                      set value_list(890,CU9332)  [expr $value_list(890,CU9332) + $tmp_user_nums1    ]
                      set value_list(890,CU9335)  [expr $value_list(890,CU9335) + $tmp_user_nums2    ]
                      set value_list(890,CU9338)  [expr $value_list(890,CU9338) + $tmp_user_nums3    ]
                      set value_list(890,CU3612)  [expr $value_list(890,CU3612) + $tmp_user_nums4    ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                      set value_list(890,CU9332)  [expr $value_list(890,CU9332) + $tmp_user_nums1    ]
                      set value_list(890,CU9335)  [expr $value_list(890,CU9335) + $tmp_user_nums2    ]
                      set value_list(890,CU9338)  [expr $value_list(890,CU9338) + $tmp_user_nums3    ]
                      set value_list(890,CU3612)  [expr $value_list(890,CU3612) + $tmp_user_nums4    ]
                    }
          }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                      set value_list(890,CU9333)  [expr $value_list(890,CU9333) + $tmp_user_nums1    ]
                      set value_list(890,CU9336)  [expr $value_list(890,CU9336) + $tmp_user_nums2    ]
                      set value_list(890,CU9339)  [expr $value_list(890,CU9339) + $tmp_user_nums3    ]
                      set value_list(890,CU3613)  [expr $value_list(890,CU3613) + $tmp_user_nums4    ]
                 }
          }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（三）==================================
#CU9420	彩信（MMS）业务使用客户数	户	400001,400002,400003,400004,400005,400006

 	set sql_buf01 "select
                         a.crm_brand_id3,
                         count(distinct a.user_id)
                 from
                     (select
               	                      b.crm_brand_id3 as crm_brand_id3,
               				          		  a.user_id as user_id
               				          	from	DW_NEWBUSI_MMS_$year$month a , dw_product_$year$month b
               				            where a.SEND_STATUS in (0,1,2,3) and  b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.SVCITEM_ID in  (400001,400002,400003,400005)
               				            and a.calltype_id in (0)
               				          	group by
               				          	        b.crm_brand_id3,
               				          	        a.user_id
                       union all
                       select
               	                      b.crm_brand_id3 as crm_brand_id3,
               				          		  a.user_id as user_id
               				          	from	DW_NEWBUSI_MMS_$year$month a , dw_product_$year$month b
               				            where a.SEND_STATUS in (0,1,2,3) and b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.SVCITEM_ID in  (400004)
               				          	group by
               				          	        b.crm_brand_id3 ,
               				          	        a.user_id
               				 union all
               				  select
               	                      b.crm_brand_id3 as crm_brand_id3,
               				          		  a.user_id  as user_id
               				          	from	DW_NEWBUSI_MMS_$year$month a , dw_product_$year$month b
               				            where a.SEND_STATUS in (0,1,2,3) and b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.SVCITEM_ID in  (400006)
               				            and a.calltype_id in (1)
               				          	group by
               				          	        b.crm_brand_id3,
               				          	        a.user_id
                     )a
               group by
                         a.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id  		      [lindex $this_row 0 ]
	        set     tmp_user_nums1 		      [lindex $this_row 1 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,CU9420)  [expr $value_list(890,CU9420) + $tmp_user_nums1    ]
                 }
          }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（三）==================================
#CU3810	    全网彩信手机报付费客户数	         户	a.SVCITEM_ID = 400006 and a.BILL_COUNTS>0
#CU3820	    其中："新闻早晚报"产品付费客户数   户	 oper_code = '110301'112335    “801234”

 	set sql_buf01 "select
                        b.crm_brand_id3,
                        count(distinct a.user_id),
                        count(distinct case when a.oper_code in ('110301','112335') and SP_CODE = '801234' then a.user_id else null end)
                 from DW_NEWBUSI_MMS_$year$month a , dw_product_$year$month b
                 where a.SEND_STATUS in (0,1,2,3) and a.user_id = b.user_id and a.SVCITEM_ID = 400006 and a.BILL_COUNTS>0
                 group by
                        b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id            [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]
		      set	    tmp_user_nums1		      [lindex $this_row 2 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,CU3810)  [expr $value_list(890,CU3810) + $tmp_user_nums     ]
                                         set value_list(890,CU3820)  [expr $value_list(890,CU3820) + $tmp_user_nums1    ]
                 }
          }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（三）==================================
#CU9440	手机游戏付费客户数
#CU9441	手机游戏使用客户数
#CU9431	手机游戏（自有）收费使用客户数

	set sql_buf01 "     select count(distinct a.user_id)
						from 
						(select user_id
                         from dw_acct_should_dtl_${year}${month}
						 where feetype_id = 647
						 group by user_id
						 having sum(fact_fee)>0 ) a,
						 dw_product_${year}${month} b
						 where  b.userstatus_Id in (1,2,3,6,8) and b.test_mark<>1 and b.usertype_Id in (1,2,9)
						     and a.user_id = b.user_id "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      	set	    tmp_user_nums1		        [lindex $this_row 0 ]

          		set value_list(890,CU9441)  [expr $value_list(890,CU9441) + $tmp_user_nums1    ]
          		set value_list(890,CU9440)  [expr $value_list(890,CU9440) + $tmp_user_nums1    ]
          		set value_list(890,CU9431)  [expr $value_list(890,CU9431) + $tmp_user_nums1    ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}  
	
#CU9442	手机游戏包客户到达数  户
#CU9444	手机游戏包新增订购客户数 户
#CU9445	手机游戏包退订客户数     户   
 

	set sql_buf01 "     
	               select count(distinct case when valid_date<'$next_month' and expire_date>='$next_month' then product_instance_id else null end),
                        count(distinct case when valid_date>='$year-$month-$day' and valid_date <'$next_month' and expire_date > '$next_month' then product_instance_id else null end ) ,
                        count(distinct case when expire_date>='$year-$mont-$day' and expire_date < '$next_month' then product_instance_id else null end  )
	               from dw_product_ins_off_ins_prod_${year}${month}
	               where offer_id in (select product_item_id from dim_prod_up_product_item
                                    where item_type = 'OFFER_PLAN'
                                    and name like '%游戏%包%')
                            
	 "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      	set	    tmp_user_nums1		        [lindex $this_row 0 ]
		      	set     tmp_user_nums2            [lindex $this_row 1 ]
		      	set     tmp_user_nums3            [lindex $this_row 2 ]

          		set value_list(890,CU9442)  [expr $value_list(890,CU9442) + $tmp_user_nums1    ]
          		set value_list(890,CU9444)  [expr $value_list(890,CU9444) + $tmp_user_nums2    ]
          		set value_list(890,CU9445)  [expr $value_list(890,CU9445) + $tmp_user_nums3    ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}  
	
#CU9450	手机地图使用客户数   户   

	set sql_buf01 "     
	               select count(distinct case when valid_date<'$next_month' and expire_date>='$next_month' then product_instance_id else null end)                       
	               from dw_product_ins_off_ins_prod_${year}${month}
	               where offer_id in (select product_item_id from dim_prod_up_product_item
                                    where item_type = 'OFFER_PLAN'
                                    and name like '%手机地图%')   "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      	set	    tmp_user_nums1		        [lindex $this_row 0 ]

          		set value_list(890,CU9450)  [expr $value_list(890,CU9450) + $tmp_user_nums1    ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}  
	
#CU9455	手机导航使用客户数                户
#CU9456	其中：手机导航包月使用客户数      户
#CU9457	其中：手机导航包次使用客户数      户
#CU9490	宜居通客户到达数                  户

	set sql_buf01 "     
	               select count(distinct case when a.valid_date<'$next_month' and a.expire_date>='$next_month' and b.name like '%手机导航%' then a.product_instance_id else null end ),
                       count(distinct case when a.valid_date<'$next_month' and a.expire_date>='$next_month' and b.name like '%手机导航%包月%' then a.product_instance_id else null end ),  
                       count(distinct case when a.valid_date<'$next_month' and a.expire_date>='$next_month' and b.name like '%手机导航%次%' then a.product_instance_id else null end ) ,
                       count(distinct case when a.valid_date<'$next_month' and a.expire_date>='$next_month' and b.name like '%宜居通%' then a.product_instance_id else null end )                 
	               from dw_product_ins_off_ins_prod_$year$month a,
				              dim_prod_up_product_item b
                where b.item_type = 'OFFER_PLAN'
                  and a.offer_id = b.product_item_id "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      	set	    tmp_user_nums1		        [lindex $this_row 0 ]
		      	set	    tmp_user_nums2		        [lindex $this_row 1 ]
		      	set	    tmp_user_nums3		        [lindex $this_row 2 ]
		      	set	    tmp_user_nums4		        [lindex $this_row 3 ]

          		set value_list(890,CU9455)  [expr $value_list(890,CU9455) + $tmp_user_nums1    ]    
          		set value_list(890,CU9456)  [expr $value_list(890,CU9456) + $tmp_user_nums2    ]
          		set value_list(890,CU9457)  [expr $value_list(890,CU9457) + $tmp_user_nums3    ]
          		set value_list(890,CU9490)  [expr $value_list(890,CU9490) + $tmp_user_nums4    ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	} 
	
#CU9491	宜居通使用客户数 户
 
	set sql_buf01 "     
	               select count(distinct user_id) 
	               from dw_acct_shoulditem_$year$month
                 where item_id in (80000648,80000649 )  "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      	set	    tmp_user_nums1		        [lindex $this_row 0 ]

          		set value_list(890,CU9491)  [expr $value_list(890,CU9491) + $tmp_user_nums1    ]    

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	} 



#CU9432	游戏业务包活跃用户数	户

#====================================移动增值业务情况统计表（三）==================================
#CU9520	      手机邮箱客户到达数	户	      300017

# 	set sql_buf01 "select
#                          count(distinct a.user_id)
#                 from
#                     dw_newbusi_ismg_$year$month a ,dw_product_$year$month b
#                 where b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.svcitem_id in (300017)"
    
    set sql_buf01 "select
                          count(distinct a.user_id)
                   from
                       dw_product_regsp_$year$month a ,dw_product_$year$month b
                   where b.usertype_id in (1,2,9) and a.user_id=b.user_id and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1
                        and a.busi_type='130' and date(b.expire_date)>('$next_month') and sp_code='931067'
				                 
				       "

    
	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

                         set value_list(890,CU9520)  [expr $value_list(890,CU9520) + $tmp_user_nums    ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（二）==================================
#CU9266	其中：12530语音下载客户数	             户
#CU3751	      彩铃铃音盒下载客户数	           户

 	   set sql_buf01 "select count(distinct user_id)
 	                  from DW_ACCT_SHOULDITEM_$year$month
 	                  where item_id= 80000144
 	                  having sum(fact_fee)>0"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums1		        [lindex $this_row 0 ]

		          set value_list(890,CU3751)  [expr $value_list(890,CU3751) + $tmp_user_nums1   ]
              set value_list(890,CU3750)  [expr $value_list(890,CU3750) + $tmp_user_nums1   ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（二）==================================
#CU9267	      12530短信下载客户数	户	      300007

 	set sql_buf01 "select
 	                    count(distinct a.user_id)
 	          from
 	         (select
                         user_id as user_id
                 from
                     dw_newbusi_ismg_$year$month a
                 where (svcitem_id in (300007) or (sp_code='600902' and ser_code='12530'))
                 group by
                         user_id
            union all
            select
                         user_id as user_id
                    from dw_mr_down_cdr_dm_$year$month
                    where DOWN_MODE in ('4')
            group by user_id)a"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row  0 ]

                         set value_list(890,CU9267)  [expr $value_list(890,CU9267) + $tmp_user_nums    ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}


##====================================其中：12530语音下载客户数=====================================
#CU3752	      全网音乐搜索下载客户数	         户
#全网音乐搜索下载客户数：是指用户通过短信、WAP、WWW以及音乐随身听客户端四种使用方式查找全网音乐资源,并下载彩铃的客户数。（全网音乐搜索为统一规划、统一运营的一类产品）。
#create table bass2.dw_mr_down_cdr_dm_yyyymm
#(
#    op_time          date,
#    user_id          varchar(12),      --服务编号
#    product_no       varchar(15),      --手机号码
#    down_time        timestamp,        --下载时间
#    down_fee         decimal(10,2),    --下载sp铃音费用(分)
#    ring_id          varchar(30),      --铃音id
#    sp_code          varchar(20),      --SP企业代码
#    ring_type        bigint,           --铃音类型编码(1:铃音,2:铃音盒)
#    pres_product_no  varchar(15),      --赠送号码
#    down_mode        varchar(2)        --下载途径(1:语音,2:Web,3:WAP,4:短信,5:USSD,6:其他/系统)

 	   set sql_buf01 "select
 	                        count(distinct a.user_id)
 	                   from
 	                  (select
                          user_id as user_id
                    from dw_mr_down_cdr_dm_$year$month
                    where DOWN_MODE in ('2','3','4')
                    group by user_id
                    union all
                    select
                           user_id as user_id
                    from dw_newbusi_kj_$year$month
                    where SERVICE_ID= 50017 and DRTYPE_ID = 60301
                    group by user_id)a"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums1		        [lindex $this_row 0 ]

		          set value_list(890,CU3752)  [expr $value_list(890,CU3752) + $tmp_user_nums1   ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}

#===================================================================================================
#CU3600	彩信手机报	              户  SP代码     =“801234”AND
#CU3601	其中：全球通	            户
#CU3602	             神州行	      户
#CU3603	             动感地带	    户

 	   set sql_buf01   "select    b.crm_brand_id3 as crm_brand_id3,
                                count(distinct a.user_id)
                      from  DW_PRODUCT_REGSP_${year}${month}  a , dw_product_${year}${month} b
                      where  a.user_id=b.user_id and a.sp_code='801234'
                      group by
                                b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id  	        [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

                            #合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,CU3600)  [expr $value_list(890,CU3600) + $tmp_user_nums     ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,CU3601)  [expr $value_list(890,CU3601) + $tmp_user_nums     ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,CU3602)  [expr $value_list(890,CU3602) + $tmp_user_nums     ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,CU3602)  [expr $value_list(890,CU3602) + $tmp_user_nums     ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,CU3603)  [expr $value_list(890,CU3603) + $tmp_user_nums     ]
                 }
          }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
##==============================================2009_NEW============================================
#CU9214	其中：网内点对点短消息使用客户数	户
#CU9215	 发送联通网间短消息使用客户数 	  户
#CU9216	 发送电信网间短消息使用客户数 	  户
#
#CU9230	点对点短信套餐用户数	            户
#CU9231	其中：全球通	                    户
#CU9232	      神州行         	            户
#CU9233	　　　动感地带	                  户
#===================================================================================================
 	   set sql_buf01   "select    b.crm_brand_id3 as crm_brand_id3,
                                count(distinct a.user_id)
                      from  (select sprom_id,user_id from dw_product_sprom_${year}${month} a
                             where a.valid_date >= '$year-$month-$day' and a.expire_date >= '$next_month'
                                and a.active_mark = 1 group by user_id,sprom_id
                            )a,dw_product_${year}${month} b,
                            (select prod_id from dim_product_item_tmp where prod_name like '%短信%' and is_prom=1)c
                      where  a.user_id=b.user_id and a.sprom_id=c.prod_id
                      group by
                                b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id  	        [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

                            #合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,CU9230)  [expr $value_list(890,CU9230) + $tmp_user_nums     ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,CU9231)  [expr $value_list(890,CU9231) + $tmp_user_nums     ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,CU9232)  [expr $value_list(890,CU9232) + $tmp_user_nums     ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,CU9232)  [expr $value_list(890,CU9232) + $tmp_user_nums     ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,CU9233)  [expr $value_list(890,CU9233) + $tmp_user_nums     ]
                 }
          }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#===================================================================================================
#CU9234	点对点短信套餐使用用户数	        户
#CU9235	其中：全球通	                    户
#CU9236	      神州行                      户
#CU9237	　　　动感地带	                  户
 	   set sql_buf01   "select    b.crm_brand_id3 as crm_brand_id3,
                                count(distinct a.user_id)
                      from (select user_id,sprom_id from dw_product_sprom_${year}${month} a
                            where a.valid_date < '$next_month' and a.expire_date >= '$next_month'
                               and a.active_mark = 1 group by user_id,sprom_id
                           )a,dw_product_${year}${month} b,
                           (select user_id,count(1) from dw_newbusi_sms_${year}${month}
                            where calltype_id in (0) and svcitem_id in (200001,200002,200003,200004,200005)
                            group by user_id
                           )c,
                           (select prod_id from dim_product_item_tmp where PROd_NAME like '%短信%' and is_prom=1
                           )d
                      where  a.user_id=b.user_id and a.user_id=c.user_id and a.sprom_id=d.prod_id
                      group by
                                b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id  	        [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,CU9234)  [expr $value_list(890,CU9234) + $tmp_user_nums     ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,CU9235)  [expr $value_list(890,CU9235) + $tmp_user_nums     ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,CU9236)  [expr $value_list(890,CU9236) + $tmp_user_nums     ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,CU9236)  [expr $value_list(890,CU9236) + $tmp_user_nums     ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,CU9237)  [expr $value_list(890,CU9237) + $tmp_user_nums     ]
                 }
          }
#End Of It
}
	  aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（三）==================================
#CU9370	其中：点对点彩信套餐用户数	      户
#CU9371	其中：全球通	                    户
#CU9372	      神州行   	                  户
#CU9373	　　　动感地带	                  户
  set sql_buf01   "select    b.crm_brand_id3,
                             count(distinct a.user_id)
                      from  (select sprom_id,user_id from dw_product_sprom_${year}${month} a
                             where a.valid_date >= '$year-$month-$day' and a.expire_date >= '$next_month'
                                and a.active_mark = 1 group by user_id,sprom_id
                            )a,dw_product_${year}${month} b,
                            (select prod_id from dim_product_item_tmp where PROd_NAME like '%彩信%' and is_prom=1)c
                      where  a.user_id=b.user_id and a.sprom_id=c.prod_id
                      group by
                             b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}
	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id  	        [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]
#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,CU9370)  [expr $value_list(890,CU9370) + $tmp_user_nums     ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,CU9371)  [expr $value_list(890,CU9371) + $tmp_user_nums     ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,CU9372)  [expr $value_list(890,CU9372) + $tmp_user_nums     ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,CU9372)  [expr $value_list(890,CU9372) + $tmp_user_nums     ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,CU9373)  [expr $value_list(890,CU9373) + $tmp_user_nums     ]
                 }
          }
#End Of It
}
	  aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（三）==================================
#CU9374	点对点彩信套餐使用用户数	        户
#CU9375	其中：全球通	                    户
#CU9376	      神州行 	                    户
#CU9377	　　　动感地带	                  户
   set sql_buf01   "select    b.crm_brand_id3,
                         count(distinct a.user_id)
                    from (select user_id,sprom_id from dw_product_sprom_${year}${month} a
                          where a.valid_date < '$next_month' and a.expire_date >= '$next_month'
                             and a.active_mark = 1
                         )a,dw_product_${year}${month} b,
                         (select user_id,count(1) from dw_newbusi_mms_${year}${month} c
                          where c.calltype_id=0 and c.svcitem_id in (400001,400002)
                          group by user_id
                         )c,
                         (select prod_id from dim_product_item_tmp where PROd_NAME like '%彩信%' and is_prom=1)d
                    where  a.user_id=b.user_id and a.user_id=c.user_id and a.sprom_id=d.prod_id
                    group by
                         b.crm_brand_id3"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}
	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id  	        [lindex $this_row 0 ]
		      set	    tmp_user_nums		        [lindex $this_row 1 ]
#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,CU9374)  [expr $value_list(890,CU9374) + $tmp_user_nums     ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,CU9375)  [expr $value_list(890,CU9375) + $tmp_user_nums     ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,CU9376)  [expr $value_list(890,CU9376) + $tmp_user_nums     ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,CU9376)  [expr $value_list(890,CU9376) + $tmp_user_nums     ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,CU9377)  [expr $value_list(890,CU9377) + $tmp_user_nums     ]
                 }
          }
#End Of It
}
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}

#====================================移动增值业务情况统计表（三）==================================
#CU9910	可视电话使用客户数	户
   set sql_buf01   "select count(distinct user_id)
                    from dw_product_td_$year$month
                    where td_video_mark=1"

	 puts $sql_buf01
	 if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	 	trace_sql $errmsg 1301
	 	return -1
	 }
	 while {[set this_row [aidb_fetch $handle]] != ""} {
	 	      set	    tmp_user_nums  	    [lindex $this_row 0 ]

          set value_list(890,CU9910)  [expr $value_list(890,CU9910) + $tmp_user_nums     ]
   #End Of It
   }
	 aidb_close $handle
	 if [catch {set handle [aidb_open $conn]} errmsg] {
	 	trace_sql $errmsg 1302
	 	return -1
	 }
	 
#CU9911	多媒体彩铃注册客户数  户
#CU9916	视频留言注册客户数   户


   set sql_buf01   "select count(distinct case when busi_type = '705' then  user_id else null end) ,
                           count(distinct case when busi_type = '707' then  user_id else null end)
                   from bass2.dw_product_regsp_${year}${month}
                   where  valid_date < '$next_month'
                    and  expire_date >= '$next_month'"

	 puts $sql_buf01
	 if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	 	trace_sql $errmsg 1301
	 	return -1
	 }
	 while {[set this_row [aidb_fetch $handle]] != ""} {
	 	      set	    tmp_user_nums  	    [lindex $this_row 0 ]
	 	      set	    tmp_707_nums  	    [lindex $this_row 1 ]

          set value_list(890,CU9911)  [expr $value_list(890,CU9911) + $tmp_user_nums     ]
          set value_list(890,CU9916)  [expr $value_list(890,CU9916) + $tmp_707_nums     ]
   #End Of It
   }
	 aidb_close $handle
	 if [catch {set handle [aidb_open $conn]} errmsg] {
	 	trace_sql $errmsg 1302
	 	return -1
	 }

#CU9913	视频点播使用客户数 户
   set sql_buf01   "
          select count(distinct product_instance_id)
 	     from dw_product_ins_off_ins_prod_${year}${month}
          where   valid_date < '$next_month'
                    and  expire_date >= '$next_month'
		             and offer_id in (select product_item_id from dim_prod_up_product_item
                                                  where name like '%视频点播%'
                                                   and item_type = 'OFFER_PLAN' )"

	 puts $sql_buf01
	 if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	 	trace_sql $errmsg 1301
	 	return -1
	 }
	 while {[set this_row [aidb_fetch $handle]] != ""} {
	 	      set	    tmp_user_nums  	    [lindex $this_row 0 ]

          set value_list(890,CU9913)  [expr $value_list(890,CU9913) + $tmp_user_nums     ]
   #End Of It
   }
	 aidb_close $handle
	 if [catch {set handle [aidb_open $conn]} errmsg] {
	 	trace_sql $errmsg 1302
	 	return -1
	 }

#CU9914	视频共享使用客户数 户
#CU9915	视频会议预约客户数 户

   set sql_buf01   "
          select 
                 count(distinct case when offer_id in (111000000704) then product_instance_id else null end),
                 count(distinct case when offer_id in (111099001688) then product_instance_id else null end)
 	     from dw_product_ins_off_ins_prod_${year}${month}
          where   valid_date < '$next_month'
                    and  expire_date >= '$next_month'
		              "

	 puts $sql_buf01
	 if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	 	trace_sql $errmsg 1301
	 	return -1
	 }
	 while {[set this_row [aidb_fetch $handle]] != ""} {
	 	      set	    tmp_user_nums  	    [lindex $this_row 0 ]
	 	      set     tmp_915_nums        [lindex $this_row 1 ]

          set value_list(890,CU9914)  [expr $value_list(890,CU9914) + $tmp_user_nums     ]
          set value_list(890,CU9915)  [expr $value_list(890,CU9915) + $tmp_915_nums    ]
   #End Of It
   }
	 aidb_close $handle
	 if [catch {set handle [aidb_open $conn]} errmsg] {
	 	trace_sql $errmsg 1302
	 	return -1
	 }


#2010——new
#====================================移动增值业务情况统计表（一）==================================
 #CU3324  飞信客户端活跃客户数	     户   

 	set sql_buf01 " select  count(distinct a.user_id)
                  from dw_newbusi_flysms_login_dm_$year$month a ,dw_product_$year$month b
                  where a.user_id=b.user_id and b.usertype_id in (1,2,9)
                        and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1 and a.optype='01'
                        and a.clienttype in ('1101','1199','1201','1202','1203','1204','1205','1299')
                    "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

#合计
          set value_list(890,CU3324)  [expr $value_list(890,CU3324) + $tmp_user_nums     ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
#CU9227	12580生活播报客户数    户  

 	set sql_buf01 "select  count(distinct a.user_id)
                 from dw_product_regsp_${year}${month} a,dw_product_${year}${month} b
                 where a.user_id = b.user_id and a.sp_code='801174' and date(a.expire_date)> '${year}-${month}-${day}'
                       and a.serv_code in ('125834','125851','125853','125855','125874','125854','125833','125835','125849','125859','125897','900','125836','125850','125860','125872','125898','125852','125857','125858','125873')
                 "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

#合计
          set value_list(890,CU9227)  [expr $value_list(890,CU9227) + $tmp_user_nums    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（一）==================================
#CU9228	12580生活播报付费客户数     户 

 	set sql_buf01 "select count(distinct a.product_no)
                 from dw_mthus_dm_${year}${month} a, dw_product_${year}${month} b
                 where a.product_no = b.product_no
                       and a.busi_type = 'MMS'
                       and a.sp_code = '801174'
                       and a.busi_code in ('125834','125851','125853','125855','125874','125854','125833','125835','125849','125859','125897','900','125836','125850','125860','125872','125898','125852','125857','125858','125873')
                       and a.js_month = '${year}${month}'
                       and b.userstatus_id in (1,2,3,6,8)
                       and b.test_mark <> 1
                  "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

#合计
          set value_list(890,CU9228)  [expr $value_list(890,CU9228) + $tmp_user_nums    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（二）==================================
#CU3754	中央音乐收费全曲下载客户数  户   

 	set sql_buf01 "select count(distinct a.user_id)
                 from (select distinct user_id,sum(base_fee+info_fee+month_fee+func_fee) counts
                       from dw_newbusi_ismg_${year}${month}
                       where sp_code in ('600901','600902','600903','600904','600905')
                       group by user_id
                       )a left join dw_product_${year}${month} b on a.user_id=b.user_id,
                      (select distinct user_id
                       from dw_mr_down_cdr_dm_${year}${month}
                       where  sp_code in ('600901','600902','600903','600904','600905')
                       )c
                 where b.usertype_id in (1,2,9)  and a.user_id=c.user_id and a.counts>0 "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row  0 ]

                         set value_list(890,CU3754)  [expr $value_list(890,CU3754) + $tmp_user_nums    ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（三）==================================
#CU9531	号簿管家（PIM）业务活跃用户      户         2010_new

 	set sql_buf01 "select count(distinct a.user_id)
                 from dw_newbusi_ismg_$year$month a,dw_product_regsp_$year$month b,dw_product_$year$month c
                 where a.svcitem_id=300012 and a.user_id=b.user_id and a.user_id = c.user_id
                 and b.busi_type='113' and c.active_mark=1
                 and date(b.expire_date)>'$next_month'
                 "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0  ]

#合计
          set value_list(890,CU9531)  [expr $value_list(890,CU9531) + $tmp_user_nums    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（三）==================================
#CU9551	手机支付/手机钱包用户数      户      

 	set sql_buf01 "select sum(a.counts)
                 from (select  count(distinct a.user_id) counts
                       from  dw_newbusi_call_$year$month a , dw_product_$year$month b
                       where b.usertype_id in (1,2,9) and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1 and a.user_id=b.user_id
                             and a.svcitem_id =100022
                       union all
                       select  count(distinct a.user_id)  counts                                               
                       from dw_product_regsp_${year}${month} a,dw_product_${year}${month} b                                        
                       where a.user_id = b.user_id and  date(a.expire_date)> '${year}-${month}-${day}'                             
                             and a.serv_code in  ('10658888','10658474','10658004') 
                       )a  
                 "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0  ]

#合计
          set value_list(890,CU9551)  [expr $value_list(890,CU9551) + $tmp_user_nums    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}	
#====================================移动增值业务情况统计表（三）==================================
#CU9552	手机支付/手机钱包使用客户数      户      

 	set sql_buf01 "select sum(a.counts)
                   from (select  count(distinct a.user_id) counts
                         from  dw_newbusi_call_$year$month a , dw_product_$year$month b
                         where b.usertype_id in (1,2,9) and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1 and a.user_id=b.user_id
                               and a.svcitem_id =100022 and a.counts>0                 
                         union all
                         select count(distinct user_id) counts
 	                          from dw_product_func_${year}${month} where busi_type = 140
 	                          and  valid_date < '$next_month'
                            and  expire_date >= '$next_month'
                            and  STS in (1)
                         )a
                    "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0  ]

#合计
          set value_list(890,CU9552)  [expr $value_list(890,CU9552) + $tmp_user_nums    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}	
#====================================移动增值业务情况统计表（三）==================================
#CU3614	彩信手机报付费客户数
	set sql_buf01 "select count(distinct a.user_id)
                 from  (select distinct user_id,sum(base_fee+info_fee+month_fee+func_fee) counts
                        from dw_newbusi_mms_$year$month
                        where send_status in (0,1,2,3) and svcitem_id = 400006
                        group by user_id
                        )a,dw_product_$year$month b
                 where a.user_id = b.user_id and a.counts>0
                 "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

          set value_list(890,CU3614)  [expr $value_list(890,CU3614) + $tmp_user_nums    ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#====================================移动增值业务情况统计表（三）==================================
#CU9436	手机阅读业务访问客户数
#CU9433	手机阅读使用客户数
#CU9434	手机阅读付费客户数
#CU9435	手机阅读业务收费使用客户数


 	set sql_buf01 "select count(distinct a.user_id),
 				   count(distinct case when (a.base_fee+a.info_fee+a.month_fee+a.func_fee) > 0 then a.user_id else null end) from
 				   (select user_id, a.base_fee, info_fee, month_fee, func_fee from dw_newbusi_gprs_$year$month a where a.drtype_id in (8308)
 				    union all
 				    select user_id, a.base_fee, info_fee, month_fee, func_fee from dw_newbusi_wap_$year$month a 
 				    where a.drtype_id in (90508, 90509)) a, dw_product_$year$month b
 				   where a.user_id = b.user_id
                 "
	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		   		set	    tmp_user_nums1		        [lindex $this_row 0 ]
		   		set	    tmp_user_nums2		        [lindex $this_row 1 ]

          		set  value_list(890,CU9436)  [expr $value_list(890,CU9436) + $tmp_user_nums1    ]
          		set  value_list(890,CU9433)  [expr $value_list(890,CU9433) + $tmp_user_nums1    ]
          		set  value_list(890,CU9434)  [expr $value_list(890,CU9434) + $tmp_user_nums2    ]
          		set  value_list(890,CU9435)  [expr $value_list(890,CU9435) + $tmp_user_nums2    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}		
#====================================移动增值业务情况统计表（三）==================================
#CU9521	139邮箱活跃用户
#CU9522	139邮箱收费客户数	      

 	set sql_buf01 "select count(distinct b.user_id), 
 						  count(distinct case when (b.base_fee+b.info_fee+b.month_fee+b.func_fee) > 0 then b.user_id else null end)
                 from dw_product_$year$month a,dw_newbusi_gprs_${year}${month} b
                 where a.user_id=b.user_id and b.service_code='1040000005'
                 "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums1		        [lindex $this_row 0 ]
		      set	    tmp_user_nums2		        [lindex $this_row 1 ]

              set value_list(890,CU9521)  [expr $value_list(890,CU9521) + $tmp_user_nums1    ]
              set value_list(890,CU9522)  [expr $value_list(890,CU9522) + $tmp_user_nums2    ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}	
#====================================移动增值业务情况统计表（三）==================================
#CU9882	手机视频使用客户数

 	set sql_buf01 "select  count(distinct c.user_id)
                 from  dw_product_$year$month b,dw_newbusi_gprs_$year$month c
                 where c.user_id=b.user_id
                       and c.service_code='1030000004'
                 "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

                         set value_list(890,CU9882)  [expr $value_list(890,CU9882) + $tmp_user_nums    ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}	
#===================================================================================================	 
#CU9920   CMMB手机电视使用用户
    	set sql_buf01 "select count(distinct a.user_id)
                     from dw_product_regsp_$year$month a ,dw_product_$year$month b
                     where a.user_id=b.user_id and a.busi_type='732'
                           and b.userstatus_id in ($rep_online_userstatus_id) and b.test_mark<>1
                 "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_user_nums		        [lindex $this_row 0 ]

                         set value_list(890,CU9920)  [expr $value_list(890,CU9920) + $tmp_user_nums    ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#===================================================================================================	 
#JT3450	农信通订购客户到达数
#JT3420	农信通当月使用客户数
#JT3460	农信通当月付费客户数

    	set sql_buf01 "select  count(distinct user_id), count(distinct fee_user_id) from (
    					select  case when b.level_def_mode = 1 then 888 else value(int(b.ent_city_id),int(a.city_id)) end as city_id, 
								a.user_id, case when a.fee > 0 then a.user_id end as fee_user_id, a.fee
						from dw_enterprise_industry_apply a 
						left join dw_enterprise_member_mid_$year$month b on a.user_id = b.user_id
						where a.apptype_id = 3 and a.op_time = $date_optime) a
                 "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      	set	    tmp_user_nums1		        [lindex $this_row 0 ]
		      	set	    tmp_user_nums2		        [lindex $this_row 1 ]

                         set value_list(890,JT3450)  [expr $value_list(890,JT3450) + $tmp_user_nums1    ]
                         set value_list(890,JT3420)  [expr $value_list(890,JT3420) + $tmp_user_nums1    ]
                         set value_list(890,JT3460)  [expr $value_list(890,JT3460) + $tmp_user_nums2    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}	
#===================================================================================================	 
#CU9880	SP到达数
#CU9881	其中：本地SP个数


    	set sql_buf01 "select count(distinct sp_code), count(distinct case when sp_attribute = 'L' then sp_code else null end)
    				   from dwd_dsmp_sp_company_code_$year$month
    				   where statu_type_id = 'A'
                 "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      	set	    tmp_user_nums1		        [lindex $this_row 0 ]
		      	set	    tmp_user_nums2		        [lindex $this_row 1 ]

                         set value_list(890,CU9880)  [expr $value_list(890,CU9880) + $tmp_user_nums1    ]
                         set value_list(890,CU9881)  [expr $value_list(890,CU9881) + $tmp_user_nums2    ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}	
#CU1141	激活上网本数量
#CU1142	本月上网本使用客户数
	set sql_buf "
	       select count(distinct a.user_id),
	              count(distinct (case when b.user_id is not null then a.user_id end))
	       from (select a.user_id,a.td_gprs_mark
				 from dw_product_td_$year$month a,dw_product_regsp_$year$month b
                 where a.user_id = b.user_id and a.td_user_mark = 1 and a.arr_user_mark=1
                 and b.sts=1 and b.busi_type='737') a 
           left join (select user_id, sum(bigint(upflow1+upflow2+downflow1+downflow2)/(1024*1024)) as gprs_flow
           			  from dw_newbusi_gprs_$year$month group by user_id) b on a.user_id = b.user_id
           left join dw_product_$year$month c on a.user_id = c.user_id
				 "
	  puts $sql_buf			 
	  if [catch {aidb_sql $handle $sql_buf} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

	  while {[set this_row [aidb_fetch $handle]] != ""} {
	  	      set     tmp_user_cnt1	   [lindex $this_row 0 ]
	  	      set     tmp_user_cnt2	   [lindex $this_row 1 ]

		      set value_list(890,CU1141)      [expr $value_list(890,CU1141) + $tmp_user_cnt1  ]
		      set value_list(890,CU1142)      [expr $value_list(890,CU1142) + $tmp_user_cnt2  ]
    }
    aidb_close $handle
	  if [catch {set handle [aidb_open $conn]} errmsg] {
	         trace_sql $errmsg 1302
	         puts $errmsg
	         return -1
	  }
##===================================================================================================
   set   sqlbuf  "delete from stat_rep_content where op_time=$date_optime and rep_no in (604,605,606,674)"
   puts $sqlbuf
   if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		 trace_sql $errmsg 1101
		 return -1
	 }
   aidb_close $handle
   if [catch {set handle [aidb_open $conn]} errmsg] {
		 trace_sql $errmsg 1102
		 return -1
	 }
   foreach index_name $name_list1 {
	 if {![info exists value_list(890,$index_name)]} {
		 trace_sql "The value of index 890,$index_name does not exist!" 1103
		 return -1
	 } else {
		 set	index_value	$value_list(890,$index_name)
		 set	rep_no		"604"
	 }
	 set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
	 if {$ret < 0} {
		 trace_sql "Failed to insert the value of index 890,$index_name!" 1104
		 return -1
	 }
   }

   foreach index_name $name_list2 {
			if {![info exists value_list(890,$index_name)]} {
				trace_sql "The value of index 890,$index_name does not exist!" 1103
				return -1
			} else {
				set	index_value	$value_list(890,$index_name)
				set	rep_no		"605"
			}
			set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
			if {$ret < 0} {
				trace_sql "Failed to insert the value of index 890,$index_name!" 1104
				return -1
		}
		}

     foreach index_name $name_list3 {
			if {![info exists value_list(890,$index_name)]} {
				trace_sql "The value of index 890,$index_name does not exist!" 1103
				return -1
			} else {
				set	index_value	$value_list(890,$index_name)
				set	rep_no		"606"
			}
			set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
			if {$ret < 0} {
				trace_sql "Failed to insert the value of index 890,$index_name!" 1104
				return -1
		}
		}

     foreach index_name $name_list4 {
			if {![info exists value_list(890,$index_name)]} {
				trace_sql "The value of index 890,$index_name does not exist!" 1103
				return -1
			} else {
				set	index_value	$value_list(890,$index_name)
				set	rep_no		"674"
			}
			set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
			if {$ret < 0} {
				trace_sql "Failed to insert the value of index 890,$index_name!" 1104
				return -1
		}
		}
	return 0
}