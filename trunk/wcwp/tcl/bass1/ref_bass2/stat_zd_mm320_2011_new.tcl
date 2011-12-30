#=======================================================================================================================#
#	  名称：stat_zd_mm320_2011.tcl                                                                  					#
#                                                                                                   					#
#	  功能描述：移动增值业务计费量1、2、3、4、5                      编号:综定MM320(621,622,623,624,678)   				#
#                                                                                                   					#
#	  编写人：AsiaInfo	heys                                         日期:2010年1月                    					#
#   Modified History:1.2011-2-17 By Liwei 修改DA6301类指标口径(原口径不够准确)
#                    2、2011-03-02 by lihongliang 修改DS1101指标口径,原代码为sum(a.counts),修改后代码为sum(a.bill_counts)  #
#=======================================================================================================================#
proc deal {p_optime p_timestamp} {

	global conn
	global handle

	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1000
		return -1
	}

	if {[stat_zd_mm320 $p_optime]  != 0} {
		aidb_roll $conn
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	aidb_close $handle

	return 0
}

proc stat_zd_mm320 {p_optime} {
	global conn
	global handle

  source  stat_insert_index.tcl
  source  report.cfg
  set     date_optime   [ai_to_date $p_optime]

  scan   $p_optime "%04s-%02s-%02s" year month day
  set    next_month [GetNextMonth [string range $year$month 0 5]]01
  scan   $next_month  "%04s%02s%02s" next_month_year next_month_month next_month_day

	scan $p_optime "%04s-%02s-%02s" year month day
	scan $p_optime "%04d-%02d-%02d" year2 month2 day2
#	=========== 初始化需要计算的简单指标值 ===========
	#set   city_list    "890 891 892 893 894 895 896 897"

  set   name_list1   "DS1001	DS1002	DS1004	DS1003
                      DS1101	DS1102	DS1104	DS1103
                      DS1141	DS1142	DS1144	DS1143
                      DS1201	DS1202	DS1204	DS1203
                      DS1251	DS1252	DS1254	DS1253
                      DS1261	DS1262	DS1264	DS1263
                      DS1501	DS1502	DS1504	DS1503
                      DS1511	DS1512	DS1514	DS1513
                      DS1521	DS1522	DS1524	DS1523
                      DS7101	DS7102	DS7104	DS7103
                      DS7151	DS7152	DS7154	DS7153
                      DS7161	DS7162	DS7164	DS7163
                      DS4101	DS4102	DS4104	DS4103
                      DS4111	DS4112	DS4114	DS4113
                      DS4121	DS4122	DS4124	DS4123
                      DS7141	DS7142	DS7144	DS7143
                      DS7171	DS7172	DS7174	DS7173
                      DS7181	DS7182	DS7184	DS7183
                      DS6121	DS6122	DS6124	DS6123
                      DS6161	DS6162	DS6164	DS6163
                      DS6171	DS6172	DS6174	DS6173
                      DS6181	DS6182	DS6184	DS6183
                      DS2101	DS2102	DS2104	DS2103
                      DS1271	DS1272	DS1274	DS1273
                      DS3101	DS3102	DS3104	DS3103
                      DS1301	DS1302	DS1304	DS1303
                      DS6111	DS6112	DS6114	DS6113
                      DS6131	DS6132	DS6134	DS6133
                      DS6141	DS6142	DS6144	DS6143
                      DS6151	DS6152	DS6154	DS6153
                      DS6101	DS6102	DS6104	DS6103
                     "
  set   name_list2   "DA2751
                      DA2761
                      DA2771
                      DA2741
                      DA2781
                      DA2791
                      DS8101	DS8102	DS8104	DS8103
                      DS8201	DS8202	DS8204	DS8203
                      DS8301	DS8302	DS8304	DS8303
                      DS8401	DS8402	DS8404	DS8403
                      DS8601	DS8602	DS8604	DS8603
                      DS8501	DS8502	DS8504	DS8503
                      DS8701	DS8702	DS8704	DS8703
                      DA2801
                      DA6301
                      DA6311
                      DA6321
                      DA6371
                      DA6361
                      DA6331
                      DA6341
                      DA6351
                      DA6401
                      DA6411
                      DA6421
                      DA6431
                      DA6441
                      "
  set   name_list3   "DA4101	DA4102	DA4104	DA4103	DA4105
                      DA5101				            DA5105
                      DS9101	DS9102	DS9104	DS9103	DS9105
                      DS9111	DS9112	DS9114	DS9113	DS9115
                      DS9121				            DS9125
                      DS9141				            DS9145
                      DS5211	DS5212	DS5214	DS5213	DS5215
                      DS5221	DS5222	DS5224	DS5223	DS5225
                      DS5231				            DS5235
                      DS5241	DS5242	DS5244	DS5243	DS5245
                      DS5251				            DS5255
                      DS9201	DS9202	DS9204	DS9203	DS9205
                      DS9211	DS9212	DS9214	DS9213	DS9215
                      DS9221	DS9222	DS9224	DS9223	DS9225
                      DS9301	DS9302	DS9304	DS9303	DS9305
                      DS9311	DS9312	DS9314	DS9313	DS9315
                      DS9321	DS9322	DS9324	DS9323	DS9325
                      DS9401	DS9402	DS9404	DS9403	DS9405
                      DS9411	DS9412	DS9414	DS9413	DS9415
                      DS9421	DS9422	DS9424	DS9423	DS9425
                      DA5611	DA5612	DA5614	DA5613
                      DA5711	DA5712	DA5714	DA5713
                      DA5401				            DA5405
                      DA5411				            DA5415
                      DA5421				            DA5425
                      DA5431				            DA5435
                      DA5441				            DA5445
                      DA5451				            DA5455
                      DA5461				            DA5465
                      DA5471				            DA5475
                      DA5481				            DA5485
                      DA5491				            DA5495
                      DA5501				            DA5505
                      DA5511				            DA5515
                      DA5521				            DA5525
                      "
  set   name_list4   "DA9901
                      DA6500
                      DA6510
                      DA9201	DA9202	DA9204	DA9203
                      DA9221	DA9222	DA9224	DA9223
                      DA9205	DA9206	DA9208	DA9207
                      DA9211	DA9212	DA9214	DA9213
                      DA9311	DA9312	DA9314	DA9313
                      DA9411	DA9412	DA9414	DA9413
                      DA9511	DA9512	DA9514	DA9513
                      DA9611	DA9612	DA9614	DA9613
                      DA9711	DA9712	DA9714	DA9713
                      DA9811	DA9812	DA9814	DA9813
                      DA7101	DA7102	DA7104	DA7103
                      DA7111	DA7112	DA7114	DA7113
                      DA7121	DA7122	DA7124	DA7123
                      DA7201	DA7202	DA7204	DA7203
                      DA7211	DA7212	DA7214	DA7213
                      DA7221	DA7222	DA7224	DA7223
                      DA7301	DA7302	DA7304	DA7303
                      DA7311	DA7312	DA7314	DA7313
                      DA7321	DA7322	DA7324	DA7323
                      DA8101	DA8102	DA8104	DA8103
                      DA8111	DA8112	DA8114	DA8113
                      DA8121	DA8122	DA8124	DA8123
                      "

  set   name_list5   "DA8201	DA8202 	DA8204 	DA8203
                      DA8701
                      DA8301
                      DA9301
                      DA9401
                      DA9501
                      DA9601
                      DA9721	DA9722	DA9724	DA9723
                      DA9731	DA9732	DA9734	DA9733
                      DA9741	DA9742	DA9744	DA9743
                      DA9751
                      DS1111	DS1112	DS1114	DS1113
                      DS1151	DS1152	DS1154	DS1153
                      DS1161
                      DS1121	DS1122	DS1124	DS1123
                      DS1231	DS1232	DS1234	DS1233
                      DS1221	DS1222	DS1224	DS1223
                      DS1241	DS1242	DS1244	DS1243
                      DS1211	DS1212	DS1214	DS1213
                      DS1131	DS1132	DS1134	DS1133
                      DS8211	DS8212	DS8214	DS8213
                      DS8221	DS8222	DS8224	DS8223
                      DS8231
                      DS8241	DS8242	DS8244	DS8243
                      DS8251	DS8252	DS8254	DS8253
                      DS8261	DS8262	DS8264	DS8263
                      DS8271	DS8272	DS8274	DS8273
                      "
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
	foreach index_name $name_list5 {
	      set	value_list(890,$index_name)	"0"
	      }
###============================================移动增值业务计费量（一、四）==============================
#一、短信业务计费量  	条	DS1001	DS1002	DS1004	DS1003
#点对点短消息计费量  	条	*DS1101	DS1102	DS1104	DS1103
#其中：网内	          条	*DS1111	DS1112	DS1114	DS1113
#      其中：省际短信	条	DS1151	DS1152	DS1154	DS1153
#      网间	          条	*DS1121	DS1122	DS1124	DS1123
#      其中：联通移动	条	*DS1231	DS1232	DS1234	DS1233 13 中国联通GSM
#            联通固网	条	*DS1221	DS1222	DS1224	DS1223 115 网通北方十省、2 中国联通固定
#            电信移动	条	*DS1241	DS1242	DS1244	DS1243 14 中国联通CDMA
#            电信固网	条	*DS1211	DS1212	DS1214	DS1213 1 中国电信固定(不含小灵通)、116 西藏好易通、4 中国电信小灵通
#      国际及港澳台	  条	*DS1131	DS1132	DS1134	DS1133
#接收到跨省网内点对点条数	条	*DS1161
   set sql_buf01 "select
                       b.crm_brand_id3,
                       case when a.svcitem_id in (200004)               then 1
                            when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (13) then 23
							              when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (115,2) then 22
							              when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (14) then 24
							              when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (1,4,116) then 21
							              when a.svcitem_id in (200001,200002,200005) then 23
                            when a.svcitem_id in (200003)               then 3
                       else 0 end,
                       sum(a.bill_counts)
                  from
                        dw_newbusi_sms_$year$month a,dw_product_$year$month b
                  where a.user_id = b.user_id and a.calltype_id=0
                     and a.svcitem_id in (200001,200002,200003,200004,200005)
                  group by
                       b.crm_brand_id3,
                       case when a.svcitem_id in (200004)               then 1
                            when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (13) then 23
							              when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (115,2) then 22
							              when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (14) then 24
							              when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (1,4,116) then 21
							              when a.svcitem_id in (200001,200002,200005) then 23
                            when a.svcitem_id in (200003)               then 3
                       else 0 end"


	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
		      set	    tmp_svcitem_id      	    [lindex $this_row 1]
          set	    tmp_sum_fees   	          [lindex $this_row 2]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                             set value_list(890,DS1101)  [expr $value_list(890,DS1101) +  $tmp_sum_fees  ]
                             set value_list(890,DS1001)  [expr $value_list(890,DS1001) +  $tmp_sum_fees  ]
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS1111)  [expr $value_list(890,DS1111) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id != 1 && $tmp_svcitem_id != 3 } {
                                         set value_list(890,DS1121)  [expr $value_list(890,DS1121) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS1131)  [expr $value_list(890,DS1131) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 23} {
                                         set value_list(890,DS1231)  [expr $value_list(890,DS1231) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 22} {
                                         set value_list(890,DS1221)  [expr $value_list(890,DS1221) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 24} {
                                         set value_list(890,DS1241)  [expr $value_list(890,DS1241) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 21} {
                                         set value_list(890,DS1211)  [expr $value_list(890,DS1211) +  $tmp_sum_fees  ]
                             }
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                             set value_list(890,DS1102)  [expr $value_list(890,DS1102) +  $tmp_sum_fees  ]
                             set value_list(890,DS1002)  [expr $value_list(890,DS1002) +  $tmp_sum_fees  ]
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS1112)  [expr $value_list(890,DS1112) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id != 1 && $tmp_svcitem_id != 3 } {
                                         set value_list(890,DS1122)  [expr $value_list(890,DS1122) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS1132)  [expr $value_list(890,DS1132) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 23} {
                                         set value_list(890,DS1232)  [expr $value_list(890,DS1232) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 22} {
                                         set value_list(890,DS1222)  [expr $value_list(890,DS1222) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 24} {
                                         set value_list(890,DS1242)  [expr $value_list(890,DS1242) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 21} {
                                         set value_list(890,DS1212)  [expr $value_list(890,DS1212) +  $tmp_sum_fees  ]
                             }
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                             set value_list(890,DS1104)  [expr $value_list(890,DS1104) +  $tmp_sum_fees  ]
                             set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS1114)  [expr $value_list(890,DS1114) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id != 1 && $tmp_svcitem_id != 3 } {
                                         set value_list(890,DS1124)  [expr $value_list(890,DS1124) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS1134)  [expr $value_list(890,DS1134) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 23} {
                                         set value_list(890,DS1234)  [expr $value_list(890,DS1234) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 22} {
                                         set value_list(890,DS1224)  [expr $value_list(890,DS1224) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 24} {
                                         set value_list(890,DS1244)  [expr $value_list(890,DS1244) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 21} {
                                         set value_list(890,DS1214)  [expr $value_list(890,DS1214) +  $tmp_sum_fees  ]
                             }
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                             set value_list(890,DS1104)  [expr $value_list(890,DS1104) +  $tmp_sum_fees  ]
                             set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS1114)  [expr $value_list(890,DS1114) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id != 1 && $tmp_svcitem_id != 3 } {
                                         set value_list(890,DS1124)  [expr $value_list(890,DS1124) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS1134)  [expr $value_list(890,DS1134) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 23} {
                                         set value_list(890,DS1234)  [expr $value_list(890,DS1234) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 22} {
                                         set value_list(890,DS1224)  [expr $value_list(890,DS1224) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 24} {
                                         set value_list(890,DS1244)  [expr $value_list(890,DS1244) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 21} {
                                         set value_list(890,DS1214)  [expr $value_list(890,DS1214) +  $tmp_sum_fees  ]
                             }
                 }
         }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                             set value_list(890,DS1103)  [expr $value_list(890,DS1103) +  $tmp_sum_fees  ]
                             set value_list(890,DS1003)  [expr $value_list(890,DS1003) +  $tmp_sum_fees  ]
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS1113)  [expr $value_list(890,DS1113) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id != 1 && $tmp_svcitem_id != 3 } {
                                         set value_list(890,DS1123)  [expr $value_list(890,DS1123) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS1133)  [expr $value_list(890,DS1133) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 23} {
                                         set value_list(890,DS1233)  [expr $value_list(890,DS1233) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 22} {
                                         set value_list(890,DS1223)  [expr $value_list(890,DS1223) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 24} {
                                         set value_list(890,DS1243)  [expr $value_list(890,DS1243) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 21} {
                                         set value_list(890,DS1213)  [expr $value_list(890,DS1213) +  $tmp_sum_fees  ]
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
#      其中：省际短信	          条	*DS1151	DS1152	DS1154	DS1153
#      接收到跨省网内点对点条数	  条	*DS1161
   set sql_buf01 "
       select crm_brand_id3,calltype_id,sum(cnt)
       from (select b.crm_brand_id3,case when a.calltype_id=1 then 1 else 0 end as calltype_id,
                substr(case when length(rtrim(substr(opp_number,1,13)))=13 then substr(opp_number,3,13)
                       else rtrim(substr(opp_number,1,13)) end,1,7) as hlr_code,
                sum(counts) as cnt
             from dw_newbusi_sms_$year$month a,dw_product_$year$month b
             where a.user_id = b.user_id and a.svcitem_id=200004
             group by b.crm_brand_id3,case when a.calltype_id=1 then 1 else 0 end,substr(case when length(rtrim(substr(opp_number,1,13)))=13 then substr(opp_number,3,13)
                else rtrim(substr(opp_number,1,13)) end,1,7)
             )a
       where hlr_code not in (select hlr_code from DIM_GSM_HLR_INFO where prov_code in ('891') and hlr_type in (0,2))
       group by crm_brand_id3,calltype_id "


	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set     tmp_brand_id              [lindex $this_row 0]
		      set	    tmp_calltype_id    	      [lindex $this_row 1]
          set	    tmp_sum_fees   	          [lindex $this_row 2]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                      if {$tmp_calltype_id == 0 } {
                      set value_list(890,DS1151)  [expr $value_list(890,DS1151) +  $tmp_sum_fees  ]
                      }
                      if {$tmp_calltype_id == 1 } {
                      set value_list(890,DS1161)  [expr $value_list(890,DS1161) +  $tmp_sum_fees  ]
                      }
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                          if {$tmp_calltype_id == 0 } {
                          set value_list(890,DS1152)  [expr $value_list(890,DS1152) +  $tmp_sum_fees  ]
                          }
                    }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                          if {$tmp_calltype_id == 0 } {
                          set value_list(890,DS1154)  [expr $value_list(890,DS1154) +  $tmp_sum_fees  ]
                          }
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                          if {$tmp_calltype_id == 0 } {
                          set value_list(890,DS1154)  [expr $value_list(890,DS1154) +  $tmp_sum_fees  ]
                          }
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                          if {$tmp_calltype_id == 0 } {
                          set value_list(890,DS1153)  [expr $value_list(890,DS1153) +  $tmp_sum_fees  ]
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

#发送网内点对点彩信	条	DS8211	DS8212	DS8214	DS8213
#其中：省际	条	DS8221	DS8222	DS8224	DS8223
#接收网内省际点对点彩信	条	DS8231
#发送网间点对点彩信	条	DS8241	DS8242	DS8244	DS8243
#其中：联通	条	DS8251	DS8252	DS8254	DS8253
#      电信	条	DS8261	DS8262	DS8264	DS8263
#发送国际点对点彩信	条	DS8271	DS8272	DS8274	DS8273
   set sql_buf01 "select
                       b.crm_brand_id3,
                       case when a.svcitem_id in (400001) then 1
                            when a.svcitem_id in (400002) and (a.opp_number like '133%' or  a.opp_number like '189%') then 24
							when a.svcitem_id in (400002) then 23
                            when a.svcitem_id in (400005) then 3
                       else 0 end,
                       sum(a.COUNTS)
                  from
                        dw_newbusi_mms_$year$month a,dw_product_$year$month b
                  where a.user_id = b.user_id and a.calltype_id=0
                     and a.svcitem_id in (400001,400002,400005)
                  group by
                       b.crm_brand_id3,
                       case when a.svcitem_id in (400001) then 1
                            when a.svcitem_id in (400002) and (a.opp_number like '133%' or  a.opp_number like '189%') then 24
							when a.svcitem_id in (400002) then 23
                            when a.svcitem_id in (400005) then 3
                       else 0 end"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
		      set	    tmp_svcitem_id      	    [lindex $this_row 1]
          	  set	    tmp_sum_fees   	            [lindex $this_row 2]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS8211)  [expr $value_list(890,DS8211) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS8271)  [expr $value_list(890,DS8271) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id != 1 && $tmp_svcitem_id != 3 } {
                                         set value_list(890,DS8241)  [expr $value_list(890,DS8241) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 23} {
                                         set value_list(890,DS8251)  [expr $value_list(890,DS8251) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 24} {
                                         set value_list(890,DS8261)  [expr $value_list(890,DS8261) +  $tmp_sum_fees  ]
                             }
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS8212)  [expr $value_list(890,DS8212) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS8272)  [expr $value_list(890,DS8272) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id != 1 && $tmp_svcitem_id != 3 } {
                                         set value_list(890,DS8242)  [expr $value_list(890,DS8242) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 23} {
                                         set value_list(890,DS8252)  [expr $value_list(890,DS8252) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 24} {
                                         set value_list(890,DS8262)  [expr $value_list(890,DS8262) +  $tmp_sum_fees  ]
                             }
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS8214)  [expr $value_list(890,DS8214) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS8274)  [expr $value_list(890,DS8274) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id != 1 && $tmp_svcitem_id != 3 } {
                                         set value_list(890,DS8244)  [expr $value_list(890,DS8244) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 23} {
                                         set value_list(890,DS8254)  [expr $value_list(890,DS8254) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 24} {
                                         set value_list(890,DS8264)  [expr $value_list(890,DS8264) +  $tmp_sum_fees  ]
                             }
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS8214)  [expr $value_list(890,DS8214) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS8274)  [expr $value_list(890,DS8274) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id != 1 && $tmp_svcitem_id != 3 } {
                                         set value_list(890,DS8244)  [expr $value_list(890,DS8244) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 23} {
                                         set value_list(890,DS8254)  [expr $value_list(890,DS8254) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 24} {
                                         set value_list(890,DS8264)  [expr $value_list(890,DS8264) +  $tmp_sum_fees  ]
                             }
                 }
         }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS8213)  [expr $value_list(890,DS8213) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS8273)  [expr $value_list(890,DS8273) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id != 1 && $tmp_svcitem_id != 3 } {
                                         set value_list(890,DS8243)  [expr $value_list(890,DS8243) +  $tmp_sum_fees  ]
                                }
                             if {$tmp_svcitem_id == 23} {
                                         set value_list(890,DS8253)  [expr $value_list(890,DS8253) +  $tmp_sum_fees  ]
                             }
                             if {$tmp_svcitem_id == 24} {
                                         set value_list(890,DS8263)  [expr $value_list(890,DS8263) +  $tmp_sum_fees  ]
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
#      其中：省际彩信	          条	*DS8221	DS8222	DS8224	DS8223
#      接收到跨省网内点对点条数	  条	*DS8231
   set sql_buf01 "
       select crm_brand_id3,calltype_id,sum(cnt)
       from (select b.crm_brand_id3,case when a.calltype_id=1 then 1 else 0 end as calltype_id,
                substr(case when length(rtrim(substr(opp_number,1,13)))=13 then substr(opp_number,3,13)
                       else rtrim(substr(opp_number,1,13)) end,1,7) as hlr_code,
                sum(counts) as cnt
             from dw_newbusi_mms_$year$month a,dw_product_$year$month b
             where a.user_id = b.user_id and a.svcitem_id=400001
             group by b.crm_brand_id3,case when a.calltype_id=1 then 1 else 0 end,substr(case when length(rtrim(substr(opp_number,1,13)))=13 then substr(opp_number,3,13)
                else rtrim(substr(opp_number,1,13)) end,1,7)
             )a
       where hlr_code not in (select hlr_code from DIM_GSM_HLR_INFO where prov_code in ('891') and hlr_type in (0,2))
       group by crm_brand_id3,calltype_id "


	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set     tmp_brand_id              [lindex $this_row 0]
		      set	  tmp_calltype_id    	    [lindex $this_row 1]
              set	  tmp_sum_fees   	        [lindex $this_row 2]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                      if {$tmp_calltype_id == 0 } {
                      set value_list(890,DS8221)  [expr $value_list(890,DS8221) +  $tmp_sum_fees  ]
                      }
                      if {$tmp_calltype_id == 1 } {
                      set value_list(890,DS8231)  [expr $value_list(890,DS8231) +  $tmp_sum_fees  ]
                      }
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                          if {$tmp_calltype_id == 0 } {
                          set value_list(890,DS8222)  [expr $value_list(890,DS8222) +  $tmp_sum_fees  ]
                          }
                    }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                          if {$tmp_calltype_id == 0 } {
                          set value_list(890,DS8224)  [expr $value_list(890,DS8224) +  $tmp_sum_fees  ]
                          }
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                          if {$tmp_calltype_id == 0 } {
                          set value_list(890,DS8224)  [expr $value_list(890,DS8224) +  $tmp_sum_fees  ]
                          }
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                          if {$tmp_calltype_id == 0 } {
                          set value_list(890,DS8223)  [expr $value_list(890,DS8223) +  $tmp_sum_fees  ]
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
##============================================移动增值业务计费量（一）==============================
#一、短信业务计费量(不含彩信)	  条	DS1001	DS1002	DS1004	DS1003
#　　　移动气象站计费量	        条	DS1201	DS1202	DS1204	DS1203	   300011
#      飞信业务计费量	          条	DS1501	DS1502	DS1504	DS1503	   300010
#　　　梦网短信计费量	          条	DS7101	DS7102	DS7104	DS7103	   300001,300002,300003,300004

   set sql_buf01 "select
                         b.crm_brand_id3,
                         case when a.svcitem_id in (300011)                       then 1
                              when a.svcitem_id in (300010)                       then 2
                              when a.svcitem_id in (300001,300002,300003,300004)  then 3
                         else 0 end,
                         a.calltype_id,
                         sum(a.COUNTS)
                  from
                          dw_newbusi_ismg_$year$month a,dw_product_$year$month b
                  where a.user_id=b.user_id and a.svcitem_id in (300011,300010,300001,300002,300003,300004)
                  group by
                         b.crm_brand_id3,
                         case when a.svcitem_id in (300011)                       then 1
                              when a.svcitem_id in (300010)                       then 2
                              when a.svcitem_id in (300001,300002,300003,300004)  then 3
                         else 0 end,
                         a.calltype_id"


	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
		      set	    tmp_svcitem_id      	    [lindex $this_row 1]
		      set       tmp_calltype_id             [lindex $this_row 2]
              set	    tmp_sum_fees   	            [lindex $this_row 3]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS1201)  [expr $value_list(890,DS1201) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1001)  [expr $value_list(890,DS1001) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS1251)  [expr $value_list(890,DS1251) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS1261)  [expr $value_list(890,DS1261) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS1501)  [expr $value_list(890,DS1501) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1001)  [expr $value_list(890,DS1001) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS1511)  [expr $value_list(890,DS1511) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS1521)  [expr $value_list(890,DS1521) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS7101)  [expr $value_list(890,DS7101) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1001)  [expr $value_list(890,DS1001) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS7151)  [expr $value_list(890,DS7151) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS7161)  [expr $value_list(890,DS7161) +  $tmp_sum_fees  ]
                                         }
                                }

                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS1202)  [expr $value_list(890,DS1202) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1002)  [expr $value_list(890,DS1002) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS1252)  [expr $value_list(890,DS1252) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS1262)  [expr $value_list(890,DS1262) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS1502)  [expr $value_list(890,DS1502) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1002)  [expr $value_list(890,DS1002) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS1512)  [expr $value_list(890,DS1512) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS1522)  [expr $value_list(890,DS1522) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS7102)  [expr $value_list(890,DS7102) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1002)  [expr $value_list(890,DS1002) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS7152)  [expr $value_list(890,DS7152) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS7162)  [expr $value_list(890,DS7162) +  $tmp_sum_fees  ]
                                         }
                                }

                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS1204)  [expr $value_list(890,DS1204) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS1254)  [expr $value_list(890,DS1254) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS1264)  [expr $value_list(890,DS1264) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS1504)  [expr $value_list(890,DS1504) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS1514)  [expr $value_list(890,DS1514) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS1524)  [expr $value_list(890,DS1524) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS7104)  [expr $value_list(890,DS7104) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS7154)  [expr $value_list(890,DS7154) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS7164)  [expr $value_list(890,DS7164) +  $tmp_sum_fees  ]
                                         }
                                }

                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS1204)  [expr $value_list(890,DS1204) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS1254)  [expr $value_list(890,DS1254) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS1264)  [expr $value_list(890,DS1264) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS1504)  [expr $value_list(890,DS1504) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS1514)  [expr $value_list(890,DS1514) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS1524)  [expr $value_list(890,DS1524) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS7104)  [expr $value_list(890,DS7104) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS7154)  [expr $value_list(890,DS7154) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS7164)  [expr $value_list(890,DS7164) +  $tmp_sum_fees  ]
                                         }
                                }

                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS1203)  [expr $value_list(890,DS1203) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1003)  [expr $value_list(890,DS1003) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS1253)  [expr $value_list(890,DS1253) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS1263)  [expr $value_list(890,DS1263) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS1503)  [expr $value_list(890,DS1503) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1003)  [expr $value_list(890,DS1003) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS1513)  [expr $value_list(890,DS1513) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS1523)  [expr $value_list(890,DS1523) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS7103)  [expr $value_list(890,DS7103) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1003)  [expr $value_list(890,DS1003) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS7153)  [expr $value_list(890,DS7153) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS7163)  [expr $value_list(890,DS7163) +  $tmp_sum_fees  ]
                                         }
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
##============================================移动增值业务计费量（一）==============================
#3一、短信业务计费量(不含彩信)	条	DS1001	DS1002	DS1004	DS1003
#彩铃业务计费量	                条	DS4101	DS4102	DS4104	DS4103	         300007
#手机邮箱计费量	                条	DS6121	DS6122	DS6124	DS6123	         300017

   set sql_buf01 "select
                       b.crm_brand_id3,
                       case when a.svcitem_id in (300007)                       then 1
                            when a.svcitem_id in (300017)                       then 2
                       else 0 end,
                       a.calltype_id,
                       sum(a.COUNTS)
                  from
                       dw_newbusi_ismg_$year$month a,dw_product_$year$month b
                  where a.user_id=b.user_id and a.svcitem_id in (300007,300017)
                  group by
                       b.crm_brand_id3,
                       case when a.svcitem_id in (300007)                       then 1
                            when a.svcitem_id in (300017)                       then 2
                       else 0 end,
                       a.calltype_id"


	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
		      set	    tmp_svcitem_id      	    [lindex $this_row 1]
              set       tmp_calltype_id             [lindex $this_row 2]
              set	    tmp_sum_fees   	            [lindex $this_row 3]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS4101)  [expr $value_list(890,DS4101) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1001)  [expr $value_list(890,DS1001) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS4111)  [expr $value_list(890,DS4111) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS4121)  [expr $value_list(890,DS4121) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS6121)  [expr $value_list(890,DS6121) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1001)  [expr $value_list(890,DS1001) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS6171)  [expr $value_list(890,DS6171) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS6181)  [expr $value_list(890,DS6181) +  $tmp_sum_fees  ]
                                         }
                                }

                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS4102)  [expr $value_list(890,DS4102) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1002)  [expr $value_list(890,DS1002) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS4112)  [expr $value_list(890,DS4112) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS4122)  [expr $value_list(890,DS4122) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS6122)  [expr $value_list(890,DS6122) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1002)  [expr $value_list(890,DS1002) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS6172)  [expr $value_list(890,DS6172) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS6182)  [expr $value_list(890,DS6182) +  $tmp_sum_fees  ]
                                         }
                                }

                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS4104)  [expr $value_list(890,DS4104) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS4114)  [expr $value_list(890,DS4114) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS4124)  [expr $value_list(890,DS4124) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS6124)  [expr $value_list(890,DS6124) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS6174)  [expr $value_list(890,DS6174) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS6184)  [expr $value_list(890,DS6184) +  $tmp_sum_fees  ]
                                         }
                                }
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS4104)  [expr $value_list(890,DS4104) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS4114)  [expr $value_list(890,DS4114) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS4124)  [expr $value_list(890,DS4124) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS6124)  [expr $value_list(890,DS6124) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS6174)  [expr $value_list(890,DS6174) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS6184)  [expr $value_list(890,DS6184) +  $tmp_sum_fees  ]
                                         }
                                }
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS4103)  [expr $value_list(890,DS4103) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1003)  [expr $value_list(890,DS1003) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS4113)  [expr $value_list(890,DS4113) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS4123)  [expr $value_list(890,DS4123) +  $tmp_sum_fees  ]
                                         }
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS6123)  [expr $value_list(890,DS6123) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1003)  [expr $value_list(890,DS1003) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS6173)  [expr $value_list(890,DS6173) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS6183)  [expr $value_list(890,DS6183) +  $tmp_sum_fees  ]
                                         }
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
##============================================移动增值业务计费量（一）==============================
#4一、短信业务计费量(不含彩信)	条	DS1001	DS1002	DS1004	DS1003
#语音短信计费量	                条	DS1301	DS1302	DS1304	DS1303	         300013

   set sql_buf01 "select
                          b.crm_brand_id3,
                          sum(a.COUNTS)
                       from
                          dw_newbusi_ismg_$year$month  a,dw_product_$year$month  b
                        where a.user_id=b.user_id
                         and a.calltype_id in (1) and a.svcitem_id in (300013)
                     group by
                          b.crm_brand_id3"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
          set	    tmp_sum_fees   	          [lindex $this_row 1]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,DS1301)  [expr $value_list(890,DS1301) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1001)  [expr $value_list(890,DS1001) +  $tmp_sum_fees  ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,DS1302)  [expr $value_list(890,DS1302) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1002)  [expr $value_list(890,DS1002) +  $tmp_sum_fees  ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,DS1304)  [expr $value_list(890,DS1304) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,DS1304)  [expr $value_list(890,DS1304) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,DS1303)  [expr $value_list(890,DS1303) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1003)  [expr $value_list(890,DS1003) +  $tmp_sum_fees  ]
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
##============================================移动增值业务计费量（一）==============================
#一、短信业务计费量(不含彩信)	条	DS1001	DS1002	DS1004	DS1003
#集团短信计费量	              条	DS6111	DS6112	DS6114	DS6113
    set sql_buf01    "select  case
                                  when c.crm_brand_id3 in ($rep_global_brand_id) then 2
                                  when c.crm_brand_id3 in ($rep_mzone_brand_id) then 3
                                  else 4
                              end,
                              sum(b.counts)
                      from dw_enterprise_member_mid_$year$month a,dw_newbusi_ismg_$year$month b,
                           dw_product_$year$month c
                      where a.user_id = b.user_id and a.user_id = c.user_id
                         and b.sp_code  = '931007' and b.calltype_id in (1)
                      group by case
                                  when c.crm_brand_id3 in ($rep_global_brand_id) then 2
                                  when c.crm_brand_id3 in ($rep_mzone_brand_id) then 3
                                  else 4
                               end"

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	          [lindex $this_row 0]
          set	    tmp_sum_fees  	            [lindex $this_row 1]

          set value_list(890,DS611${tmp_brand_id})  [expr $value_list(890,DS611${tmp_brand_id}) +  $tmp_sum_fees  ]
          set value_list(890,DS6111)                [expr $value_list(890,DS6111) +  $tmp_sum_fees  ]
          set value_list(890,DS100${tmp_brand_id})  [expr $value_list(890,DS100${tmp_brand_id}) +  $tmp_sum_fees  ]
          set value_list(890,DS1001)                [expr $value_list(890,DS1001) +  $tmp_sum_fees  ]

}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
##============================================移动增值业务计费量（一）==============================
#一、短信业务计费量(不含彩信)	条	DS1001	DS1002	DS1004	DS1003
#语音杂志计费量	              条	DS7141	DS7142	DS7144	DS7143	    100002,100026

   set sql_buf01 "select
                       b.crm_brand_id3,
                       a.calltype_id,
                       sum(a.COUNTS)
                  from
                        dw_newbusi_call_$year$month a,dw_product_$year$month b
                  where a.user_id=b.user_id and a.svcitem_id in (100002,100026)
                  group by
                       b.crm_brand_id3,
                       a.calltype_id"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	      [lindex $this_row 0]
		      set	    tmp_calltype_id    	      [lindex $this_row 1]
              set	    tmp_sum_fees   	          [lindex $this_row 2]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,DS7141)  [expr $value_list(890,DS7141) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1001)  [expr $value_list(890,DS1001) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS7171)  [expr $value_list(890,DS7171) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS7181)  [expr $value_list(890,DS7181) +  $tmp_sum_fees  ]
                                         }
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,DS7142)  [expr $value_list(890,DS7142) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1002)  [expr $value_list(890,DS1002) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS7172)  [expr $value_list(890,DS7172) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS7182)  [expr $value_list(890,DS7182) +  $tmp_sum_fees  ]
                                         }
                 }
          }
#其中：神州行
          foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,DS7144)  [expr $value_list(890,DS7144) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS7174)  [expr $value_list(890,DS7174) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS7184)  [expr $value_list(890,DS7184) +  $tmp_sum_fees  ]
                                         }
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,DS7144)  [expr $value_list(890,DS7144) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS7174)  [expr $value_list(890,DS7174) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS7184)  [expr $value_list(890,DS7184) +  $tmp_sum_fees  ]
                                         }
                         }
          }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,DS7143)  [expr $value_list(890,DS7143) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1003)  [expr $value_list(890,DS1003) +  $tmp_sum_fees  ]
                                         if {$tmp_calltype_id == 0 } {
                                                 set value_list(890,DS7173)  [expr $value_list(890,DS7173) +  $tmp_sum_fees  ]
                                         }
                                         if {$tmp_calltype_id != 0 } {
                                                 set value_list(890,DS7183)  [expr $value_list(890,DS7183) +  $tmp_sum_fees  ]
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
##============================================移动增值业务计费量（一）==============================
#一、短信业务计费量(不含彩信)	条	DS1001	DS1002	DS1004	DS1003 (其它短信)
#12580短信计费量	            条	DS1271	DS1272	DS1274  DS1273

   set sql_buf01 "select
                        b.crm_brand_id3,
                        sum(a.COUNTS)
                  from
                        dw_newbusi_ismg_${year}${month} a,dw_product_${year}${month} b
                  where a.user_id=b.user_id
                     and a.calltype_id=0 and a.svcitem_id=300024
                  group by
                        b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
          set	    tmp_sum_fees   	          [lindex $this_row 1]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,DS1271)  [expr $value_list(890,DS1271) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1001)  [expr $value_list(890,DS1001) +  $tmp_sum_fees  ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,DS1272)  [expr $value_list(890,DS1272) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1002)  [expr $value_list(890,DS1002) +  $tmp_sum_fees  ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,DS1274)  [expr $value_list(890,DS1274) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,DS1274)  [expr $value_list(890,DS1274) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,DS1273)  [expr $value_list(890,DS1273) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1003)  [expr $value_list(890,DS1003) +  $tmp_sum_fees  ]
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
##============================================移动增值业务计费量（一）==============================
#一、短信业务计费量(不含彩信)	条	DS1001	DS1002	DS1004	DS1003
#移动秘书计费量	              条	DS2101	DS2102	DS2104	DS2103
#   and a.calltype_id in (1)

   set sql_buf01 "select
                        b.crm_brand_id3,
                        sum(a.COUNTS)
                  from
                       dw_newbusi_call_$year$month a,dw_product_$year$month b
                  where a.user_id=b.user_id and a.svcitem_id in (100021)
                  group by
                       b.crm_brand_id3"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
          set	    tmp_sum_fees   	          [lindex $this_row 1]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,DS2101)  [expr $value_list(890,DS2101) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1001)  [expr $value_list(890,DS1001) +  $tmp_sum_fees  ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,DS2102)  [expr $value_list(890,DS2102) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1002)  [expr $value_list(890,DS1002) +  $tmp_sum_fees  ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,DS2104)  [expr $value_list(890,DS2104) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,DS2104)  [expr $value_list(890,DS2104) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,DS2103)  [expr $value_list(890,DS2103) +  $tmp_sum_fees  ]
                                         set value_list(890,DS1003)  [expr $value_list(890,DS1003) +  $tmp_sum_fees  ]
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
##============================================移动增值业务计费量（一）==============================
#一、短信业务计费量(不含彩信)	条	DS1001	DS1002	DS1004	DS1003
#来电提醒计费量	              条	DS3101	DS3102	DS3104	DS3103	    200007
#   and a.calltype_id in (1)

   set sql_buf01 "select
                        b.crm_brand_id3,
                        sum(a.COUNTS)
                  from
                       dw_newbusi_sms_$year$month a,dw_product_$year$month b
                  where a.user_id=b.user_id and a.svcitem_id in (200007)
                  group by
                       b.crm_brand_id3"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
          set	    tmp_sum_fees   	          [lindex $this_row 1]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                             set value_list(890,DS3101)  [expr $value_list(890,DS3101) +  $tmp_sum_fees  ]
                                             set value_list(890,DS1001)  [expr $value_list(890,DS1001) +  $tmp_sum_fees  ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                             set value_list(890,DS3102)  [expr $value_list(890,DS3102) +  $tmp_sum_fees  ]
                                             set value_list(890,DS1002)  [expr $value_list(890,DS1002) +  $tmp_sum_fees  ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                             set value_list(890,DS3104)  [expr $value_list(890,DS3104) +  $tmp_sum_fees  ]
                                             set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                             set value_list(890,DS3104)  [expr $value_list(890,DS3104) +  $tmp_sum_fees  ]
                                             set value_list(890,DS1004)  [expr $value_list(890,DS1004) +  $tmp_sum_fees  ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                             set value_list(890,DS3103)  [expr $value_list(890,DS3103) +  $tmp_sum_fees  ]
                                             set value_list(890,DS1003)  [expr $value_list(890,DS1003) +  $tmp_sum_fees  ]
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
##============================================移动增值业务计费量（一）==============================
#其中：点对点短信套餐客户计费量	条	DS1141	DS1142	DS1144	DS1143
#信息点播计费量	                条	DS6131	DS6132	DS6134	DS6133 (无)
set sql_buf01   "select b.crm_brand_id3,sum(a.counts)
                 from (select user_id,sum(counts) as counts
                       from dw_newbusi_sms_${year}${month}
                       where calltype_id=0 and svcitem_id in (200001,200002,200003,200004,200005)
                       group by user_id
                       )a,dw_product_${year}${month} b,
                      (select user_id,sprom_id from dw_product_sprom_${year}${month} a
                       where a.valid_date < '$next_month_year-$next_month_month-$next_month_day'
                          and a.expire_date >='$next_month_year-$next_month_month-$next_month_day'
                          and a.active_mark = 1 group by user_id,sprom_id)c,
                      (select prod_id from dim_product_item_tmp where PROd_NAME like '%短信%' and is_prom=1)d
                 where a.user_id=b.user_id and b.user_id=c.user_id and c.sprom_id=d.prod_id
                 group by b.crm_brand_id3 "

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id  	        [lindex $this_row 0 ]
		      set	    tmp_user_counts		      [lindex $this_row 1 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,DS1141)  [expr $value_list(890,DS1141) + $tmp_user_counts     ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,DS1142)  [expr $value_list(890,DS1142) + $tmp_user_counts     ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,DS1144)  [expr $value_list(890,DS1144) + $tmp_user_counts     ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,DS1144)  [expr $value_list(890,DS1144) + $tmp_user_counts     ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,DS1143)  [expr $value_list(890,DS1143) + $tmp_user_counts     ]
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
##============================================移动增值业务计费量（一）==============================
#二、彩信 （MMS）计费量	条	DS8101	DS8102	DS8104	DS8103	    400001,400002,400003,400004
#    其中：点对点彩信	  条	DS8201	DS8202	DS8204	DS8203	    400001,400002,400005   上
#彩信计费量等于MO、AO、EO之和。其中点对点彩信计费量取MO条数，梦网彩信计费量取AO条数，邮箱彩信计费量取EO条数，彩信手机报计费量取AO条数。
#全网彩信手机报计费量：取AO条数。
#MO(上)、MT(下)、AO(下)、AT(上)、EO(下)、ET(上)
#400001	网内点对点彩信
#400002	网外点对点彩信
#400003	邮箱彩信
#400004	梦网彩信
#400005	国际彩信
#400006	手机报(梦网彩信)

   set sql_buf01 "select
                          b.crm_brand_id3,
                          sum(a.COUNTS)
                  from
                        dw_newbusi_mms_$year$month a,dw_product_$year$month b
                  where a.send_status in (0,1,2,3) and a.user_id=b.user_id
                     and a.calltype_id = 0 and a.svcitem_id in (400001,400002,400005)
                  group by
                       b.crm_brand_id3"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
          set	    tmp_sum_fees   	          [lindex $this_row 1]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                          set value_list(890,DS8201)  [expr $value_list(890,DS8201) +  $tmp_sum_fees  ]
                                          set value_list(890,DS8101)  [expr $value_list(890,DS8101) +  $tmp_sum_fees  ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                          set value_list(890,DS8202)  [expr $value_list(890,DS8202) +  $tmp_sum_fees  ]
                                          set value_list(890,DS8102)  [expr $value_list(890,DS8102) +  $tmp_sum_fees  ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                          set value_list(890,DS8204)  [expr $value_list(890,DS8204) +  $tmp_sum_fees  ]
                                          set value_list(890,DS8104)  [expr $value_list(890,DS8104) +  $tmp_sum_fees  ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                          set value_list(890,DS8204)  [expr $value_list(890,DS8204) +  $tmp_sum_fees  ]
                                          set value_list(890,DS8104)  [expr $value_list(890,DS8104) +  $tmp_sum_fees  ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                          set value_list(890,DS8203)  [expr $value_list(890,DS8203) +  $tmp_sum_fees  ]
                                          set value_list(890,DS8103)  [expr $value_list(890,DS8103) +  $tmp_sum_fees  ]
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
##============================================移动增值业务计费量（一）==============================
#二、彩信 （MMS）计费量	条	DS8101	DS8102	DS8104	DS8103	    400001,400002,400003,400004
#          梦网彩信	    条	DS8301	DS8302	DS8304	DS8303	    400004
#          邮箱彩信	    条	DS8401	DS8402	DS8404	DS8403	    400003
#彩信计费量等于MO、AO、EO之和。其中点对点彩信计费量取MO条数，梦网彩信计费量取AO条数，邮箱彩信计费量取EO条数，彩信手机报计费量取AO条数。
#全网彩信手机报计费量：取AO条数。
#MO(上)、MT(下)、AO(下)、AT(上)、EO(下)、ET(上)

   set sql_buf01 "select
                        b.crm_brand_id3,
                        case when a.svcitem_id in (400004)                              then 1
                             when a.svcitem_id in (400003)                              then 2
                        else 0 end,
                        sum(a.counts)
                  from
                      dw_newbusi_mms_$year$month a,dw_product_$year$month b
                  where a.user_id=b.user_id and a.send_status in (0,1,2,3)
                     and a.svcitem_id in (400003,400004)
                  group by
                        b.crm_brand_id3,
                        case when a.svcitem_id in (400004)                              then 1
                             when a.svcitem_id in (400003)                              then 2
                        else 0 end"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
		      set	    tmp_svcitem_id      	    [lindex $this_row 1]
          set	    tmp_sum_fees   	          [lindex $this_row 2]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                    if {$tmp_svcitem_id == 1 } {
                                          set value_list(890,DS8301)  [expr $value_list(890,DS8301) +  $tmp_sum_fees  ]
                                       }
                                    if {$tmp_svcitem_id == 2 } {
                                          set value_list(890,DS8401)  [expr $value_list(890,DS8401) +  $tmp_sum_fees  ]
                                       }
                                          set value_list(890,DS8101)  [expr $value_list(890,DS8101) +  $tmp_sum_fees  ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                    if {$tmp_svcitem_id == 1 } {
                                          set value_list(890,DS8302)  [expr $value_list(890,DS8302) +  $tmp_sum_fees  ]
                                       }
                                    if {$tmp_svcitem_id == 2 } {
                                          set value_list(890,DS8402)  [expr $value_list(890,DS8402) +  $tmp_sum_fees  ]
                                       }
                                          set value_list(890,DS8102)  [expr $value_list(890,DS8102) +  $tmp_sum_fees  ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                    if {$tmp_svcitem_id == 1 } {
                                          set value_list(890,DS8304)  [expr $value_list(890,DS8304) +  $tmp_sum_fees  ]
                                       }
                                    if {$tmp_svcitem_id == 2 } {
                                          set value_list(890,DS8404)  [expr $value_list(890,DS8404) +  $tmp_sum_fees  ]
                                       }
                                          set value_list(890,DS8104)  [expr $value_list(890,DS8104) +  $tmp_sum_fees  ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                    if {$tmp_svcitem_id == 1 } {
                                          set value_list(890,DS8304)  [expr $value_list(890,DS8304) +  $tmp_sum_fees  ]
                                       }
                                    if {$tmp_svcitem_id == 2 } {
                                          set value_list(890,DS8404)  [expr $value_list(890,DS8404) +  $tmp_sum_fees  ]
                                       }
                                          set value_list(890,DS8104)  [expr $value_list(890,DS8104) +  $tmp_sum_fees  ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                    if {$tmp_svcitem_id == 1 } {
                                          set value_list(890,DS8303)  [expr $value_list(890,DS8303) +  $tmp_sum_fees  ]
                                       }
                                    if {$tmp_svcitem_id == 2 } {
                                          set value_list(890,DS8403)  [expr $value_list(890,DS8403) +  $tmp_sum_fees  ]
                                       }
                                          set value_list(890,DS8103)  [expr $value_list(890,DS8103) +  $tmp_sum_fees  ]
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

###============================================移动增值业务计费量（一）=============================
#      彩信手机报	条	DS8601	DS8602	DS8604	DS8603
#DS8101:2372821 == DS8201+DS8301+DS8401+DS8601:3162279
#DS8102:1226994 == DS8202+DS8302+DS8402+DS8602:1658450
#DS8103:286458  == DS8203+DS8303+DS8403+DS8603:377715
#DS8104:859369  == DS8204+DS8304+DS8404+DS8604:1126114

   set sql_buf01 "select
                        b.crm_brand_id3,
                        sum(a.counts)
                 from
                        DW_NEWBUSI_MMS_$year$month a ,dw_product_$year$month b
                       where a.send_status in (0,1,2,3) and a.user_id = b.user_id
                          and b.usertype_id in (1,2,9)
                          and a.calltype_id <> 0 and a.svcitem_id in (400006)
                 group by
                        b.crm_brand_id3"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0 ]
          set	    tmp_sum_fees   	          [lindex $this_row 1 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,DS8601)  [expr $value_list(890,DS8601) +  $tmp_sum_fees  ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,DS8602)  [expr $value_list(890,DS8602) +  $tmp_sum_fees  ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,DS8604)  [expr $value_list(890,DS8604) +  $tmp_sum_fees  ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,DS8604)  [expr $value_list(890,DS8604) +  $tmp_sum_fees  ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,DS8603)  [expr $value_list(890,DS8603) +  $tmp_sum_fees  ]
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
##============================================移动增值业务计费量（一）==============================
#二、彩信 （MMS）计费量	条	DS8101	DS8102	DS8104	DS8103	    400001,400002,400003,400004
#其中：全网彩信手机报	  条	DS8501	DS8502	DS8504	DS8503	    400006

   set sql_buf01 "select
                          b.crm_brand_id3,
                          sum(a.COUNTS)
                  from
                          dw_newbusi_mms_$year$month a,dw_product_$year$month b
                  where a.user_id=b.user_id and a.send_status in (0,1,2,3)
                         and a.calltype_id <> 0 and a.svcitem_id in (400006)
                  group by
                          b.crm_brand_id3"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
          set	    tmp_sum_fees   	          [lindex $this_row 1]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,DS8501)  [expr $value_list(890,DS8501) +  $tmp_sum_fees  ]
                                         set value_list(890,DS8101)  [expr $value_list(890,DS8101) +  $tmp_sum_fees  ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,DS8502)  [expr $value_list(890,DS8502) +  $tmp_sum_fees  ]
                                         set value_list(890,DS8102)  [expr $value_list(890,DS8102) +  $tmp_sum_fees  ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,DS8504)  [expr $value_list(890,DS8504) +  $tmp_sum_fees  ]
                                         set value_list(890,DS8104)  [expr $value_list(890,DS8104) +  $tmp_sum_fees  ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,DS8504)  [expr $value_list(890,DS8504) +  $tmp_sum_fees  ]
                                         set value_list(890,DS8104)  [expr $value_list(890,DS8104) +  $tmp_sum_fees  ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,DS8503)  [expr $value_list(890,DS8503) +  $tmp_sum_fees  ]
                                         set value_list(890,DS8103)  [expr $value_list(890,DS8103) +  $tmp_sum_fees  ]
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

#其中：点对点彩信套餐客户计费量	条	DS8701	DS8702	DS8704	DS8703
set sql_buf01   "select b.crm_brand_id3,sum(a.counts)
                 from (select user_id,sum(bill_counts) as counts
                       from dw_newbusi_mms_${year}${month}
                       where svcitem_id in (400001,400002)
                       group by user_id
                       )a,dw_product_${year}${month} b,
                      (select user_id,sprom_id from dw_product_sprom_${year}${month} a
                       where a.valid_date < '$next_month_year-$next_month_month-$next_month_day'
                          and a.expire_date >='$next_month_year-$next_month_month-$next_month_day'
                          and a.active_mark = 1 group by user_id,sprom_id)c,
                      (select prod_id from dim_product_item_tmp where PROd_NAME like '%彩信%' and is_prom=1)d
                 where a.user_id=b.user_id and b.user_id=c.user_id and c.sprom_id=d.prod_id
                 group by b.crm_brand_id3"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id  	        [lindex $this_row 0 ]
		      set	    tmp_user_counts		      [lindex $this_row 1 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                                         set value_list(890,DS8701)  [expr $value_list(890,DS8701) + $tmp_user_counts     ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                                         set value_list(890,DS8702)  [expr $value_list(890,DS8702) + $tmp_user_counts     ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                                         set value_list(890,DS8704)  [expr $value_list(890,DS8704) + $tmp_user_counts     ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                                         set value_list(890,DS8704)  [expr $value_list(890,DS8704) + $tmp_user_counts     ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                                         set value_list(890,DS8703)  [expr $value_list(890,DS8703) + $tmp_user_counts     ]
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

##============================================移动增值业务计费量（二）==============================
#三、手机上网（WAP）业务计费时长
#      17266CSD计费时长	              分钟	*DA4101	DA4102	DA4104	DA4103	DA4105
#四、随e行业务计费时长
#随e行业务（over CSD）计费时长	      分钟	*DA4201	DA4202	DA4204	DA4203	DA4205

set sql_buf01    "select
                             b.crm_brand_id3,
                             case when a.svcitem_id in (100019) then 1
                                  when a.svcitem_id in (100020) then 2
                             else 1 end,a.mns_type,
                             sum(a.CALL_DURATION_M)
                 from
                        DW_NEWBUSI_call_$year$month a ,dw_product_$year$month b
                       where a.user_id = b.user_id and b.usertype_id in (1,2,9) and a.svcitem_id in (100019,100020)
                 group by
                        b.crm_brand_id3,
                   case when a.svcitem_id in (100019) then 1
                        when a.svcitem_id in (100020) then 2
                        else 1 end,a.mns_type"

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	     [lindex $this_row 0]
		      set     tmp_svcitem_id            [lindex $this_row 1]
		      set     tmp_td_mark            	[lindex $this_row 2]
		      set	    tmp_sum_counts       	[lindex $this_row 3]

                      foreach  all  $brandid_all   {
                                if {$tmp_brand_id == $all } {
                                          if {$tmp_svcitem_id == 1 } {
                                                        set value_list(890,DA4101) [expr $value_list(890,DA4101) + $tmp_sum_counts  ]
                                             }
                                          if {$tmp_svcitem_id == 2 } {
#                                                        set value_list(890,DA4201) [expr $value_list(890,DA4201) + $tmp_sum_counts  ]
                                             }
                                   }
                            }
                  #其中：全球通
                            foreach  global_all	 $brandid_global_all  {
                                if {$tmp_brand_id == $global_all } {
                                          if {$tmp_svcitem_id == 1 } {
                                                        set value_list(890,DA4102) [expr $value_list(890,DA4102) + $tmp_sum_counts  ]
                                             }
                                          if {$tmp_svcitem_id == 2 } {
#                                                        set value_list(890,DA4202) [expr $value_list(890,DA4202) + $tmp_sum_counts  ]
                                             }
                                   }
                            }
                  #其中：神州行
                              foreach   china_all	 $brandid_china_all  {
                                if {$tmp_brand_id == $china_all } {
                                          if {$tmp_svcitem_id == 1 } {
                                                        set value_list(890,DA4104) [expr $value_list(890,DA4104) + $tmp_sum_counts  ]
                                             }
                                          if {$tmp_svcitem_id == 2 } {
#                                                        set value_list(890,DA4204) [expr $value_list(890,DA4204) + $tmp_sum_counts  ]
                                             }
                                   }
                            }
                  #3、本地品牌客户数
                            foreach   other4	 $brandid_other4  {
                                if {$tmp_brand_id == $other4 } {
                                          if {$tmp_svcitem_id == 1 } {
                                                        set value_list(890,DA4104) [expr $value_list(890,DA4104) + $tmp_sum_counts  ]
                                             }
                                          if {$tmp_svcitem_id == 2 } {
#                                                        set value_list(890,DA4204) [expr $value_list(890,DA4204) + $tmp_sum_counts  ]
                                             }
                                         }
                                   }
                  #其中：动感地带
                           foreach  mzone_all  $brandid_mzone_all {
                                   if {$tmp_brand_id == $mzone_all } {
                                          if {$tmp_svcitem_id == 1 } {
                                                        set value_list(890,DA4103) [expr $value_list(890,DA4103) + $tmp_sum_counts  ]
                                             }
                                          if {$tmp_svcitem_id == 2 } {
#                                                        set value_list(890,DA4203) [expr $value_list(890,DA4203) + $tmp_sum_counts  ]
                                             }
                                   }
                            }
                  #其中：TD
                           foreach  td_all  {1} {
                                   if {$tmp_td_mark == $td_all } {
                                          if {$tmp_svcitem_id == 1 } {
                                                        set value_list(890,DA4105) [expr $value_list(890,DA4105) + $tmp_sum_counts  ]
                                             }
                                          if {$tmp_svcitem_id == 2 } {
#                                                        set value_list(890,DA4205) [expr $value_list(890,DA4205) + $tmp_sum_counts  ]
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

##============================================移动增值业务计费量（二）==============================
#随e行WLAN业务计费时长	              分钟	DA6301	-	-	-	-
#其中： 手机号码+密码认证方式计费时长	分钟	DA6311	-	-	-	-
#       SIM卡认证方式计费时长	        分钟	DA6321	-	-	-	-
#		其中：漫游计费时长				分钟	DA6361  -   -   -   -
#		WLAN业务流量					MB		DA6401  -   -   -   -
   #set sql_buf01 "select a.auth_type, roamtype_id, sum(duration_m), sum(bigint(upflow+downflow)/(1024*1024))
   #               from bass2.dw_newbusi_wlan_$year$month a, bass2.dw_product_regsp_$year$month  b
   #               where a.user_id=b.user_id and b.busi_type='6'
   #               group by a.auth_type, roamtype_id
   #              "
   set sql_buf01 "select a.auth_type, a.roamtype_id, sum(a.duration_m), sum(bigint(a.upflow+a.downflow)/(1024*1024))
                  from bass2.dw_newbusi_wlan_$year$month a,
                      (select user_id from bass2.dw_product_regsp_$year$month
                       where busi_type='6' group by user_id
                      ) b
                  where a.user_id=b.user_id
                  group by a.auth_type, a.roamtype_id
                 "

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		        set	    auth_type     	      [lindex $this_row 0]
		        set	    roamtype_id     	    [lindex $this_row 1]
		        set	    duration_m     	      [lindex $this_row 2]
		        set	    gprs_flow     	      [lindex $this_row 3]

            if {$auth_type == 1 } {
                        set value_list(890,DA6311) [expr $value_list(890,DA6311) + $duration_m]
               }
            if {$auth_type == 2 } {
                        set value_list(890,DA6321) [expr $value_list(890,DA6321) + $duration_m]
               }

            if {$roamtype_id != 0 } {
                        set value_list(890,DA6361) [expr $value_list(890,DA6361) + $duration_m]
               }

            set value_list(890,DA6301) [expr $value_list(890,DA6301) + $duration_m ]
            set value_list(890,DA6401) [expr $value_list(890,DA6401) + $gprs_flow  ]
#End Of It
   }
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
	
#DA6331	其中：高校WLAN客户计费时长 分钟
#DA6411	其中：高校WLAN客户流量  MB
#DA6431	高校WLAN客户的WLAN业务收入 元

   set sql_buf01 "
                  select sum(a.duration_m),sum(bigint(upflow+downflow))/1024/1024,sum(base_fee)
                  from dw_newbusi_wlan_$year$month a,
                      (select distinct product_instance_id
	                     from dw_product_ins_off_ins_prod_$year$month
	                     where offer_id in (select product_item_id 
	                                         from bass2.dim_prod_up_product_item
		                   				  where item_type = 'OFFER_PLAN'
		                   				       and name like '%高校WLAN%')
	                     ) b
                   where a.user_id = b.product_instance_id 
                 "

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		        set	    tmp_duration_m     	      [lindex $this_row 0]
		        set     tmp_flow                  [lindex $this_row 1]
		        set     tmp_fee                   [lindex $this_row 2]

            set value_list(890,DA6331) [expr $value_list(890,DA6331) + $tmp_duration_m ]
            set value_list(890,DA6411) [expr $value_list(890,DA6411) + $tmp_flow ]
            set value_list(890,DA6431) [expr $value_list(890,DA6431) + $tmp_fee ]
       
#End Of It
   }
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}

	

##============================================移动增值业务计费量（二）==============================
#五、GPRS业务
#GPRS计费量	                  MB	*DS9101	DS9102	DS9104	DS9103	DS9105
#其中：套餐用户计费量	        MB	DS9111	DS9112	DS9114	DS9113	DS9115
#其中：数据卡产生的计费量	    MB	DS9121	                        DS9125
#其中：非漫游及省内出访计费量	MB	DS5211	DS5212	DS5214	DS5213	DS5215
#      省际漫游出访计费量	    MB	DS5221	DS5222	DS5224	DS5223	DS5225
#      省际漫游来访计费量	    MB	DS5231	                        DS5235
#      国际漫游出访计费量	    MB	DS5241	DS5242	DS5244	DS5243	DS5245
#      国际漫游来访计费量	    MB	DS5251	                        DS5255
#其中：CMNET计费量	          MB	*DS9201	DS9202	DS9204	DS9203	DS9205
#      其中： 上行	          MB	*DS9211	DS9212	DS9214	DS9213	DS9215
#             下行	          MB	*DS9221	DS9222	DS9224	DS9223	DS9225
#      CMWAP计费量	          MB	*DS9301	DS9302	DS9304	DS9303	DS9305
#      其中： 上行	          MB	*DS9311	DS9312	DS9314	DS9313	DS9315
#             下行	          MB	*DS9321	DS9322	DS9324	DS9323	DS9325
#      其他APN计费量	        MB	*DS9401	DS9402	DS9404	DS9403	DS9405
#      其中： 上行	          MB	*DS9411	DS9412	DS9414	DS9413	DS9415
#             下行	          MB	*DS9421	DS9422	DS9424	DS9423	DS9425
  set sql_buf01 "select b.crm_brand_id3,
                         case
                              when a.svcitem_id in (500001)                              then 1
                              when a.svcitem_id in (500002)                              then 2
                              else 3
                         end,a.mns_type,
                         sum(a.flow1),sum(a.flow2)
                  from dw_product_$year$month b,
                      (select user_id,svcitem_id,roamtype_id,mns_type,
                          sum(bigint(upflow1+upflow2))/(1024*1024) as flow1,
                          sum(bigint(downflow1+downflow2))/(1024*1024) as flow2
                       from dw_newbusi_gprs_$year$month
                       group by user_id,svcitem_id,roamtype_id,mns_type
                       having sum(base_fee+info_fee)>0) a
                  where a.user_id = b.user_id
                  group by b.crm_brand_id3,
                           case
                                when a.svcitem_id in (500001)                              then 1
                                when a.svcitem_id in (500002)                              then 2
                                else 3
                           end,a.mns_type"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	    [lindex $this_row 0]
		      set	    tmp_svcitem_id      	    [lindex $this_row 1]
		      set	    tmp_td_mark      	    [lindex $this_row 2]
          	 set	    tmp_sum_ups   	         [lindex $this_row 3]
          	 set	    tmp_sum_downs   	    [lindex $this_row 4]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS9211)  [expr $value_list(890,DS9211) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9221)  [expr $value_list(890,DS9221) +  $tmp_sum_downs]
                                         set value_list(890,DS9201)  [expr $value_list(890,DS9201) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9201)  [expr $value_list(890,DS9201) +  $tmp_sum_downs]
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS9311)  [expr $value_list(890,DS9311) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9321)  [expr $value_list(890,DS9321) +  $tmp_sum_downs]
                                         set value_list(890,DS9301)  [expr $value_list(890,DS9301) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9301)  [expr $value_list(890,DS9301) +  $tmp_sum_downs]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS9411)  [expr $value_list(890,DS9411) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9421)  [expr $value_list(890,DS9421) +  $tmp_sum_downs]
                                         set value_list(890,DS9401)  [expr $value_list(890,DS9401) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9401)  [expr $value_list(890,DS9401) +  $tmp_sum_downs]
                                }
                 set value_list(890,DS9101)  [expr $value_list(890,DS9101) + $tmp_sum_ups + $tmp_sum_downs ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS9212)  [expr $value_list(890,DS9212) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9222)  [expr $value_list(890,DS9222) +  $tmp_sum_downs]
                                         set value_list(890,DS9202)  [expr $value_list(890,DS9202) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9202)  [expr $value_list(890,DS9202) +  $tmp_sum_downs]
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS9312)  [expr $value_list(890,DS9312) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9322)  [expr $value_list(890,DS9322) +  $tmp_sum_downs]
                                         set value_list(890,DS9302)  [expr $value_list(890,DS9302) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9302)  [expr $value_list(890,DS9302) +  $tmp_sum_downs]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS9412)  [expr $value_list(890,DS9412) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9422)  [expr $value_list(890,DS9422) +  $tmp_sum_downs]
                                         set value_list(890,DS9402)  [expr $value_list(890,DS9402) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9402)  [expr $value_list(890,DS9402) +  $tmp_sum_downs]
                                }
                 set value_list(890,DS9102)  [expr $value_list(890,DS9102) + $tmp_sum_ups + $tmp_sum_downs ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS9214)  [expr $value_list(890,DS9214) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9224)  [expr $value_list(890,DS9224) +  $tmp_sum_downs]
                                         set value_list(890,DS9204)  [expr $value_list(890,DS9204) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9204)  [expr $value_list(890,DS9204) +  $tmp_sum_downs]
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS9314)  [expr $value_list(890,DS9314) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9324)  [expr $value_list(890,DS9324) +  $tmp_sum_downs]
                                         set value_list(890,DS9304)  [expr $value_list(890,DS9304) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9304)  [expr $value_list(890,DS9304) +  $tmp_sum_downs]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS9414)  [expr $value_list(890,DS9414) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9424)  [expr $value_list(890,DS9424) +  $tmp_sum_downs]
                                         set value_list(890,DS9404)  [expr $value_list(890,DS9404) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9404)  [expr $value_list(890,DS9404) +  $tmp_sum_downs]
                                }
                 set value_list(890,DS9104)  [expr $value_list(890,DS9104) + $tmp_sum_ups + $tmp_sum_downs ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS9214)  [expr $value_list(890,DS9214) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9224)  [expr $value_list(890,DS9224) +  $tmp_sum_downs]
                                         set value_list(890,DS9204)  [expr $value_list(890,DS9204) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9204)  [expr $value_list(890,DS9204) +  $tmp_sum_downs]
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS9314)  [expr $value_list(890,DS9314) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9324)  [expr $value_list(890,DS9324) +  $tmp_sum_downs]
                                         set value_list(890,DS9304)  [expr $value_list(890,DS9304) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9304)  [expr $value_list(890,DS9304) +  $tmp_sum_downs]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS9414)  [expr $value_list(890,DS9414) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9424)  [expr $value_list(890,DS9424) +  $tmp_sum_downs]
                                         set value_list(890,DS9404)  [expr $value_list(890,DS9404) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9404)  [expr $value_list(890,DS9404) +  $tmp_sum_downs]
                                }
                 set value_list(890,DS9104)  [expr $value_list(890,DS9104) + $tmp_sum_ups + $tmp_sum_downs ]
                 }
         }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS9213)  [expr $value_list(890,DS9213) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9223)  [expr $value_list(890,DS9223) +  $tmp_sum_downs]
                                         set value_list(890,DS9203)  [expr $value_list(890,DS9203) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9203)  [expr $value_list(890,DS9203) +  $tmp_sum_downs]
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS9313)  [expr $value_list(890,DS9313) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9323)  [expr $value_list(890,DS9323) +  $tmp_sum_downs]
                                         set value_list(890,DS9303)  [expr $value_list(890,DS9303) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9303)  [expr $value_list(890,DS9303) +  $tmp_sum_downs]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS9413)  [expr $value_list(890,DS9413) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9423)  [expr $value_list(890,DS9423) +  $tmp_sum_downs]
                                         set value_list(890,DS9403)  [expr $value_list(890,DS9403) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9403)  [expr $value_list(890,DS9403) +  $tmp_sum_downs]
                                }
                 set value_list(890,DS9103)  [expr $value_list(890,DS9103) + $tmp_sum_ups + $tmp_sum_downs ]
                 }
          }
#其中：TD
         foreach  td_all  {1} {
                 if {$tmp_td_mark == $td_all } {
                             if {$tmp_svcitem_id == 1 } {
                                         set value_list(890,DS9215)  [expr $value_list(890,DS9215) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9225)  [expr $value_list(890,DS9225) +  $tmp_sum_downs]
                                         set value_list(890,DS9205)  [expr $value_list(890,DS9205) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9205)  [expr $value_list(890,DS9205) +  $tmp_sum_downs]
                                }
                             if {$tmp_svcitem_id == 2 } {
                                         set value_list(890,DS9315)  [expr $value_list(890,DS9315) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9325)  [expr $value_list(890,DS9325) +  $tmp_sum_downs]
                                         set value_list(890,DS9305)  [expr $value_list(890,DS9305) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9305)  [expr $value_list(890,DS9305) +  $tmp_sum_downs]
                                }
                             if {$tmp_svcitem_id == 3 } {
                                         set value_list(890,DS9415)  [expr $value_list(890,DS9415) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9425)  [expr $value_list(890,DS9425) +  $tmp_sum_downs]
                                         set value_list(890,DS9405)  [expr $value_list(890,DS9405) +  $tmp_sum_ups  ]
                                         set value_list(890,DS9405)  [expr $value_list(890,DS9405) +  $tmp_sum_downs]
                                }
                 set value_list(890,DS9105)  [expr $value_list(890,DS9105) + $tmp_sum_ups + $tmp_sum_downs ]
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
#移动数据流量合计	MB	DA5101
  set sql_buf01 "select a.mns_type,sum(a.flow1),sum(a.flow2)
                  from dw_product_$year$month b,
                      (select user_id,mns_type,
                          sum(bigint(upflow1+upflow2))/(1024*1024) as flow1,
                          sum(bigint(downflow1+downflow2))/(1024*1024) as flow2
                       from dw_newbusi_gprs_$year$month
                       group by user_id,mns_type) a
                  where a.user_id = b.user_id
                  group by a.mns_type"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_td_mark      	     [lindex $this_row 0]
          	  set	    tmp_sum_ups   	         [lindex $this_row 1]
          	  set	    tmp_sum_downs   	     [lindex $this_row 2]

                 set value_list(890,DA5101)  [expr $value_list(890,DA5101) + $tmp_sum_ups + $tmp_sum_downs ]

#其中：TD
         foreach  td_all {1} {
                 if {$tmp_td_mark == $td_all } {
                  		set value_list(890,DA5105)  [expr $value_list(890,DA5105) + $tmp_sum_ups + $tmp_sum_downs ]
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
#其中：套餐用户计费量	        MB	DS9111	DS9112	DS9114	DS9113	DS9115
  set sql_buf01  "select b.crm_brand_id3,a.mns_type,sum(a.flow1+a.flow2)
                 from (select user_id,svcitem_id,roamtype_id,mns_type,
                          sum(bigint(upflow1+upflow2)/(1024*1024)) as flow1,
                          sum(bigint(downflow1+downflow2)/(1024*1024)) as flow2
                       from dw_newbusi_gprs_$year$month
                       group by user_id,svcitem_id,roamtype_id,mns_type
                       having sum(base_fee+info_fee)>0)a,
                       dw_product_${year}${month} b,
                      (select user_id,sprom_id from dw_product_sprom_${year}${month} a
                       where a.valid_date < '$next_month_year-$next_month_month-$next_month_day'
                          and a.expire_date >='$next_month_year-$next_month_month-$next_month_day'
                          and a.active_mark = 1 group by user_id,sprom_id)c,
                      (select prod_id from dim_product_item_tmp where prod_name like '%手机上网%' or prod_name like '%GPRS%')d
                 where a.user_id=b.user_id and b.user_id=c.user_id and c.sprom_id=d.prod_id
                 group by b.crm_brand_id3,a.mns_type"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id  	        [lindex $this_row 0 ]
		      set	    tmp_td_mark  	        [lindex $this_row 1 ]
		      set	    tmp_gprs_flow		    [lindex $this_row 2 ]

#合计
          foreach  all  $brandid_all   {
                 if {$tmp_brand_id == $all } {
                          set value_list(890,DS9111)  [expr $value_list(890,DS9111) + $tmp_gprs_flow     ]
                 }
          }
#其中：全球通
          foreach  global_all	 $brandid_global_all  {
                 if {$tmp_brand_id == $global_all } {
                          set value_list(890,DS9112)  [expr $value_list(890,DS9112) + $tmp_gprs_flow     ]
                 }
          }
#其中：神州行
            foreach   china_all	 $brandid_china_all  {
                 if {$tmp_brand_id == $china_all } {
                          set value_list(890,DS9114)  [expr $value_list(890,DS9114) + $tmp_gprs_flow     ]
                 }
          }
#3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                 if {$tmp_brand_id == $other4 } {
                          set value_list(890,DS9114)  [expr $value_list(890,DS9114) + $tmp_gprs_flow     ]
                         }
                 }
#其中：动感地带
         foreach  mzone_all  $brandid_mzone_all {
                 if {$tmp_brand_id == $mzone_all } {
                          set value_list(890,DS9113)  [expr $value_list(890,DS9113) + $tmp_gprs_flow     ]
                 }
          }
#其中：TD
         foreach  td_all {1} {
                 if {$tmp_td_mark == $td_all } {
                          set value_list(890,DS9115)  [expr $value_list(890,DS9115) + $tmp_gprs_flow     ]
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
#其中：数据卡产生的计费量	    MB	DS9121	                        DS9125
#2010-6-3 为满足公式DS9121>=BT2700>=DS9125，把条件 b.arr_user_mark = 1 and b.td_user_mark = 1 and b.td_gprs_mark = 1 只加到DS9125上 by heys

  set sql_buf01 "select sum(a.flow),
  				  		sum(case when a.mns_type = 1 and b.arr_user_mark = 1 and b.td_user_mark = 1 and b.td_gprs_mark = 1 then a.flow else 0 end)
                 from (select user_id,mns_type
                      ,sum(bigint(upflow1+upflow2+downflow1+downflow2)/(1024*1024)) as flow
                       from dw_newbusi_gprs_$year$month
                       group by user_id,mns_type
                       having sum(base_fee+info_fee)>0) a
                       left join dw_product_td_${year}${month} b
                 on a.user_id = b.user_id
                 where b.crm_brand_id3 in (700)"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_gprs_flow		    [lindex $this_row 0 ]
		      set	    tmp_td_gprs_flow		[lindex $this_row 1 ]

		      set value_list(890,DS9121)  [expr $value_list(890,DS9121) + $tmp_gprs_flow    ]
              set value_list(890,DS9125)  [expr $value_list(890,DS9125) + $tmp_td_gprs_flow ]
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}

#其中：上网本客户产生的计费量	    MB	DS9141	                        DS9145
  #set sql_buf01 "select a.mns_type, sum(a.flow)
  #               from (select a.user_id,a.mns_type,a.drtype_id,sum(bigint(a.upflow1+a.upflow2+a.downflow1+a.downflow2)/(1024*1024)) as flow
  #                     from dw_newbusi_gprs_$year$month a, dw_product_td_$year$month b
  #                     where a.user_id=b.user_id and b.arr_user_mark=1 and b.td_user_mark = 1
  #                     group by a.user_id,a.mns_type,a.drtype_id
  #                     having sum(a.base_fee+a.info_fee)>0) a
  #               where a.drtype_id = 8307
  #               group by a.mns_type"
	#puts $sql_buf01
	#if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	#	trace_sql $errmsg 1301
	#	return -1
	#}
  #while {[set this_row [aidb_fetch $handle]] != ""} {
  #		      set	    tmp_td_mark		        [lindex $this_row 0 ]
  #		      set	    tmp_gprs_flow		    [lindex $this_row 1 ]
  #
  #		      set value_list(890,DS9141)  [expr $value_list(890,DS9141) + $tmp_gprs_flow   ]
  ##其中：TD
  #         foreach  td_all {1} {
  #                 if {$tmp_td_mark == $td_all } {
  #                          set value_list(890,DS9145)  [expr $value_list(890,DS9145) + $tmp_gprs_flow     ]
  #                 }
  #          }
  #
  ##End Of It
  #}
  set sql_buf01 "
      select sum(flow),
             sum(case
                   when b.user_id is not null and a.mns_type = 1 then
                    flow
                 end)
        from (select a.user_id,
                     a.mns_type,
                     sum(bigint(a.upflow1 + a.upflow2 + a.downflow1 + a.downflow2) /
                         (1024 * 1024)) as flow
                from dw_newbusi_gprs_$year$month a
               where a.drtype_id = 8307
               group by a.user_id, a.mns_type
              having sum(a.base_fee + a.info_fee) > 0) a
        left join (select user_id
                     from dw_product_td_$year$month
                    where arr_user_mark = 1
                      and td_user_mark = 1) b
          on a.user_id = b.user_id
      "
	 puts $sql_buf01
	 if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	 	trace_sql $errmsg 1301
	 	return -1
	 }

	 while {[set this_row [aidb_fetch $handle]] != ""} {
	 	      set	    tmp_gprs_flow		        [lindex $this_row 0 ]
	 	      set	    tmp_gprs_td_flow		    [lindex $this_row 1 ]

	 	      set value_list(890,DS9141)  [expr $value_list(890,DS9141) + $tmp_gprs_flow      ]
	 	      set value_list(890,DS9145)  [expr $value_list(890,DS9145) + $tmp_gprs_td_flow   ]
   }
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
##============================================移动增值业务计费量（二）==============================
#PDP激活次数	  次	DA5611	DA5612	DA5614	DA5613	-
#GPRS计费时长	分钟	DA5711	DA5712	DA5714	DA5713	-
    set sql_buf01    "select
                             b.crm_brand_id3,
                             count(1),
                             sum(bigint(a.DURATION1 + a.DURATION2))/60
                 from
                        DW_NEWBUSI_gprs_$year$month a ,dw_product_$year$month b
                       where a.user_id=b.user_id and b.usertype_id in (1,2,9) and a.svcitem_id in (500001,500002,500003)
                 group by
                        b.crm_brand_id3"

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
		      set	    tmp_gprs_counts       	  [lindex $this_row 1]
		      set	    tmp_gprs_duration     	  [lindex $this_row 2]

                      foreach  all  $brandid_all   {
                                if {$tmp_brand_id == $all } {
                                                       set value_list(890,DA5611) [expr $value_list(890,DA5611) + $tmp_gprs_counts      ]
                                                       set value_list(890,DA5711) [expr $value_list(890,DA5711) + $tmp_gprs_duration    ]
                                                    }
                                   }
                  #其中：全球通
                            foreach  global_all	 $brandid_global_all  {
                                if {$tmp_brand_id == $global_all } {
                                                       set value_list(890,DA5612) [expr $value_list(890,DA5612) + $tmp_gprs_counts      ]
                                                       set value_list(890,DA5712) [expr $value_list(890,DA5712) + $tmp_gprs_duration    ]
                                   }
                            }
                  #其中：神州行
                              foreach   china_all	 $brandid_china_all  {
                                if {$tmp_brand_id == $china_all } {
                                                       set value_list(890,DA5614) [expr $value_list(890,DA5614) + $tmp_gprs_counts      ]
                                                       set value_list(890,DA5714) [expr $value_list(890,DA5714) + $tmp_gprs_duration    ]
                                   }
                            }
                  #3、本地品牌客户数
                            foreach   other4	 $brandid_other4  {
                                if {$tmp_brand_id == $other4 } {
                                                       set value_list(890,DA5614) [expr $value_list(890,DA5614) + $tmp_gprs_counts      ]
                                                       set value_list(890,DA5714) [expr $value_list(890,DA5714) + $tmp_gprs_duration    ]
                                         }
                                   }
                  #其中：动感地带
                           foreach  mzone_all  $brandid_mzone_all {
                                   if {$tmp_brand_id == $mzone_all } {
                                                       set value_list(890,DA5613) [expr $value_list(890,DA5613) + $tmp_gprs_counts      ]
                                                       set value_list(890,DA5713) [expr $value_list(890,DA5713) + $tmp_gprs_duration    ]
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
#====================================================================================================
#其中：非漫游及省内出访计费量	MB	*DS5211	DS5212	DS5214	DS5213	DS5215
#      省际漫游出访计费量	    MB	*DS5221	DS5222	DS5224	DS5223	DS5225
#      国际漫游出访计费量	    MB	*DS5241	DS5242	DS5244	DS5243	DS5245
#      省际漫游来访计费量	    MB	DS5231	                        DS5235
#      国际漫游来访计费量	    MB	DS5251	                        DS5255

    set sql_buf01    "select
                             b.crm_brand_id3,
                             case when a.roamtype_id in (0,1) then 1
                                  when a.roamtype_id in (4) then 2
                                  else 4 end,a.mns_type,
                             sum(a.flow1+a.flow2)
                 from (select user_id,svcitem_id,roamtype_id,mns_type,
                          sum(bigint(upflow1+upflow2))/(1024*1024) as flow1,
                          sum(bigint(downflow1+downflow2))/(1024*1024) as flow2
                       from dw_newbusi_gprs_$year$month
                       group by user_id,svcitem_id,roamtype_id,mns_type
                       having sum(base_fee+info_fee)>0) a,dw_product_$year$month b
                 where a.user_id=b.user_id
                 group by b.crm_brand_id3,
                             case when a.roamtype_id in (0,1) then 1
                                  when a.roamtype_id in (4) then 2
                                  else 4 end,a.mns_type"

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	          [lindex $this_row 0]
		      set	    tmp_roamtype_id       	      [lindex $this_row 1]
		      set	    tmp_td_mark       	  		  [lindex $this_row 2]
		      set	    tmp_gprs_flow     	      	  [lindex $this_row 3]

    foreach  all  $brandid_all   {
        if {$tmp_brand_id == $all } {
                 set value_list(890,DS52${tmp_roamtype_id}1) [expr $value_list(890,DS52${tmp_roamtype_id}1) + $tmp_gprs_flow ]
           }
    }
    #其中：全球通
    foreach  global_all	 $brandid_global_all  {
        if {$tmp_brand_id == $global_all } {
                 set value_list(890,DS52${tmp_roamtype_id}2) [expr $value_list(890,DS52${tmp_roamtype_id}2) + $tmp_gprs_flow ]
           }
    }
    #其中：神州行
    foreach   china_all	 $brandid_china_all  {
        if {$tmp_brand_id == $china_all } {
                 set value_list(890,DS52${tmp_roamtype_id}4) [expr $value_list(890,DS52${tmp_roamtype_id}4) + $tmp_gprs_flow ]
           }
    }
    #3、本地品牌客户数
    foreach   other4	 $brandid_other4  {
        if {$tmp_brand_id == $other4 } {
                 set value_list(890,DS52${tmp_roamtype_id}4) [expr $value_list(890,DS52${tmp_roamtype_id}4) + $tmp_gprs_flow ]
           }
    }
    #其中：动感地带
    foreach  mzone_all  $brandid_mzone_all {
        if {$tmp_brand_id == $mzone_all } {
                 set value_list(890,DS52${tmp_roamtype_id}3) [expr $value_list(890,DS52${tmp_roamtype_id}3) + $tmp_gprs_flow ]
           }
    }
    #其中：TD
    foreach  td_all  {1} {
        if {$tmp_td_mark == $td_all } {
                 set value_list(890,DS52${tmp_roamtype_id}5) [expr $value_list(890,DS52${tmp_roamtype_id}5) + $tmp_gprs_flow ]
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
#====================================================================================================
#      省际漫游来访计费量	    MB	DS5231	                        DS5235
#====================================================================================================
#      国际漫游来访计费量	    MB	DS5251	                        DS5255
   set sql_buf01 "select
                         sum(bigint(a.upflow1+a.upflow2+a.downflow1+a.downflow2)/(1024*1024))
                 from dw_newbusi_gprs_romain_$year$month a
                 "

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_gprs_flow     	      [lindex $this_row 0]

    set value_list(890,DS5251) [expr $value_list(890,DS5251) + $tmp_gprs_flow ]
#End Of It
   }
#====================================================================================================
  	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
##============================================移动增值业务计费量（三）==============================
#七、彩铃业务12530计费时长（拨打12530）	      分钟	DA9201	DA9202	DA9204	DA9203
set sql_buf01    "select
                             b.crm_brand_id3,
                             sum(a.COUNTS),
                             sum(a.CALL_DURATION_M)
                 from
                        DW_NEWBUSI_call_$year$month a ,dw_product_$year$month b
                 where b.usertype_id in (1,2,9) and a.svcitem_id in (100004) and a.user_id=b.user_id
                 group by
                        b.crm_brand_id3"

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
		      set	    tmp_sum_counts         	  [lindex $this_row 1]
		      set	    tmp_call_duration         [lindex $this_row 2]

    foreach  all  $brandid_all   {
                  if {$tmp_brand_id == $all } {
                                         set value_list(890,DA9201) [expr $value_list(890,DA9201) + $tmp_call_duration    ]
                                      }
                     }
    #其中：全球通
              foreach  global_all	 $brandid_global_all  {
                  if {$tmp_brand_id == $global_all } {
                                         set value_list(890,DA9202) [expr $value_list(890,DA9202) + $tmp_call_duration    ]
                     }
              }
    #其中：神州行
                foreach   china_all	 $brandid_china_all  {
                  if {$tmp_brand_id == $china_all } {
                                          set value_list(890,DA9204) [expr $value_list(890,DA9204) + $tmp_call_duration    ]
                     }
              }
    #3、本地品牌客户数
              foreach   other4	 $brandid_other4  {
                  if {$tmp_brand_id == $other4 } {
                                          set value_list(890,DA9204) [expr $value_list(890,DA9204) + $tmp_call_duration    ]
                           }
                     }
    #其中：动感地带
             foreach  mzone_all  $brandid_mzone_all {
                     if {$tmp_brand_id == $mzone_all } {
                                          set value_list(890,DA9203) [expr $value_list(890,DA9203) + $tmp_call_duration    ]
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
#===================================================================================================
#其中：无线音乐俱乐部计费时长（拨打12530999）	分钟	DA9221	DA9222	DA9224	DA9223
set sql_buf01    "select
                         b.crm_brand_id3,
                         sum(a.CALL_DURATION_M)
                  from
                         DW_CALL_OPPOSITE_$year$month a,dw_product_$year$month b
                  where a.user_id = b.user_id and b.usertype_id in (1,2,9) and a.OPP_NUMBER like '12530999%'
                  group by
                         b.crm_brand_id3"

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
		      set	    tmp_sum_duration       	  [lindex $this_row 1]

    foreach  all  $brandid_all   {
                  if {$tmp_brand_id == $all } {
                                      set value_list(890,DA9221) [expr $value_list(890,DA9221) + $tmp_sum_duration   ]
                     }
              }
    #其中：全球通
              foreach  global_all	 $brandid_global_all  {
                  if {$tmp_brand_id == $global_all } {
                                      set value_list(890,DA9222) [expr $value_list(890,DA9222) + $tmp_sum_duration   ]
                     }
              }
    #其中：神州行
                foreach   china_all	 $brandid_china_all  {
                  if {$tmp_brand_id == $china_all } {
                                      set value_list(890,DA9224) [expr $value_list(890,DA9224) + $tmp_sum_duration   ]
                     }
              }
    #3、本地品牌客户数
              foreach   other4	 $brandid_other4  {
                  if {$tmp_brand_id == $other4 } {
                                      set value_list(890,DA9224) [expr $value_list(890,DA9224) + $tmp_sum_duration   ]
                           }
                     }
    #其中：动感地带
             foreach  mzone_all  $brandid_mzone_all {
                     if {$tmp_brand_id == $mzone_all } {
                                      set value_list(890,DA9223) [expr $value_list(890,DA9223) + $tmp_sum_duration   ]
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
##============================================移动增值业务计费量（三）==============================
#八、收费彩铃铃音下载次数	          次	DA9205	DA9206	DA9208	DA9207
#其中：12530语音下载次数	          次	*DA9211	DA9212	DA9214	DA9213
  set sql_buf01    "select
                             b.crm_brand_id3,
                             sum(a.counts),
                             sum(case when a.svcitem_id in (300020) then a.counts else 0 end),
                             sum(case when a.svcitem_id in (300021) then a.counts else 0 end)
                 from
                        DW_NEWBUSI_ISMG_$year$month a ,dw_product_$year$month b
                       where b.usertype_id in (1,2,9) and a.svcitem_id in (300020,300021) and a.user_id=b.user_id
                 group by
                        b.crm_brand_id3"

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	          [lindex $this_row 0]
		      set	    tmp_sum_counts1         	  [lindex $this_row 1]
		      set	    tmp_sum_counts2         	  [lindex $this_row 2]
		      set	    tmp_sum_counts3         	  [lindex $this_row 3]


                  foreach  all  $brandid_all   {
                           if {$tmp_brand_id == $all } {
                                   set value_list(890,DA9211) [expr $value_list(890,DA9211) + $tmp_sum_counts1    ]

                                   set value_list(890,DA9205) [expr $value_list(890,DA9205) + $tmp_sum_counts1    ]
                              }
                  }
                  #其中：全球通
                  foreach  global_all	 $brandid_global_all  {
                      if {$tmp_brand_id == $global_all } {
                              set value_list(890,DA9212) [expr $value_list(890,DA9212) + $tmp_sum_counts1    ]

                              set value_list(890,DA9206) [expr $value_list(890,DA9206) + $tmp_sum_counts1    ]
                         }
                  }
                  #其中：神州行
                  foreach   china_all	 $brandid_china_all  {
                      if {$tmp_brand_id == $china_all } {
                              set value_list(890,DA9214) [expr $value_list(890,DA9214) + $tmp_sum_counts1    ]

                              set value_list(890,DA9208) [expr $value_list(890,DA9208) + $tmp_sum_counts1    ]
                         }
                  }
                  #3、本地品牌客户数
                  foreach   other4	 $brandid_other4  {
                      if {$tmp_brand_id == $other4 } {
                              set value_list(890,DA9214) [expr $value_list(890,DA9214) + $tmp_sum_counts1    ]

                              set value_list(890,DA9208) [expr $value_list(890,DA9208) + $tmp_sum_counts1    ]
                         }
                  }
                  #其中：动感地带
                  foreach  mzone_all  $brandid_mzone_all {
                          if {$tmp_brand_id == $mzone_all } {
                                  set value_list(890,DA9213) [expr $value_list(890,DA9213) + $tmp_sum_counts1    ]

                                  set value_list(890,DA9207) [expr $value_list(890,DA9207) + $tmp_sum_counts1    ]
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
#====================================================================================================
#      12530短信下载次数	          次	*DA9311	DA9312	DA9314	DA9313
#      WWW网站下载次数	            次	*DA9411	DA9412	DA9414	DA9413
#      WAP下载次数	                次	*DA9511	DA9512	DA9514	DA9513
#      其他方式下载次数	            次	*DA9611	DA9612	DA9614	DA9613
#其中：彩铃铃音盒下载次数	          次	*DA9711	DA9712	DA9714	DA9713
#      全网音乐搜索下载次数	        次	*DA9811	DA9812	DA9814	DA9813
   set sql_buf01 "select crm_brand_id3,sum(cnt1),sum(cnt2),sum(cnt3),sum(cnt4),sum(cnt5)
                  from (select
 	                      value(b.crm_brand_id3,101) as crm_brand_id3,
                          value(sum(case when DOWN_MODE in ('2') then 1 end),0) as cnt1,
                          value(sum(case when DOWN_MODE in ('3') then 1 end),0) as cnt2,
                          value(sum(case when DOWN_MODE in ('4') then 1 end),0) as cnt3,
                          value(sum(case when DOWN_MODE in ('6') then 1 end),0) as cnt4,
                          0 as cnt5
                        from dw_mr_down_cdr_dm_${year}${month} a , dw_product_${year}${month} b
                        where a.user_id = b.user_id
                        group by value(b.crm_brand_id3,101)
                        union all
                        select value(b.crm_brand_id3,101) as crm_brand_id3,
 	                         0 as cnt1,0 as cnt2,0 as cnt3,0 as cnt4,sum(1) as cnt5
 	                    from dw_acct_shoulditem_${year}${month} a,dw_product_${year}${month} b
 	                    where a.user_id=b.user_id and a.item_id= 80000144
 	                    group by value(b.crm_brand_id3,101)
 	                   ) a
 	              group by crm_brand_id3"

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	          [lindex $this_row 0]
		      set	    tmp_sum_counts1         	  [lindex $this_row 1]
		      set	    tmp_sum_counts2         	  [lindex $this_row 2]
		      set	    tmp_sum_counts3         	  [lindex $this_row 3]
		      set	    tmp_sum_counts4         	  [lindex $this_row 4]
		      set	    tmp_sum_counts5         	  [lindex $this_row 5]


          foreach  all  $brandid_all   {
                   if {$tmp_brand_id == $all } {
                            set value_list(890,DA9311) [expr $value_list(890,DA9311) + $tmp_sum_counts3    ]
                            set value_list(890,DA9411) [expr $value_list(890,DA9411) + $tmp_sum_counts1    ]
                            set value_list(890,DA9511) [expr $value_list(890,DA9511) + $tmp_sum_counts2    ]
                            set value_list(890,DA9611) [expr $value_list(890,DA9611) + $tmp_sum_counts4    ]
                            set value_list(890,DA9711) [expr $value_list(890,DA9711) + $tmp_sum_counts5    ]
                            set value_list(890,DA9811) [expr $value_list(890,DA9811) + $tmp_sum_counts1 + $tmp_sum_counts2   ]

                            set value_list(890,DA9205) [expr $value_list(890,DA9205) + $tmp_sum_counts1 + $tmp_sum_counts2 + $tmp_sum_counts3 + $tmp_sum_counts4 ]
                      }
          }
          #其中：全球通
          foreach  global_all	 $brandid_global_all  {
                   if {$tmp_brand_id == $global_all } {
                                 set value_list(890,DA9312) [expr $value_list(890,DA9312) + $tmp_sum_counts3    ]
                                 set value_list(890,DA9412) [expr $value_list(890,DA9412) + $tmp_sum_counts1    ]
                                 set value_list(890,DA9512) [expr $value_list(890,DA9512) + $tmp_sum_counts2    ]
                                 set value_list(890,DA9612) [expr $value_list(890,DA9612) + $tmp_sum_counts4    ]
                                 set value_list(890,DA9712) [expr $value_list(890,DA9712) + $tmp_sum_counts5    ]
                                 set value_list(890,DA9812) [expr $value_list(890,DA9812) + $tmp_sum_counts1 + $tmp_sum_counts2    ]

                                 set value_list(890,DA9206) [expr $value_list(890,DA9206) + $tmp_sum_counts1 + $tmp_sum_counts2 + $tmp_sum_counts3 + $tmp_sum_counts4 ]
                   }
          }
          #其中：神州行
          foreach   china_all	 $brandid_china_all  {
                    if {$tmp_brand_id == $china_all } {
                                 set value_list(890,DA9314) [expr $value_list(890,DA9314) + $tmp_sum_counts3    ]
                                 set value_list(890,DA9414) [expr $value_list(890,DA9414) + $tmp_sum_counts1    ]
                                 set value_list(890,DA9514) [expr $value_list(890,DA9514) + $tmp_sum_counts2    ]
                                 set value_list(890,DA9614) [expr $value_list(890,DA9614) + $tmp_sum_counts4    ]
                                 set value_list(890,DA9714) [expr $value_list(890,DA9714) + $tmp_sum_counts5    ]
                                 set value_list(890,DA9814) [expr $value_list(890,DA9814) + $tmp_sum_counts1 + $tmp_sum_counts2    ]

                                 set value_list(890,DA9208) [expr $value_list(890,DA9208) + $tmp_sum_counts1 + $tmp_sum_counts2 + $tmp_sum_counts3 + $tmp_sum_counts4 ]
                    }
          }
          #3、本地品牌客户数
          foreach   other4	 $brandid_other4  {
                    if {$tmp_brand_id == $other4 } {
                                     set value_list(890,DA9314) [expr $value_list(890,DA9314) + $tmp_sum_counts3    ]
                                     set value_list(890,DA9414) [expr $value_list(890,DA9414) + $tmp_sum_counts1    ]
                                     set value_list(890,DA9514) [expr $value_list(890,DA9514) + $tmp_sum_counts2    ]
                                     set value_list(890,DA9614) [expr $value_list(890,DA9614) + $tmp_sum_counts4    ]
                                     set value_list(890,DA9714) [expr $value_list(890,DA9714) + $tmp_sum_counts5    ]
                                     set value_list(890,DA9814) [expr $value_list(890,DA9814) + $tmp_sum_counts1 + $tmp_sum_counts2    ]

                                 	 set value_list(890,DA9208) [expr $value_list(890,DA9208) + $tmp_sum_counts1 + $tmp_sum_counts2 + $tmp_sum_counts3 + $tmp_sum_counts4 ]
                       }
          }
          #其中：动感地带
          foreach  mzone_all  $brandid_mzone_all {
                   if {$tmp_brand_id == $mzone_all } {
                                 set value_list(890,DA9313) [expr $value_list(890,DA9313) + $tmp_sum_counts3    ]
                                 set value_list(890,DA9413) [expr $value_list(890,DA9413) + $tmp_sum_counts1    ]
                                 set value_list(890,DA9513) [expr $value_list(890,DA9513) + $tmp_sum_counts2    ]
                                 set value_list(890,DA9613) [expr $value_list(890,DA9613) + $tmp_sum_counts4    ]
                                 set value_list(890,DA9713) [expr $value_list(890,DA9713) + $tmp_sum_counts5    ]
                                 set value_list(890,DA9813) [expr $value_list(890,DA9813) + $tmp_sum_counts1 + $tmp_sum_counts2    ]

                                 set value_list(890,DA9207) [expr $value_list(890,DA9207) + $tmp_sum_counts1 + $tmp_sum_counts2 + $tmp_sum_counts3 + $tmp_sum_counts4 ]
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
##============================================移动增值业务计费量（三）==============================
#九、语音杂志业务
#语音杂志业务计费时长	  分钟	*DA7101	DA7102	DA7104	DA7103	100002,100026
#其中：      呼入时长	  分钟	*DA7111	DA7112	DA7114	DA7113
#            呼出时长	  分钟	*DA7121	DA7122	DA7124	DA7123
#其中：全网业务计费时长	分钟	*DA7201	DA7202	DA7204	DA7203	100002
#　　　其中：呼入时长	  分钟	*DA7211	DA7212	DA7214	DA7213
#　　　　　　呼出时长	  分钟	*DA7221	DA7222	DA7224	DA7223
#本地业务计费时长	      分钟	*DA7301	DA7302	DA7304	DA7303	100026
#　　　其中：呼入时长	  分钟	*DA7311	DA7312	DA7314	DA7313
#　　　　　　呼出时长	  分钟	*DA7321	DA7322	DA7324	DA7323
#十、移动沙龙业务
#移动沙龙业务计费时长	  分钟	*DA8101	DA8102	DA8104	DA8103
#其中：  呼入时长       分钟	*DA8111	DA8112	DA8114	DA8113
#        呼出时长       分钟	*DA8121	DA8122	DA8124	DA8123
    set sql_buf01    "select
                                  b.crm_brand_id3,
                                  case when a.calltype_id in (0,2,3) then 1
                                       when a.calltype_id = 1 then 2
                                  else 1 end,
                                  case when a.svcitem_id in (100002) then 1
                                       when a.svcitem_id in (100026) then 2
                                  else 3 end,
                                  sum(a.call_duration_m)
                      from
                             dw_newbusi_call_$year$month a ,dw_product_$year$month b
                      where b.usertype_id in (1,2,9) and a.svcitem_id in (100002,100026,100001)
                      and a.user_id=b.user_id and a.calltype_id in (0,1,2,3)
                      group by
                             b.crm_brand_id3,
                                  case when a.calltype_id in (0,2,3) then 1
                                       when a.CALLTYPE_ID = 1 then 2
                                  else 1 end,
                                  case when a.svcitem_id in (100002) then 1
                                       when a.svcitem_id in (100026) then 2
                                  else 3 end"

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
		      set	    tmp_calltype_id   	      [lindex $this_row 1]
		      set	    tmp_svcitem_id  	        [lindex $this_row 2]
		      set	    tmp_sum_counts         	  [lindex $this_row 3]

	if {$tmp_svcitem_id == 1 } {
	                 foreach  all  $brandid_all   {
                       if {$tmp_brand_id == $all } {
                                if {$tmp_calltype_id == 1 } {
                                       set value_list(890,DA7221) [expr $value_list(890,DA7221) + $tmp_sum_counts    ]
                                       set value_list(890,DA7121) [expr $value_list(890,DA7121) + $tmp_sum_counts    ]
                                       }
                                if {$tmp_calltype_id == 2 } {
                                       set value_list(890,DA7211) [expr $value_list(890,DA7211) + $tmp_sum_counts    ]
                                       set value_list(890,DA7111) [expr $value_list(890,DA7111) + $tmp_sum_counts    ]
                                       }
                                       set value_list(890,DA7201) [expr $value_list(890,DA7201) + $tmp_sum_counts    ]
                                       set value_list(890,DA7101) [expr $value_list(890,DA7101) + $tmp_sum_counts    ]
                       }
                   }
                  #其中：全球通
                  foreach  global_all	 $brandid_global_all  {
                      if {$tmp_brand_id == $global_all } {
                               if {$tmp_calltype_id == 1 } {
                                      set value_list(890,DA7222) [expr $value_list(890,DA7222) + $tmp_sum_counts    ]
                                      set value_list(890,DA7122) [expr $value_list(890,DA7122) + $tmp_sum_counts    ]
                                  }
                               if {$tmp_calltype_id == 2 } {
                                      set value_list(890,DA7212) [expr $value_list(890,DA7212) + $tmp_sum_counts    ]
                                      set value_list(890,DA7112) [expr $value_list(890,DA7112) + $tmp_sum_counts    ]
                                  }
                                      set value_list(890,DA7202) [expr $value_list(890,DA7202) + $tmp_sum_counts    ]
                                      set value_list(890,DA7102) [expr $value_list(890,DA7102) + $tmp_sum_counts    ]
                         }
                  }
                  #其中：神州行
                  foreach   china_all	 $brandid_china_all  {
                    if {$tmp_brand_id == $china_all } {
                             if {$tmp_calltype_id == 1 } {
                                    set value_list(890,DA7224) [expr $value_list(890,DA7224) + $tmp_sum_counts    ]
                                    set value_list(890,DA7124) [expr $value_list(890,DA7124) + $tmp_sum_counts    ]
                                }
                             if {$tmp_calltype_id == 2 } {
                                    set value_list(890,DA7214) [expr $value_list(890,DA7214) + $tmp_sum_counts    ]
                                    set value_list(890,DA7114) [expr $value_list(890,DA7114) + $tmp_sum_counts    ]
                                }
                                    set value_list(890,DA7204) [expr $value_list(890,DA7204) + $tmp_sum_counts    ]
                                    set value_list(890,DA7104) [expr $value_list(890,DA7104) + $tmp_sum_counts    ]
                       }
                  }
                  #3、本地品牌客户数
                  foreach   other4	 $brandid_other4  {
                      if {$tmp_brand_id == $other4 } {
                               if {$tmp_calltype_id == 1 } {
                                      set value_list(890,DA7224) [expr $value_list(890,DA7224) + $tmp_sum_counts    ]
                                      set value_list(890,DA7124) [expr $value_list(890,DA7124) + $tmp_sum_counts    ]
                                  }
                               if {$tmp_calltype_id == 2 } {
                                      set value_list(890,DA7214) [expr $value_list(890,DA7214) + $tmp_sum_counts    ]
                                      set value_list(890,DA7114) [expr $value_list(890,DA7114) + $tmp_sum_counts    ]
                                  }
                                      set value_list(890,DA7204) [expr $value_list(890,DA7204) + $tmp_sum_counts    ]
                                      set value_list(890,DA7104) [expr $value_list(890,DA7104) + $tmp_sum_counts    ]
                         }
                  }
                  #其中：动感地带
                  foreach  mzone_all  $brandid_mzone_all {
                     if {$tmp_brand_id == $mzone_all } {
                              if {$tmp_calltype_id == 1 } {
                                     set value_list(890,DA7223) [expr $value_list(890,DA7223) + $tmp_sum_counts    ]
                                     set value_list(890,DA7123) [expr $value_list(890,DA7123) + $tmp_sum_counts    ]
                                 }
                              if {$tmp_calltype_id == 2 } {
                                     set value_list(890,DA7213) [expr $value_list(890,DA7213) + $tmp_sum_counts    ]
                                     set value_list(890,DA7113) [expr $value_list(890,DA7113) + $tmp_sum_counts    ]
                                 }
                                     set value_list(890,DA7203) [expr $value_list(890,DA7203) + $tmp_sum_counts    ]
                                     set value_list(890,DA7103) [expr $value_list(890,DA7103) + $tmp_sum_counts    ]
                     }
                  }
#End Of If
	}
#===================================================================================================
	if {$tmp_svcitem_id == 2 } {
	                foreach  all  $brandid_all   {
                      if {$tmp_brand_id == $all } {
                               if {$tmp_calltype_id == 1 } {
                                      set value_list(890,DA7321) [expr $value_list(890,DA7321) + $tmp_sum_counts    ]
                                      set value_list(890,DA7121) [expr $value_list(890,DA7121) + $tmp_sum_counts    ]
                                  }
                               if {$tmp_calltype_id == 2 } {
                                      set value_list(890,DA7311) [expr $value_list(890,DA7311) + $tmp_sum_counts    ]
                                      set value_list(890,DA7111) [expr $value_list(890,DA7111) + $tmp_sum_counts    ]
                                  }
                                      set value_list(890,DA7301) [expr $value_list(890,DA7301) + $tmp_sum_counts    ]
                                      set value_list(890,DA7101) [expr $value_list(890,DA7101) + $tmp_sum_counts    ]
                         }
                  }
                  #其中：全球通
                  foreach  global_all	 $brandid_global_all  {
                      if {$tmp_brand_id == $global_all } {
                               if {$tmp_calltype_id == 1 } {
                                      set value_list(890,DA7322) [expr $value_list(890,DA7322) + $tmp_sum_counts    ]
                                      set value_list(890,DA7122) [expr $value_list(890,DA7122) + $tmp_sum_counts    ]
                                  }
                               if {$tmp_calltype_id == 2 } {
                                      set value_list(890,DA7312) [expr $value_list(890,DA7312) + $tmp_sum_counts    ]
                                      set value_list(890,DA7112) [expr $value_list(890,DA7112) + $tmp_sum_counts    ]
                                  }
                                      set value_list(890,DA7302) [expr $value_list(890,DA7302) + $tmp_sum_counts    ]
                                      set value_list(890,DA7102) [expr $value_list(890,DA7102) + $tmp_sum_counts    ]
                         }
                  }
                  #其中：神州行
                  foreach   china_all	 $brandid_china_all  {
                     if {$tmp_brand_id == $china_all } {
                              if {$tmp_calltype_id == 1 } {
                                     set value_list(890,DA7324) [expr $value_list(890,DA7324) + $tmp_sum_counts    ]
                                     set value_list(890,DA7124) [expr $value_list(890,DA7124) + $tmp_sum_counts    ]
                                 }
                              if {$tmp_calltype_id == 2 } {
                                     set value_list(890,DA7314) [expr $value_list(890,DA7314) + $tmp_sum_counts    ]
                                     set value_list(890,DA7114) [expr $value_list(890,DA7114) + $tmp_sum_counts    ]
                                 }
                                     set value_list(890,DA7304) [expr $value_list(890,DA7304) + $tmp_sum_counts    ]
                                     set value_list(890,DA7104) [expr $value_list(890,DA7104) + $tmp_sum_counts    ]
                        }
                   }
                  #3、本地品牌客户数
                  foreach   other4	 $brandid_other4  {
                      if {$tmp_brand_id == $other4 } {
                               if {$tmp_calltype_id == 1 } {
                                      set value_list(890,DA7324) [expr $value_list(890,DA7324) + $tmp_sum_counts    ]
                                      set value_list(890,DA7124) [expr $value_list(890,DA7124) + $tmp_sum_counts    ]
                                  }
                               if {$tmp_calltype_id == 2 } {
                                      set value_list(890,DA7314) [expr $value_list(890,DA7314) + $tmp_sum_counts    ]
                                      set value_list(890,DA7114) [expr $value_list(890,DA7114) + $tmp_sum_counts    ]
                                  }
                                      set value_list(890,DA7304) [expr $value_list(890,DA7304) + $tmp_sum_counts    ]
                                      set value_list(890,DA7104) [expr $value_list(890,DA7104) + $tmp_sum_counts    ]
                         }
                  }
                  #其中：动感地带
                  foreach  mzone_all  $brandid_mzone_all {
                          if {$tmp_brand_id == $mzone_all } {
                                   if {$tmp_calltype_id == 1 } {
                                          set value_list(890,DA7323) [expr $value_list(890,DA7323) + $tmp_sum_counts    ]
                                          set value_list(890,DA7123) [expr $value_list(890,DA7123) + $tmp_sum_counts    ]
                                      }
                                   if {$tmp_calltype_id == 2 } {
                                          set value_list(890,DA7313) [expr $value_list(890,DA7313) + $tmp_sum_counts    ]
                                          set value_list(890,DA7113) [expr $value_list(890,DA7113) + $tmp_sum_counts    ]
                                      }
                                          set value_list(890,DA7303) [expr $value_list(890,DA7303) + $tmp_sum_counts    ]
                                          set value_list(890,DA7103) [expr $value_list(890,DA7103) + $tmp_sum_counts    ]
                          }
                  }
#End Of If
	 }
	 if {$tmp_svcitem_id == 3 } {
                  foreach  all  $brandid_all   {
                       if {$tmp_brand_id == $all } {
                                if {$tmp_calltype_id == 1 } {
                                          set value_list(890,DA8121) [expr $value_list(890,DA8121) + $tmp_sum_counts    ]
                                          set value_list(890,DA8101) [expr $value_list(890,DA8101) + $tmp_sum_counts    ]
                                   }
                                if {$tmp_calltype_id == 2 } {
                                          set value_list(890,DA8111) [expr $value_list(890,DA8111) + $tmp_sum_counts    ]
                                          set value_list(890,DA8101) [expr $value_list(890,DA8101) + $tmp_sum_counts    ]
                                   }
                              }
                          }
                  #其中：全球通
                  foreach  global_all	 $brandid_global_all  {
                      if {$tmp_brand_id == $global_all } {
                               if {$tmp_calltype_id == 1 } {
                                         set value_list(890,DA8122) [expr $value_list(890,DA8122) + $tmp_sum_counts    ]
                                         set value_list(890,DA8102) [expr $value_list(890,DA8102) + $tmp_sum_counts    ]
                                  }
                               if {$tmp_calltype_id == 2 } {
                                         set value_list(890,DA8112) [expr $value_list(890,DA8112) + $tmp_sum_counts    ]
                                         set value_list(890,DA8102) [expr $value_list(890,DA8102) + $tmp_sum_counts    ]
                                  }
                         }
                  }
                  #其中：神州行
                  foreach   china_all	 $brandid_china_all  {
                     if {$tmp_brand_id == $china_all } {
                             if {$tmp_calltype_id == 1 } {
                                    set value_list(890,DA8124) [expr $value_list(890,DA8124) + $tmp_sum_counts    ]
                                    set value_list(890,DA8104) [expr $value_list(890,DA8104) + $tmp_sum_counts    ]
                                }
                             if {$tmp_calltype_id == 2 } {
                                    set value_list(890,DA8114) [expr $value_list(890,DA8114) + $tmp_sum_counts    ]
                                    set value_list(890,DA8104) [expr $value_list(890,DA8104) + $tmp_sum_counts    ]
                                }
                        }
                  }
                  #3、本地品牌客户数
                  foreach   other4	 $brandid_other4  {
                      if {$tmp_brand_id == $other4 } {
                               if {$tmp_calltype_id == 1 } {
                                         set value_list(890,DA8124) [expr $value_list(890,DA8124) + $tmp_sum_counts    ]
                                         set value_list(890,DA8104) [expr $value_list(890,DA8104) + $tmp_sum_counts    ]
                                  }
                               if {$tmp_calltype_id == 2 } {
                                         set value_list(890,DA8114) [expr $value_list(890,DA8114) + $tmp_sum_counts    ]
                                         set value_list(890,DA8104) [expr $value_list(890,DA8104) + $tmp_sum_counts    ]
                                  }
                         }
                  }
                  #其中：动感地带
                  foreach  mzone_all  $brandid_mzone_all {
                      if {$tmp_brand_id == $mzone_all } {
                               if {$tmp_calltype_id == 1 } {
                                         set value_list(890,DA8123) [expr $value_list(890,DA8123) + $tmp_sum_counts    ]
                                         set value_list(890,DA8103) [expr $value_list(890,DA8103) + $tmp_sum_counts    ]
                                  }
                               if {$tmp_calltype_id == 2 } {
                                         set value_list(890,DA8113) [expr $value_list(890,DA8113) + $tmp_sum_counts    ]
                                         set value_list(890,DA8103) [expr $value_list(890,DA8103) + $tmp_sum_counts    ]
                                  }
                      }
                  }
#End Of It
   }
#End Of It
   }
#===================================================================================================
  	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
##============================================移动增值业务计费量（四）==============================
#十一、中国移动客户拨打12580计费时长	分钟	*DA8201	DA8202 	DA8204 	DA8203
#      12580综合信息服务门户查询次数	次	  DA8701	-	-	-
#十二、语音短信计费时长	              分钟	*DA8301	-	-	-
#十三、会议电话
#会议电话次数	                        次	  DA9301	-	-	-
#会议电话时长	                        分钟	DA9401	-	-	-
#会议电话呼入时长	                    分钟	DA9501	-	-	-
#会议电话呼出时长	                    分钟	DA9601	-	-	-
#十四、TD专有增值业务
#多媒体彩铃下载次数	                  次	  DA9721	DA9722	DA9723	DA9724
#视频点播计费时长                     分钟	DA9731	DA9732	DA9733	DA9734
#视频共享计费时长                     分钟	DA9741	DA9742	DA9743	DA9744
#视频会议计费时长                     分钟	DA9751	-	-	-
##============================================移动增值业务计费量（四）==============================
#十一、中国移动客户拨打12580计费时长	分钟	DA8201	DA8202 	DA8204 	DA8203
   set sql_buf01    "select
                             value(b.crm_brand_id3,101) ,
                             sum(a.CALL_DURATION_M)
                      from DW_newbusi_call_${year}${month} a ,dw_product_${year}${month} b
                      where a.user_id=b.user_id and a.svcitem_id in (100045)
                      group by value(b.crm_brand_id3,101) "

	  puts $sql_buf01
	  if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_brand_id    	        [lindex $this_row 0]
		      set	    tmp_sum_counts         	  [lindex $this_row 1]

		#合计
             foreach  all  $brandid_all   {
                  if {$tmp_brand_id == $all } {
		                       set value_list(890,DA8201) [expr $value_list(890,DA8201) + $tmp_sum_counts    ]
		                 }
		         }
    #其中：全球通
             foreach  global_all	 $brandid_global_all  {
                  if {$tmp_brand_id == $global_all } {
                           set value_list(890,DA8202) [expr $value_list(890,DA8202) + $tmp_sum_counts    ]
                     }
             }
    #其中：神州行
             foreach   china_all	 $brandid_china_all  {
                  if {$tmp_brand_id == $china_all } {
                           set value_list(890,DA8204) [expr $value_list(890,DA8204) + $tmp_sum_counts    ]
                     }
             }
    #3、本地品牌客户数
             foreach   other4	 $brandid_other4  {
                  if {$tmp_brand_id == $other4 } {
                           set value_list(890,DA8204) [expr $value_list(890,DA8204) + $tmp_sum_counts    ]
                     }
             }
    #其中：动感地带
             foreach  mzone_all  $brandid_mzone_all {
                     if {$tmp_brand_id == $mzone_all } {
                              set value_list(890,DA8203) [expr $value_list(890,DA8203) + $tmp_sum_counts    ]
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
#十二、语音短信计费时长            分钟	DA8301	-	-	-
  set sql_buf01    "select
                             sum(a.CALL_DURATION_M)
                    from
                          dw_call_opposite_$year$month a ,dw_product_$year$month b
                    where a.user_id=b.user_id and b.usertype_id in (1,2,9)
                        and a.opp_number LIKE '1380____166' "

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
	  	trace_sql $errmsg 1301
	  	return -1
	  }

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_sum_counts         	  [lindex $this_row 0 ]

                  set value_list(890,DA8301) [expr $value_list(890,DA8301) + $tmp_sum_counts    ]
#End Of It
   }
#===================================================================================================
  	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}

#10086短信通信量	条	DA2751
#其中：上行	条	DA2761
#      下行	条	DA2771
#短信营业厅通信量	条	DA2741
#其中：上行	条	DA2771
#      下行	条	DA2781
   set sql_buf01 "select calltype_id,
                         sum(case when opp_number in ('10086') then counts else 0 end),
                         sum(case when opp_number like '10086%' then counts else 0 end)
                  from dw_newbusi_sms_$year$month
                  group by calltype_id"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_calltype_id    	      [lindex $this_row 0]
              set	    tmp_sum_fees1   	      [lindex $this_row 1]
              set	    tmp_sum_fees2   	      [lindex $this_row 2]

#合计
                 set value_list(890,DA2751)  [expr $value_list(890,DA2751) +  $tmp_sum_fees1  ]
                 set value_list(890,DA2741)  [expr $value_list(890,DA2741) +  $tmp_sum_fees2  ]
                 if {$tmp_calltype_id == 0 } {
                      set value_list(890,DA2771)  [expr $value_list(890,DA2771) +  $tmp_sum_fees1  ]
                      set value_list(890,DA2781)  [expr $value_list(890,DA2781) +  $tmp_sum_fees2  ]
                 }
                 if {$tmp_calltype_id == 1 } {
                      set value_list(890,DA2761)  [expr $value_list(890,DA2761) +  $tmp_sum_fees1  ]
                      set value_list(890,DA2791)  [expr $value_list(890,DA2791) +  $tmp_sum_fees2  ]
                 }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#其中：10086彩信	条	DA2801
   set sql_buf01 "select value(sum(counts),0) from dw_newbusi_mms_$year$month
                  where opp_number like '10086%'"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
              set	    tmp_sum_fees   	      [lindex $this_row 0]

#合计
                 set value_list(890,DA2801)  [expr $value_list(890,DA2801) +  $tmp_sum_fees  ]
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#彩信业务产生的移动数据流量	MB	DA5401	-	-	-	DA5405
#手机视频业务产生的移动数据流量	MB	DA5411	-	-	-	DA5415
#CMMB手机电视业务产生的移动数据流量	MB	DA5421	-	-	-	DA5425
#全曲下载业务产生的移动数据流量	MB	DA5431	-	-	-	DA5435
#手机阅读业务产生的移动数据流量	MB	DA5451	-	-	-	DA5455
#手机游戏业务产生的移动数据流量	MB	DA5461	-	-	-	DA5465
   set sql_buf01 "select case when service_code in ('1010000001','1010000002') then 0
                              when service_code in ('1030000004') then 1
                              when service_code in ('1030000014') then 2
                              when service_code in ('1030000008') then 3
                              when service_code in ('1030000012') then 5
                              when service_code in ('1040000010') then 6 end, mns_type,
                         sum(bigint(upflow1+upflow2+downflow1+downflow2)/(1024*1024)) as flow
                         from dw_newbusi_gprs_$year$month
                         where service_code in ('1010000001','1010000002','1030000004','1030000014','1030000008','1030000012','1040000010')
                         group by case when service_code in ('1010000001','1010000002') then 0
                                       when service_code in ('1030000004') then 1
                                       when service_code in ('1030000014') then 2
                                       when service_code in ('1030000008') then 3
                                       when service_code in ('1030000012') then 5
                                       when service_code in ('1040000010') then 6 end, mns_type"

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {
		      set	    tmp_service_code    	  [lindex $this_row 0]
		      set       tmp_mns_type              [lindex $this_row 1]
              set	    tmp_grps_flow   	      [lindex $this_row 2]

#合计
                 set value_list(890,DA54${tmp_service_code}1)  [expr $value_list(890,DA54${tmp_service_code}1) +  $tmp_grps_flow  ]

                 if {$tmp_mns_type == 1 } {
                         set value_list(890,DA54${tmp_service_code}5)  [expr $value_list(890,DA54${tmp_service_code}5) +  $tmp_grps_flow  ]
                 }
#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
	
#DA5521	客户使用WAP营业厅产生的移动数据流量 MB
   set sql_buf01 "select 
                         sum(bigint(upflow1+upflow2+downflow1+downflow2)/(1024*1024)) as flow
                         from dw_newbusi_gprs_$year$month
                         where service_code in ('1030000018') "
                     

	puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

		while {[set this_row [aidb_fetch $handle]] != ""} {

              set	    tmp_grps_flow   	      [lindex $this_row 0]

                 set value_list(890,DA5521)  [expr $value_list(890,DA5521) +  $tmp_grps_flow  ]

#End Of It
}
#===================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}


#===================================================================================================
   set   sqlbuf  "delete from stat_rep_content where op_time=$date_optime and rep_no in (621,622,623,624,678)"
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
		set	rep_no		"621"
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
				set	rep_no		"622"
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
				set	rep_no		"623"
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
				set	rep_no		"624"
			}
			set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
			if {$ret < 0} {
				trace_sql "Failed to insert the value of index 890,$index_name!" 1104
				return -1
		}
		}
	  foreach index_name $name_list5 {
			if {![info exists value_list(890,$index_name)]} {
				trace_sql "The value of index 890,$index_name does not exist!" 1103
				return -1
			} else {
				set	index_value	$value_list(890,$index_name)
				set	rep_no		"678"
			}
			set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
			if {$ret < 0} {
				trace_sql "Failed to insert the value of index 890,$index_name!" 1104
				return -1
		}
		}
	return 0
}