#===================================================================================================#
#	  名称：stat_zd_mm304_2010.tcl                                                                    #
#                                                                                                   #
#	  功能描述： 国内漫游出访通话情况1、2               编号:综定657,658,659,660,661,662              #
#              	 移动电话客户省际漫游来访通话情况1、2            663,664                            #
#                移动电话客户国际及港澳台漫游通话情况            665                                #
#	  编写人：AsiaInfo	heys                        			             日期:2010年1月                 #
#   修改历史：by lihongliang 2011年3月21日  增加TD通话次数、TD基本通话时长、TD长途计费时长相关统计                 #
#===================================================================================================#
proc deal {p_optime p_timestamp} {

	global conn
	global handle

	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1000
		return -1
	}

	if {[stat_zd_mm304 $p_optime]  != 0} {
		aidb_roll $conn
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	aidb_close $handle

	return 0
}

proc stat_zd_mm304 {p_optime} {
	global conn
	global handle

	source	stat_insert_index.tcl
	source	report.cfg
	set	    date_optime	 [ai_to_date $p_optime]

	scan $p_optime "%04s-%02s-%02s" year month day
	scan $p_optime "%04d-%02d-%02d" year2 month2 day2
#	=========== 初始化需要计算的简单指标值 ===========

  set   name_list1   "BB3001 BB3004	BB3002 BB3005	BB3003 BB3006 BB3101 BB3104	BB3102 BB3105	BB3103 BB3106 BB3111 BB3114	BB3112 BB3115 BB3121 BB3124	BB3122 BB3125 BB3131 BB3134	BB3132 BB3135 BB3141 BB3144	BB3142 BB3145	BB3143 BB3146 BB3151 BB3154	BB3152 BB3155	BB3153 BB3156 BB3201 BB3204	BB3202 BB3205	BB3203 BB3206 BB3301 BB3304	BB3302 BB3305 BB3311 BB3314	BB3312 BB3315 BB3321 BB3324	BB3322 BB3325 BB3331 BB3334	BB3332 BB3335 BB3801 BB3804	BB3802 BB3805 BB3811 BB3814	BB3812 BB3815 BB3821 BB3824	BB3822 BB3825 BB3831 BB3834	BB3832 BB3835 BB3501 BB3504	BB3502 BB3505 BB3511 BB3514	BB3512 BB3515 BB3521 BB3524	BB3522 BB3525 BB3531 BB3534	BB3532 BB3535 BB3701 BB3704	BB3702 BB3705 BB3711 BB3714	BB3712 BB3715 BB3721 BB3724	BB3722 BB3725 BB3731 BB3734	BB3732 BB3735"
  set   name_list2   "BB3601 BB3604	BB3602 BB3605 BB3611 BB3614	BB3612 BB3615 BB3621 BB3624	BB3622 BB3625 BB3631 BB3634	BB3632 BB3635 BB3401 BB3404	BB3402 BB3405 BB3411 BB3414	BB3412 BB3415 BB3421 BB3424	BB3422 BB3425 BB3431 BB3434	BB3432 BB3435 BB3901 BB3904	BB3902 BB3905	BB3903 BB3906 BB4101 BB4104	BB4102 BB4105 BB4151 BB4154	BB4152 BB4155 BB4161 BB4164	BB4162 BB4165 BB4171 BB4174	BB4172 BB4175 BB4181 BB4184	BB4182 BB4185 BB4191 BB4194	BB4192 BB4195 BB4201 BB4204	BB4202 BB4205 BB4301 BB4304	BB4302 BB4305 BB4801 BB4804	BB4802 BB4805 BB4501 BB4504	BB4502 BB4505 BB4701 BB4704	BB4702 BB4705 BB4601 BB4604	BB4602 BB4605 BB4401 BB4404	BB4402 BB4405 BB4901 BB4904	BB4902 BB4905"
  set   name_list3   "BF3001 BF3004	BF3002 BF3005	BF3003 BF3006 BF3101 BF3104	BF3102 BF3105	BF3103 BF3106 BF3111 BF3114	BF3112 BF3115 BF3121 BF3124	BF3122 BF3125 BF3131 BF3134	BF3132 BF3135 BF3141 BF3144	BF3142 BF3145	BF3143 BF3146 BF3151 BF3154	BF3152 BF3155	BF3153 BF3156 BF3201 BF3204	BF3202 BF3205	BF3203 BF3206 BF3301 BF3304	BF3302 BF3305 BF3311 BF3314	BF3312 BF3315 BF3321 BF3324	BF3322 BF3325 BF3331 BF3334	BF3332 BF3335 BF3801 BF3804	BF3802 BF3805 BF3811 BF3814	BF3812 BF3815 BF3821 BF3824	BF3822 BF3825 BF3831 BF3834	BF3832 BF3835 BF3501 BF3504	BF3502 BF3505 BF3511 BF3514	BF3512 BF3515 BF3521 BF3524	BF3522 BF3525 BF3531 BF3534	BF3532 BF3535 BF3701 BF3704	BF3702 BF3705 BF3711 BF3714	BF3712 BF3715 BF3721 BF3724	BF3722 BF3725 BF3731 BF3734	BF3732 BF3735"
  set   name_list4   "BF3601 BF3604	BF3602 BF3605 BF3611 BF3614	BF3612 BF3615 BF3621 BF3624	BF3622 BF3625 BF3631 BF3634	BF3632 BF3635 BF3401 BF3404	BF3402 BF3405 BF3411 BF3414	BF3412 BF3415 BF3421 BF3424	BF3422 BF3425 BF3431 BF3434	BF3432 BF3435 BF3901 BF3904	BF3902 BF3905	BF3903 BF3906 BF4101 BF4104	BF4102 BF4105 BF4151 BF4154	BF4152 BF4155 BF4161 BF4164	BF4162 BF4165 BF4171 BF4174	BF4172 BF4175 BF4181 BF4184	BF4182 BF4185 BF4191 BF4194	BF4192 BF4195 BF4201 BF4204	BF4202 BF4205 BF4301 BF4304	BF4302 BF4305 BF4801 BF4804	BF4802 BF4805 BF4501 BF4504	BF4502 BF4505 BF4701 BF4704	BF4702 BF4705 BF4601 BF4604	BF4602 BF4605 BF4401 BF4404	BF4402 BF4405 BF4901 BF4904	BF4902 BF4905"
  set   name_list5   "BE3001 BE3004	BE3002 BE3005	BE3003 BE3006 BE3101 BE3104	BE3102 BE3105	BE3103 BE3106 BE3111 BE3114	BE3112 BE3115 BE3121 BE3124	BE3122 BE3125 BE3131 BE3134	BE3132 BE3135 BE3141 BE3144	BE3142 BE3145	BE3143 BE3146 BE3151 BE3154	BE3152 BE3155	BE3153 BE3156 BE3201 BE3204	BE3202 BE3205	BE3203 BE3206 BE3301 BE3304	BE3302 BE3305 BE3311 BE3314	BE3312 BE3315 BE3321 BE3324	BE3322 BE3325 BE3331 BE3334	BE3332 BE3335 BE3801 BE3804	BE3802 BE3805 BE3811 BE3814	BE3812 BE3815 BE3821 BE3824	BE3822 BE3825 BE3831 BE3834	BE3832 BE3835 BE3501 BE3504	BE3502 BE3505 BE3511 BE3514	BE3512 BE3515 BE3521 BE3524	BE3522 BE3525 BE3531 BE3534	BE3532 BE3535 BE3701 BE3704	BE3702 BE3705 BE3711 BE3714	BE3712 BE3715 BE3721 BE3724	BE3722 BE3725 BE3731 BE3734	BE3732 BE3735"
  set   name_list6   "BE3601 BE3604	BE3602 BE3605 BE3611 BE3614	BE3612 BE3615 BE3621 BE3624	BE3622 BE3625 BE3631 BE3634	BE3632 BE3635 BE3401 BE3404	BE3402 BE3405 BE3411 BE3414	BE3412 BE3415 BE3421 BE3424	BE3422 BE3425 BE3431 BE3434	BE3432 BE3435 BE3901 BE3904	BE3902 BE3905	BE3903 BE3906 BE4101 BE4104	BE4102 BE4105 BE4151 BE4154	BE4152 BE4155 BE4161 BE4164	BE4162 BE4165 BE4171 BE4174	BE4172 BE4175 BE4181 BE4184	BE4182 BE4185 BE4191 BE4194	BE4192 BE4195 BE4201 BE4204	BE4202 BE4205 BE4301 BE4304	BE4302 BE4305 BE4801 BE4804	BE4802 BE4805 BE4501 BE4504	BE4502 BE4505 BE4701 BE4704	BE4702 BE4705 BE4601 BE4604	BE4602 BE4605 BE4401 BE4404	BE4402 BE4405 BE4901 BE4904	BE4902 BE4905"
  set   name_list7   "BT5001 BT5004	BT5002 BT5005	BT5003 BT5006 BT5101 BT5104	BT5102 BT5105	BT5103 BT5106 BT5111 BT5114	BT5112 BT5115 BT5121 BT5124	BT5122 BT5125 BT5131 BT5134	BT5132 BT5135 BT5141 BT5144	BT5142 BT5145	BT5143 BT5146 BT5151 BT5154	BT5152 BT5155	BT5153 BT5156 BT5201 BT5204	BT5202 BT5205	BT5203 BT5206 BT5301 BT5304	BT5302 BT5305 BT5311 BT5314	BT5312 BT5315 BT5321 BT5324	BT5322 BT5325 BT5331 BT5334	BT5332 BT5335 BT5801 BT5804	BT5802 BT5805 BT5811 BT5814	BT5812 BT5815 BT5821 BT5824	BT5822 BT5825 BT5831 BT5834	BT5832 BT5835 BT5501 BT5504	BT5502 BT5505 BT5511 BT5514	BT5512 BT5515 BT5521 BT5524	BT5522 BT5525 BT5531 BT5534	BT5532 BT5535 BT5701 BT5704	BT5702 BT5705 BT5711 BT5714	BT5712 BT5715 BT5721 BT5724	BT5722 BT5725 BT5731 BT5734	BT5732 BT5735"
  set   name_list8   "BT5401 BT5404	BT5402 BT5405 BT5411 BT5414	BT5412 BT5415 BT5421 BT5424	BT5422 BT5425 BT5431 BT5434	BT5432 BT5435 BT5601 BT5604	BT5602 BT5605 BT5611 BT5614	BT5612 BT5615 BT5621 BT5624	BT5622 BT5625 BT5631 BT5634	BT5632 BT5635 BT5901 BT5904	BT5902 BT5905	BT5903 BT5906 BT6101 BT6104	BT6102 BT6105 BT6151 BT6154	BT6152 BT6155 BT6161 BT6164	BT6162 BT6165 BT6171 BT6174	BT6172 BT6175 BT6181 BT6184	BT6182 BT6185 BT6191 BT6194	BT6192 BT6195 BT6201 BT6204	BT6202 BT6205 BT6301 BT6304	BT6302 BT6305 BT6801 BT6804	BT6802 BT6805 BT6501 BT6504	BT6502 BT6505 BT6701 BT6704	BT6702 BT6705 BT6601 BT6604	BT6602 BT6605 BT6401 BT6404	BT6402 BT6405 BT6901 BT6904	BT6902 BT6905"
  set   name_list9   "BV1001 BV1003 BV1101 BV1103 BV1111 BV1113 BV1241 BV1243 BV1131 BV1133 BV1221 BV1223 BV1141 BV1143 BV1211 BV1213 BV1331 BV1333 BV1341 BV1343 BV1391 BV1393 BV2101 BV2103 BV2111 BV2113 BV2121 BV2123 BV2241 BV2243 BV2221 BV2223 BV2131 BV2133 BV2211 BV2213 BV2291 BV2293 BV2001 BV2003	BV2002 BV2007	BV2006 BV3101 BV3103 BV3102 BV3107 BV3106 BV3111 BV3113	BV3112 BV3117	BV3116 BV3121 BV3123 BV3122 BV3127 BV3126 BV3201 BV3203	BV3202 BV3207	BV3206 BV4101 BV4103 BV4102 BV4107 BV4106 BV4111 BV4113	BV4112 BV4117	BV4116 BV4121 BV4123 BV4122 BV4127 BV4126 BV4201 BV4203	BV4202 BV4207	BV4206"

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
  foreach index_name $name_list6 {
	      set	value_list(890,$index_name)	"0"
	      }
  foreach index_name $name_list7 {
	      set	value_list(890,$index_name)	"0"
        }
  foreach index_name $name_list8 {
	      set	value_list(890,$index_name) "0"
	      }
  foreach index_name $name_list9 {
	      set	value_list(890,$index_name)	"0"
	      }
#====================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#===================================================================================================
if {1} {
###====================================全球通客户国内漫游出访通话情况===============================
#指标名称	通话次数（次）	其中：TD通话	基本计费时长	其中：TD基本计	长途计费时长  （分钟）	其中：TD长途计费时长（分钟）次数（次）	（分钟）	费时长（分钟）
#全球通客户国内漫游出访通话量合计	  BB3001 BB3004	BB3002 BB3005	BB3003 BB3006
#1、主叫合计                      	BB3101 BB3104	BB3102 BB3105	BB3103 BB3106
#   本地主叫通话合计              	BB3111 BB3114	BB3112 BB3115	                      0
#   省内长途主叫通话合计          	BB3121 BB3124	BB3122 BB3125	                      1
#   省际长途主叫通话合计  	        BB3131 BB3134	BB3132 BB3135	                      2
#   港澳台长途主叫通话合计        	BB3141 BB3144	BB3142 BB3145	BB3143 BB3146         3,4,5
#   国际长途主叫通话合计          	BB3151 BB3154	BB3152 BB3155	BB3153 BB3156         其它

 	set sql_buf01 "select       b.crm_brand_id3,
                              case when a.TOLLTYPE_ID in (0)  then 1
                                   when a.TOLLTYPE_ID in (1)  then 2
                                   when a.TOLLTYPE_ID in (2)  then 3
                                   when a.TOLLTYPE_ID in (3,4,5)  then 4
                                   else 5
                              end,a.mns_type,
                  	          sum(a.CALL_COUNTS)-sum(case when a.rate_prod_id is null then a.CALL_COUNTS else 0 end)*2,
                  	          sum(a.call_duration_m),
                  	          sum(a.call_duration_s)
                 from dw_call_$year$month a ,dw_product_$year$month b
                 where a.user_id=b.user_id
                 and a.roamtype_id in (1,4,6,8) and a.CALLTYPE_ID in (0,2,3) and a.TOLLTYPE_ID < 100
                 group by b.crm_brand_id3,
                        case when a.TOLLTYPE_ID in (0)  then 1
                             when a.TOLLTYPE_ID in (1)  then 2
                             when a.TOLLTYPE_ID in (2)  then 3
                             when a.TOLLTYPE_ID in (3,4,5)  then 4
                             else 5
                        end,a.mns_type"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id                   [lindex $this_row 0]
	        set     tmp_tolltype_id                [lindex $this_row 1]
	        set     tmp_td_mark                	  [lindex $this_row 2]
	        set     tmp_call_counts                [lindex $this_row 3]
		   set	 tmp_call_duration_m            [lindex $this_row 4]
		   set	 tmp_call_duration_s            [lindex $this_row 5]
#其中：全球通
   foreach global_all $brandid_global_all {
          if {$tmp_brand_id == $global_all } {
                          if {$tmp_tolltype_id == 1 } {
                                         set value_list(890,BB3111)  [expr $value_list(890,BB3111) + $tmp_call_counts       ]
                                         set value_list(890,BB3112)  [expr $value_list(890,BB3112) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BB3114)  [expr $value_list(890,BB3114) + $tmp_call_counts       ]
                                         	set value_list(890,BB3115)  [expr $value_list(890,BB3115) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 2 } {
                                         set value_list(890,BB3121)  [expr $value_list(890,BB3121) + $tmp_call_counts       ]
                                         set value_list(890,BB3122)  [expr $value_list(890,BB3122) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BB3124)  [expr $value_list(890,BB3124) + $tmp_call_counts       ]
                                         	set value_list(890,BB3125)  [expr $value_list(890,BB3125) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 3 } {
                                         set value_list(890,BB3131)  [expr $value_list(890,BB3131) + $tmp_call_counts       ]
                                         set value_list(890,BB3132)  [expr $value_list(890,BB3132) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BB3134)  [expr $value_list(890,BB3134) + $tmp_call_counts       ]
                                         	set value_list(890,BB3135)  [expr $value_list(890,BB3135) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 4 } {
                                         set value_list(890,BB3141)  [expr $value_list(890,BB3141) + $tmp_call_counts       ]
                                         set value_list(890,BB3142)  [expr $value_list(890,BB3142) + $tmp_call_duration_m   ]
                                         set value_list(890,BB3143)  [expr $value_list(890,BB3143) + $tmp_call_duration_s   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BB3144)  [expr $value_list(890,BB3144) + $tmp_call_counts       ]
                                         	set value_list(890,BB3145)  [expr $value_list(890,BB3145) + $tmp_call_duration_m   ]
                                         	set value_list(890,BB3146)  [expr $value_list(890,BB3146) + $tmp_call_duration_s   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 5 } {
                                         set value_list(890,BB3151)  [expr $value_list(890,BB3151) + $tmp_call_counts       ]
                                         set value_list(890,BB3152)  [expr $value_list(890,BB3152) + $tmp_call_duration_m   ]
                                         set value_list(890,BB3153)  [expr $value_list(890,BB3153) + $tmp_call_duration_s  ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BB3154)  [expr $value_list(890,BB3154) + $tmp_call_counts       ]
                                         	set value_list(890,BB3155)  [expr $value_list(890,BB3155) + $tmp_call_duration_m   ]
                                         	set value_list(890,BB3156)  [expr $value_list(890,BB3156) + $tmp_call_duration_s  ]
 						  		 }
                             }
                          if {$tmp_tolltype_id != "" } {
                                         set value_list(890,BB3101)  [expr $value_list(890,BB3101) + $tmp_call_counts       ]
                                         set value_list(890,BB3102)  [expr $value_list(890,BB3102) + $tmp_call_duration_m   ]
                                         set value_list(890,BB3103)  [expr $value_list(890,BB3103) + $tmp_call_duration_s   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BB3104)  [expr $value_list(890,BB3104) + $tmp_call_counts       ]
                                         	set value_list(890,BB3105)  [expr $value_list(890,BB3105) + $tmp_call_duration_m   ]
                                         	set value_list(890,BB3106)  [expr $value_list(890,BB3106) + $tmp_call_duration_s   ]
 						  		 }
                             }
              }
    }
#动感地带
  foreach mzone_all $brandid_mzone_all {
             if {$tmp_brand_id == $mzone_all } {
                          if {$tmp_tolltype_id == 1 } {
                                         set value_list(890,BE3111)  [expr $value_list(890,BE3111) + $tmp_call_counts       ]
                                         set value_list(890,BE3112)  [expr $value_list(890,BE3112) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BE3114)  [expr $value_list(890,BE3114) + $tmp_call_counts       ]
                                         	set value_list(890,BE3115)  [expr $value_list(890,BE3115) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 2 } {
                                         set value_list(890,BE3121)  [expr $value_list(890,BE3121) + $tmp_call_counts       ]
                                         set value_list(890,BE3122)  [expr $value_list(890,BE3122) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BE3124)  [expr $value_list(890,BE3124) + $tmp_call_counts       ]
                                         	set value_list(890,BE3125)  [expr $value_list(890,BE3125) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 3 } {
                                         set value_list(890,BE3131)  [expr $value_list(890,BE3131) + $tmp_call_counts       ]
                                         set value_list(890,BE3132)  [expr $value_list(890,BE3132) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BE3134)  [expr $value_list(890,BE3134) + $tmp_call_counts       ]
                                         	set value_list(890,BE3135)  [expr $value_list(890,BE3135) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 4 } {
                                         set value_list(890,BE3141)  [expr $value_list(890,BE3141) + $tmp_call_counts       ]
                                         set value_list(890,BE3142)  [expr $value_list(890,BE3142) + $tmp_call_duration_m   ]
                                         set value_list(890,BE3143)  [expr $value_list(890,BE3143) + $tmp_call_duration_s   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BE3144)  [expr $value_list(890,BE3144) + $tmp_call_counts       ]
                                         	set value_list(890,BE3145)  [expr $value_list(890,BE3145) + $tmp_call_duration_m   ]
                                         	set value_list(890,BE3146)  [expr $value_list(890,BE3146) + $tmp_call_duration_s   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 5 } {
                                         set value_list(890,BE3151)  [expr $value_list(890,BE3151) + $tmp_call_counts       ]
                                         set value_list(890,BE3152)  [expr $value_list(890,BE3152) + $tmp_call_duration_m   ]
                                         set value_list(890,BE3153)  [expr $value_list(890,BE3153) + $tmp_call_duration_s   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BE3154)  [expr $value_list(890,BE3154) + $tmp_call_counts       ]
                                         	set value_list(890,BE3155)  [expr $value_list(890,BE3155) + $tmp_call_duration_m   ]
                                         	set value_list(890,BE3156)  [expr $value_list(890,BE3156) + $tmp_call_duration_s   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id != "" } {
                                         set value_list(890,BE3101)  [expr $value_list(890,BE3101) + $tmp_call_counts       ]
                                         set value_list(890,BE3102)  [expr $value_list(890,BE3102) + $tmp_call_duration_m   ]
                                         set value_list(890,BE3103)  [expr $value_list(890,BE3103) + $tmp_call_duration_s   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BE3104)  [expr $value_list(890,BE3104) + $tmp_call_counts       ]
                                         	set value_list(890,BE3105)  [expr $value_list(890,BE3105) + $tmp_call_duration_m   ]
                                         	set value_list(890,BE3106)  [expr $value_list(890,BE3106) + $tmp_call_duration_s   ]
 						  		 }
                             }
              }
    }
#其中：神州行
     foreach china_all $brandid_china_all {
             if {$tmp_brand_id == $china_all } {
                          if {$tmp_tolltype_id == 1 } {
                                         set value_list(890,BF3111)  [expr $value_list(890,BF3111) + $tmp_call_counts       ]
                                         set value_list(890,BF3112)  [expr $value_list(890,BF3112) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF3114)  [expr $value_list(890,BF3114) + $tmp_call_counts       ]
                                         	set value_list(890,BF3115)  [expr $value_list(890,BF3115) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 2 } {
                                         set value_list(890,BF3121)  [expr $value_list(890,BF3121) + $tmp_call_counts       ]
                                         set value_list(890,BF3122)  [expr $value_list(890,BF3122) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF3124)  [expr $value_list(890,BF3124) + $tmp_call_counts       ]
                                         	set value_list(890,BF3125)  [expr $value_list(890,BF3125) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 3 } {
                                         set value_list(890,BF3131)  [expr $value_list(890,BF3131) + $tmp_call_counts       ]
                                         set value_list(890,BF3132)  [expr $value_list(890,BF3132) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF3134)  [expr $value_list(890,BF3134) + $tmp_call_counts       ]
                                         	set value_list(890,BF3135)  [expr $value_list(890,BF3135) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 4 } {
                                         set value_list(890,BF3141)  [expr $value_list(890,BF3141) + $tmp_call_counts       ]
                                         set value_list(890,BF3142)  [expr $value_list(890,BF3142) + $tmp_call_duration_m   ]
                                         set value_list(890,BF3143)  [expr $value_list(890,BF3143) + $tmp_call_duration_s   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF3144)  [expr $value_list(890,BF3144) + $tmp_call_counts       ]
                                         	set value_list(890,BF3145)  [expr $value_list(890,BF3145) + $tmp_call_duration_m   ]
                                         	set value_list(890,BF3146)  [expr $value_list(890,BF3146) + $tmp_call_duration_s   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 5 } {
                                         set value_list(890,BF3151)  [expr $value_list(890,BF3151) + $tmp_call_counts       ]
                                         set value_list(890,BF3152)  [expr $value_list(890,BF3152) + $tmp_call_duration_m   ]
                                         set value_list(890,BF3153)  [expr $value_list(890,BF3153) + $tmp_call_duration_s   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF3154)  [expr $value_list(890,BF3154) + $tmp_call_counts       ]
                                         	set value_list(890,BF3155)  [expr $value_list(890,BF3155) + $tmp_call_duration_m   ]
                                         	set value_list(890,BF3156)  [expr $value_list(890,BF3156) + $tmp_call_duration_s   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id != "" } {
                                         set value_list(890,BF3101)  [expr $value_list(890,BF3101) + $tmp_call_counts       ]
                                         set value_list(890,BF3102)  [expr $value_list(890,BF3102) + $tmp_call_duration_m   ]
                                         set value_list(890,BF3103)  [expr $value_list(890,BF3103) + $tmp_call_duration_s   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF3104)  [expr $value_list(890,BF3104) + $tmp_call_counts       ]
                                         	set value_list(890,BF3105)  [expr $value_list(890,BF3105) + $tmp_call_duration_m   ]
                                         	set value_list(890,BF3106)  [expr $value_list(890,BF3106) + $tmp_call_duration_s   ]
 						  		 }
                             }
              }
    }
#其中：本地品牌客户数
       foreach   other4	 $brandid_other4  {
             if {$tmp_brand_id == $other4 } {
                          if {$tmp_tolltype_id == 1 } {
                                         set value_list(890,BF3111)  [expr $value_list(890,BF3111) + $tmp_call_counts       ]
                                         set value_list(890,BF3112)  [expr $value_list(890,BF3112) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF3114)  [expr $value_list(890,BF3114) + $tmp_call_counts       ]
                                         	set value_list(890,BF3115)  [expr $value_list(890,BF3115) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 2 } {
                                         set value_list(890,BF3121)  [expr $value_list(890,BF3121) + $tmp_call_counts       ]
                                         set value_list(890,BF3122)  [expr $value_list(890,BF3122) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF3124)  [expr $value_list(890,BF3124) + $tmp_call_counts       ]
                                         	set value_list(890,BF3125)  [expr $value_list(890,BF3125) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 3 } {
                                         set value_list(890,BF3131)  [expr $value_list(890,BF3131) + $tmp_call_counts       ]
                                         set value_list(890,BF3132)  [expr $value_list(890,BF3132) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF3134)  [expr $value_list(890,BF3134) + $tmp_call_counts       ]
                                         	set value_list(890,BF3135)  [expr $value_list(890,BF3135) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 4 } {
                                         set value_list(890,BF3141)  [expr $value_list(890,BF3141) + $tmp_call_counts       ]
                                         set value_list(890,BF3142)  [expr $value_list(890,BF3142) + $tmp_call_duration_m   ]
                                         set value_list(890,BF3143)  [expr $value_list(890,BF3143) + $tmp_call_duration_s   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF3144)  [expr $value_list(890,BF3144) + $tmp_call_counts       ]
                                         	set value_list(890,BF3145)  [expr $value_list(890,BF3145) + $tmp_call_duration_m   ]
                                         	set value_list(890,BF3146)  [expr $value_list(890,BF3146) + $tmp_call_duration_s   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 5 } {
                                         set value_list(890,BF3151)  [expr $value_list(890,BF3151) + $tmp_call_counts       ]
                                         set value_list(890,BF3152)  [expr $value_list(890,BF3152) + $tmp_call_duration_m   ]
                                         set value_list(890,BF3153)  [expr $value_list(890,BF3153) + $tmp_call_duration_s   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF3154)  [expr $value_list(890,BF3154) + $tmp_call_counts       ]
                                         	set value_list(890,BF3155)  [expr $value_list(890,BF3155) + $tmp_call_duration_m   ]
                                         	set value_list(890,BF3156)  [expr $value_list(890,BF3156) + $tmp_call_duration_s   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id != "" } {
                                         set value_list(890,BF3101)  [expr $value_list(890,BF3101) + $tmp_call_counts       ]
                                         set value_list(890,BF3102)  [expr $value_list(890,BF3102) + $tmp_call_duration_m   ]
                                         set value_list(890,BF3103)  [expr $value_list(890,BF3103) + $tmp_call_duration_s   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF3104)  [expr $value_list(890,BF3104) + $tmp_call_counts       ]
                                         	set value_list(890,BF3105)  [expr $value_list(890,BF3105) + $tmp_call_duration_m   ]
                                         	set value_list(890,BF3106)  [expr $value_list(890,BF3106) + $tmp_call_duration_s   ]
 						  		 }
                             }
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
###=====================================全球通客户国内漫游通话情况=====================================
#2、主叫构成合计	    BB3201 BB3204	BB3202 BB3205	BB3203 BB3206
#去本公司移动合计	    BB3301 BB3304	BB3302 BB3305	-	-   72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78
#其中：本地通话	      BB3311 BB3314	BB3312 BB3315	-	-
#      省内长途	      BB3321 BB3324	BB3322 BB3325	-	-
#      省际长途	      BB3331 BB3334	BB3332 BB3335	-	-
#去中国铁通固定合计	  BB3801 BB3804	BB3802 BB3805	-	-   3 中国铁通固定
#其中：本地通话	      BB3811 BB3814	BB3812 BB3815	-	-
#      省内长途	      BB3821 BB3824	BB3822 BB3825	-	-
#      省际长途	      BB3831 BB3834	BB3832 BB3835	-	-
#去中国联通移动合计	  BB3501 BB3504	BB3502 BB3505	-	-   13 中国联通GSM
#其中：本地通话	      BB3511 BB3514	BB3512 BB3515	-	-
#      省内长途    	  BB3521 BB3524	BB3522 BB3525	-	-
#      省际长途	      BB3531 BB3534	BB3532 BB3535	-	-
#去中国联通固定合计	  BB3701 BB3704	BB3702 BB3705	-	-   115 网通北方十省、2 中国联通固定
#其中：本地通话	      BB3711 BB3714	BB3712 BB3715	-	-
#      省内长途	      BB3721 BB3724	BB3722 BB3725	-	-
#      省际长途	      BB3731 BB3734	BB3732 BB3735	-	-
#去中国电信移动合计	  BB3601 BB3604	BB3602 BB3605	-	-    14 中国联通CDMA
#其中：本地通话	      BB3611 BB3614	BB3612 BB3615	-	-
#      省内长途	      BB3621 BB3624	BB3622 BB3625	-	-
#      省际长途	      BB3631 BB3634	BB3632 BB3635	-	-
#去中国电信固定合计	  BB3401 BB3404	BB3402 BB3405	-	-    1 中国电信固定(不含小灵通)、116 西藏好易通、4 中国电信小灵通
#其中：本地通话	      BB3411 BB3414	BB3412 BB3415	-	-
#      省内长途	      BB3421 BB3424	BB3422 BB3425	-	-
#      省际长途	      BB3431 BB3434	BB3432 BB3435	-	-
#其他去话	            BB3901 BB3904	BB3902 BB3905	BB3903 BB3906
#and a.roamtype_id in (1,4,6,8) and a.CALLTYPE_ID in (0,2,3) and a.TOLLTYPE_ID < 100

 	set sql_buf01 "select       b.crm_brand_id3,
 	                            case
 	                                 when a.TOLLTYPE_ID in (0,1,2) then
 	                                     case
 	                                         when a.OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
                                           when a.opposite_id in (3)           then 2
                                           when a.opposite_id in (13)          then 3
                                           when a.opposite_id in (115,2)       then 4
                                           when a.opposite_id in (14)          then 5
                                           when a.opposite_id in (1,4,116)     then 6
                                           else 0
                                       end
                                   else 0
                              end,
                              case when a.TOLLTYPE_ID in (0) then 1
                                   when a.TOLLTYPE_ID in (1) then 2
                                   when a.TOLLTYPE_ID in (2) then 3
                               else 0 end,a.mns_type,
                  	          sum(a.CALL_COUNTS)-sum(case when a.rate_prod_id is null then a.CALL_COUNTS else 0 end)*2,
                  	          sum(a.call_duration_m),
                  	          sum(a.call_duration_s)
                 from dw_call_$year$month a ,dw_product_$year$month b
                 where a.user_id=b.user_id
                 and a.roamtype_id in (1,4,6,8) and a.CALLTYPE_ID in (0,2,3) and a.TOLLTYPE_ID < 100
                 group by
                             b.crm_brand_id3,
 	                           case
 	                                 when a.TOLLTYPE_ID in (0,1,2) then
 	                                     case
 	                                         when a.OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
                                           when a.opposite_id in (3)           then 2
                                           when a.opposite_id in (13)          then 3
                                           when a.opposite_id in (115,2)       then 4
                                           when a.opposite_id in (14)          then 5
                                           when a.opposite_id in (1,4,116)     then 6
                                           else 0
                                       end
                                   else 0
                              end,
                              case when a.TOLLTYPE_ID in (0) then 1
                                   when a.TOLLTYPE_ID in (1) then 2
                                   when a.TOLLTYPE_ID in (2) then 3
                               else 0 end,a.mns_type"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id                   [lindex $this_row 0]
	        set     tmp_opposite_id                [lindex $this_row 1]
	        set     tmp_tolltype_id                [lindex $this_row 2]
	        set     tmp_td_mark                	  [lindex $this_row 3]
	        set     tmp_call_counts                [lindex $this_row 4]
		   set	 tmp_call_duration_m            [lindex $this_row 5]
		   set	 tmp_call_duration_s            [lindex $this_row 6]

#其中：全球通
   foreach global_all $brandid_global_all {
          if {$tmp_brand_id == $global_all } {
                if {$tmp_opposite_id == 1 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BB3311)  [expr $value_list(890,BB3311) + $tmp_call_counts            ]
                                set value_list(890,BB3312)  [expr $value_list(890,BB3312) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3314)  [expr $value_list(890,BB3314) + $tmp_call_counts            ]
                                	set value_list(890,BB3315)  [expr $value_list(890,BB3315) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BB3321)  [expr $value_list(890,BB3321) + $tmp_call_counts            ]
                                set value_list(890,BB3322)  [expr $value_list(890,BB3322) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3324)  [expr $value_list(890,BB3324) + $tmp_call_counts            ]
                                	set value_list(890,BB3325)  [expr $value_list(890,BB3325) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BB3331)  [expr $value_list(890,BB3331) + $tmp_call_counts            ]
                                set value_list(890,BB3332)  [expr $value_list(890,BB3332) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3334)  [expr $value_list(890,BB3334) + $tmp_call_counts            ]
                                	set value_list(890,BB3335)  [expr $value_list(890,BB3335) + $tmp_call_duration_m        ]
 						  }
                            }
#======去本公司移动合计========
                                set value_list(890,BB3301)  [expr $value_list(890,BB3301) + $tmp_call_counts            ]
                                set value_list(890,BB3302)  [expr $value_list(890,BB3302) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3304)  [expr $value_list(890,BB3304) + $tmp_call_counts            ]
                                	set value_list(890,BB3305)  [expr $value_list(890,BB3305) + $tmp_call_duration_m        ]
 						  }
#==============================
             }
          if {$tmp_opposite_id == 2 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BB3811)  [expr $value_list(890,BB3811) + $tmp_call_counts            ]
                                set value_list(890,BB3812)  [expr $value_list(890,BB3812) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3814)  [expr $value_list(890,BB3814) + $tmp_call_counts            ]
                                	set value_list(890,BB3815)  [expr $value_list(890,BB3815) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BB3821)  [expr $value_list(890,BB3821) + $tmp_call_counts            ]
                                set value_list(890,BB3822)  [expr $value_list(890,BB3822) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3824)  [expr $value_list(890,BB3824) + $tmp_call_counts            ]
                                	set value_list(890,BB3825)  [expr $value_list(890,BB3825) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BB3831)  [expr $value_list(890,BB3831) + $tmp_call_counts            ]
                                set value_list(890,BB3832)  [expr $value_list(890,BB3832) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3834)  [expr $value_list(890,BB3834) + $tmp_call_counts            ]
                                	set value_list(890,BB3835)  [expr $value_list(890,BB3835) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国铁通固定合计========
                                set value_list(890,BB3801)  [expr $value_list(890,BB3801) + $tmp_call_counts            ]
                                set value_list(890,BB3802)  [expr $value_list(890,BB3802) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3804)  [expr $value_list(890,BB3804) + $tmp_call_counts            ]
                                	set value_list(890,BB3805)  [expr $value_list(890,BB3805) + $tmp_call_duration_m        ]
 						  }
#================================
            }
          if {$tmp_opposite_id == 3 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BB3511)  [expr $value_list(890,BB3511) + $tmp_call_counts            ]
                                set value_list(890,BB3512)  [expr $value_list(890,BB3512) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3514)  [expr $value_list(890,BB3514) + $tmp_call_counts            ]
                                	set value_list(890,BB3515)  [expr $value_list(890,BB3515) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BB3521)  [expr $value_list(890,BB3521) + $tmp_call_counts            ]
                                set value_list(890,BB3522)  [expr $value_list(890,BB3522) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3524)  [expr $value_list(890,BB3524) + $tmp_call_counts            ]
                                	set value_list(890,BB3525)  [expr $value_list(890,BB3525) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BB3531)  [expr $value_list(890,BB3531) + $tmp_call_counts            ]
                                set value_list(890,BB3532)  [expr $value_list(890,BB3532) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3534)  [expr $value_list(890,BB3534) + $tmp_call_counts            ]
                                	set value_list(890,BB3535)  [expr $value_list(890,BB3535) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国联通移动合计========
                                set value_list(890,BB3501)  [expr $value_list(890,BB3501) + $tmp_call_counts            ]
                                set value_list(890,BB3502)  [expr $value_list(890,BB3502) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3504)  [expr $value_list(890,BB3504) + $tmp_call_counts            ]
                                	set value_list(890,BB3505)  [expr $value_list(890,BB3505) + $tmp_call_duration_m        ]
 						  }
             }
#==============================
          if {$tmp_opposite_id == 4 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BB3711)  [expr $value_list(890,BB3711) + $tmp_call_counts            ]
                                set value_list(890,BB3712)  [expr $value_list(890,BB3712) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3714)  [expr $value_list(890,BB3714) + $tmp_call_counts            ]
                                	set value_list(890,BB3715)  [expr $value_list(890,BB3715) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BB3721)  [expr $value_list(890,BB3721) + $tmp_call_counts            ]
                                set value_list(890,BB3722)  [expr $value_list(890,BB3722) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3724)  [expr $value_list(890,BB3724) + $tmp_call_counts            ]
                                	set value_list(890,BB3725)  [expr $value_list(890,BB3725) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BB3731)  [expr $value_list(890,BB3731) + $tmp_call_counts            ]
                                set value_list(890,BB3732)  [expr $value_list(890,BB3732) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3734)  [expr $value_list(890,BB3734) + $tmp_call_counts            ]
                                	set value_list(890,BB3735)  [expr $value_list(890,BB3735) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国联通固定合计========
                                set value_list(890,BB3701)  [expr $value_list(890,BB3701) + $tmp_call_counts            ]
                                set value_list(890,BB3702)  [expr $value_list(890,BB3702) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3704)  [expr $value_list(890,BB3704) + $tmp_call_counts            ]
                                	set value_list(890,BB3705)  [expr $value_list(890,BB3705) + $tmp_call_duration_m        ]
 						  }
             }
#==============================
          if {$tmp_opposite_id == 5 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BB3611)  [expr $value_list(890,BB3611) + $tmp_call_counts            ]
                                set value_list(890,BB3612)  [expr $value_list(890,BB3612) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3614)  [expr $value_list(890,BB3614) + $tmp_call_counts            ]
                                	set value_list(890,BB3615)  [expr $value_list(890,BB3615) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BB3621)  [expr $value_list(890,BB3621) + $tmp_call_counts            ]
                                set value_list(890,BB3622)  [expr $value_list(890,BB3622) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3624)  [expr $value_list(890,BB3624) + $tmp_call_counts            ]
                                	set value_list(890,BB3625)  [expr $value_list(890,BB3625) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BB3631)  [expr $value_list(890,BB3631) + $tmp_call_counts            ]
                                set value_list(890,BB3632)  [expr $value_list(890,BB3632) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3634)  [expr $value_list(890,BB3634) + $tmp_call_counts            ]
                                	set value_list(890,BB3635)  [expr $value_list(890,BB3635) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国电信移动合计========
                                set value_list(890,BB3601)  [expr $value_list(890,BB3601) + $tmp_call_counts            ]
                                set value_list(890,BB3602)  [expr $value_list(890,BB3602) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3604)  [expr $value_list(890,BB3604) + $tmp_call_counts            ]
                                	set value_list(890,BB3605)  [expr $value_list(890,BB3605) + $tmp_call_duration_m        ]
 						  }
             }
#================================
          if {$tmp_opposite_id == 6 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BB3411)  [expr $value_list(890,BB3411) + $tmp_call_counts            ]
                                set value_list(890,BB3412)  [expr $value_list(890,BB3412) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3414)  [expr $value_list(890,BB3414) + $tmp_call_counts            ]
                                	set value_list(890,BB3415)  [expr $value_list(890,BB3415) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BB3421)  [expr $value_list(890,BB3421) + $tmp_call_counts            ]
                                set value_list(890,BB3422)  [expr $value_list(890,BB3422) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3424)  [expr $value_list(890,BB3424) + $tmp_call_counts            ]
                                	set value_list(890,BB3425)  [expr $value_list(890,BB3425) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BB3431)  [expr $value_list(890,BB3431) + $tmp_call_counts            ]
                                set value_list(890,BB3432)  [expr $value_list(890,BB3432) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3434)  [expr $value_list(890,BB3434) + $tmp_call_counts            ]
                                	set value_list(890,BB3435)  [expr $value_list(890,BB3435) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国电信固定合计========
                                set value_list(890,BB3401)  [expr $value_list(890,BB3401) + $tmp_call_counts            ]
                                set value_list(890,BB3402)  [expr $value_list(890,BB3402) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3404)  [expr $value_list(890,BB3404) + $tmp_call_counts            ]
                                	set value_list(890,BB3405)  [expr $value_list(890,BB3405) + $tmp_call_duration_m        ]
 						  }
             }
#================================
          if {$tmp_opposite_id  == 0 } {
                                set value_list(890,BB3901)  [expr $value_list(890,BB3901) + $tmp_call_counts            ]
                                set value_list(890,BB3902)  [expr $value_list(890,BB3902) + $tmp_call_duration_m        ]
                                set value_list(890,BB3903)  [expr $value_list(890,BB3903) + $tmp_call_duration_s        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB3904)  [expr $value_list(890,BB3904) + $tmp_call_counts            ]
                                	set value_list(890,BB3905)  [expr $value_list(890,BB3905) + $tmp_call_duration_m        ]
                                	set value_list(890,BB3906)  [expr $value_list(890,BB3906) + $tmp_call_duration_s        ]
 						  }
          }
      set value_list(890,BB3201)  [expr $value_list(890,BB3201) + $tmp_call_counts            ]
      set value_list(890,BB3202)  [expr $value_list(890,BB3202) + $tmp_call_duration_m        ]
      set value_list(890,BB3203)  [expr $value_list(890,BB3203) + $tmp_call_duration_s        ]
 	 if {$tmp_td_mark == 1} {
      	set value_list(890,BB3204)  [expr $value_list(890,BB3204) + $tmp_call_counts            ]
      	set value_list(890,BB3205)  [expr $value_list(890,BB3205) + $tmp_call_duration_m        ]
      	set value_list(890,BB3206)  [expr $value_list(890,BB3206) + $tmp_call_duration_s        ]
 	 }

      set value_list(890,BB3001)  [expr $value_list(890,BB3001) + $tmp_call_counts            ]
      set value_list(890,BB3002)  [expr $value_list(890,BB3002) + $tmp_call_duration_m        ]
      set value_list(890,BB3003)  [expr $value_list(890,BB3003) + $tmp_call_duration_s        ]
 	 if {$tmp_td_mark == 1} {
      	set value_list(890,BB3004)  [expr $value_list(890,BB3004) + $tmp_call_counts            ]
      	set value_list(890,BB3005)  [expr $value_list(890,BB3005) + $tmp_call_duration_m        ]
      	set value_list(890,BB3006)  [expr $value_list(890,BB3006) + $tmp_call_duration_s        ]
 	 }
#      puts $value_list(890,BB3001)
     }
  }
###=====================================动感地带客户国内漫游通话情况=================================
#2、主叫构成合计	  BE3201	BE3204	BE3202	BE3205	BE3203	BE3206
#去本公司移动合计	  BE3301	BE3304	BE3302	BE3305	-	-
#其中：本地通话	    BE3311	BE3314	BE3312	BE3315	-	-
#      省内长途	    BE3321	BE3324	BE3322	BE3325	-	-
#      省际长途	    BE3331	BE3334	BE3332	BE3335	-	-
#去中国铁通固定合计	BE3801	BE3804	BE3802	BE3805	-	-
#其中：本地通话	    BE3811	BE3814	BE3812	BE3815	-	-
#      省内长途	    BE3821	BE3824	BE3822	BE3825	-	-
#      省际长途	    BE3831	BE3834	BE3832	BE3835	-	-
#去中国联通移动合计	BE3501	BE3504	BE3502	BE3505	-	-
#其中：本地通话	    BE3511	BE3514	BE3512	BE3515	-	-
#      省内长途	    BE3521	BE3524	BE3522	BE3525	-	-
#      省际长途	    BE3531	BE3534	BE3532	BE3535	-	-
#去中国联通固定合计	BE3701	BE3704	BE3702	BE3705	-	-
#其中：本地通话	    BE3711	BE3714	BE3712	BE3715	-	-
#      省内长途	    BE3721	BE3724	BE3722	BE3725	-	-
#      省际长途	    BE3731	BE3734	BE3732	BE3735	-	-
#去中国电信移动合计	BE3601	BE3604	BE3602	BE3605	-	-
#其中：本地通话	    BE3611	BE3614	BE3612	BE3615	-	-
#      省内长途	    BE3621	BE3624	BE3622	BE3625	-	-
#      省际长途	    BE3631	BE3634	BE3632	BE3635	-	-
#去中国电信固定合计	BE3401	BE3404	BE3402	BE3405	-	-
#其中：本地通话	    BE3411	BE3414	BE3412	BE3415	-	-
#      省内长途	    BE3421	BE3424	BE3422	BE3425	-	-
#      省际长途	    BE3431	BE3434	BE3432	BE3435	-	-
#其他去话	          BE3901	BE3904	BE3902	BE3905	BE3903	BE3906

  foreach mzone_all $brandid_mzone_all {
             if {$tmp_brand_id == $mzone_all } {
               if {$tmp_opposite_id == 1 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BE3311)  [expr $value_list(890,BE3311) + $tmp_call_counts            ]
                                set value_list(890,BE3312)  [expr $value_list(890,BE3312) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3314)  [expr $value_list(890,BE3314) + $tmp_call_counts            ]
                                	set value_list(890,BE3315)  [expr $value_list(890,BE3315) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BE3321)  [expr $value_list(890,BE3321) + $tmp_call_counts            ]
                                set value_list(890,BE3322)  [expr $value_list(890,BE3322) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3324)  [expr $value_list(890,BE3324) + $tmp_call_counts            ]
                                	set value_list(890,BE3325)  [expr $value_list(890,BE3325) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BE3331)  [expr $value_list(890,BE3331) + $tmp_call_counts            ]
                                set value_list(890,BE3332)  [expr $value_list(890,BE3332) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3334)  [expr $value_list(890,BE3334) + $tmp_call_counts            ]
                                	set value_list(890,BE3335)  [expr $value_list(890,BE3335) + $tmp_call_duration_m        ]
 						  }
                            }
#======去本公司移动合计========
                         set value_list(890,BE3301)  [expr $value_list(890,BE3301) + $tmp_call_counts            ]
                         set value_list(890,BE3302)  [expr $value_list(890,BE3302) + $tmp_call_duration_m        ]
 					if {$tmp_td_mark == 1} {
                         	set value_list(890,BE3304)  [expr $value_list(890,BE3304) + $tmp_call_counts            ]
                         	set value_list(890,BE3305)  [expr $value_list(890,BE3305) + $tmp_call_duration_m        ]
 					}
#==============================
             }
          if {$tmp_opposite_id == 2 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BE3811)  [expr $value_list(890,BE3811) + $tmp_call_counts            ]
                                set value_list(890,BE3812)  [expr $value_list(890,BE3812) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3814)  [expr $value_list(890,BE3814) + $tmp_call_counts            ]
                                	set value_list(890,BE3815)  [expr $value_list(890,BE3815) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BE3821)  [expr $value_list(890,BE3821) + $tmp_call_counts            ]
                                set value_list(890,BE3822)  [expr $value_list(890,BE3822) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3824)  [expr $value_list(890,BE3824) + $tmp_call_counts            ]
                                	set value_list(890,BE3825)  [expr $value_list(890,BE3825) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BE3831)  [expr $value_list(890,BE3831) + $tmp_call_counts            ]
                                set value_list(890,BE3832)  [expr $value_list(890,BE3832) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3834)  [expr $value_list(890,BE3834) + $tmp_call_counts            ]
                                	set value_list(890,BE3835)  [expr $value_list(890,BE3835) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国铁通固定合计========
                                set value_list(890,BE3801)  [expr $value_list(890,BE3801) + $tmp_call_counts            ]
                                set value_list(890,BE3802)  [expr $value_list(890,BE3802) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3804)  [expr $value_list(890,BE3804) + $tmp_call_counts            ]
                                	set value_list(890,BE3805)  [expr $value_list(890,BE3805) + $tmp_call_duration_m        ]
 						  }
#================================
            }
          if {$tmp_opposite_id == 3 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BE3511)  [expr $value_list(890,BE3511) + $tmp_call_counts            ]
                                set value_list(890,BE3512)  [expr $value_list(890,BE3512) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3514)  [expr $value_list(890,BE3514) + $tmp_call_counts            ]
                                	set value_list(890,BE3515)  [expr $value_list(890,BE3515) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BE3521)  [expr $value_list(890,BE3521) + $tmp_call_counts            ]
                                set value_list(890,BE3522)  [expr $value_list(890,BE3522) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3524)  [expr $value_list(890,BE3524) + $tmp_call_counts            ]
                                	set value_list(890,BE3525)  [expr $value_list(890,BE3525) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BE3531)  [expr $value_list(890,BE3531) + $tmp_call_counts            ]
                                set value_list(890,BE3532)  [expr $value_list(890,BE3532) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3534)  [expr $value_list(890,BE3534) + $tmp_call_counts            ]
                                	set value_list(890,BE3535)  [expr $value_list(890,BE3535) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国联通移动合计========
                                set value_list(890,BE3501)  [expr $value_list(890,BE3501) + $tmp_call_counts            ]
                                set value_list(890,BE3502)  [expr $value_list(890,BE3502) + $tmp_call_duration_m            ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3504)  [expr $value_list(890,BE3504) + $tmp_call_counts            ]
                                	set value_list(890,BE3505)  [expr $value_list(890,BE3505) + $tmp_call_duration_m            ]
 						  }
          }
#==============================
          if {$tmp_opposite_id == 4 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BE3711)  [expr $value_list(890,BE3711) + $tmp_call_counts            ]
                                set value_list(890,BE3712)  [expr $value_list(890,BE3712) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3714)  [expr $value_list(890,BE3714) + $tmp_call_counts            ]
                                	set value_list(890,BE3715)  [expr $value_list(890,BE3715) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BE3721)  [expr $value_list(890,BE3721) + $tmp_call_counts            ]
                                set value_list(890,BE3722)  [expr $value_list(890,BE3722) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3724)  [expr $value_list(890,BE3724) + $tmp_call_counts            ]
                                	set value_list(890,BE3725)  [expr $value_list(890,BE3725) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BE3731)  [expr $value_list(890,BE3731) + $tmp_call_counts            ]
                                set value_list(890,BE3732)  [expr $value_list(890,BE3732) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3734)  [expr $value_list(890,BE3734) + $tmp_call_counts            ]
                                	set value_list(890,BE3735)  [expr $value_list(890,BE3735) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国联通固定合计========
                                set value_list(890,BE3701)  [expr $value_list(890,BE3701) + $tmp_call_counts            ]
                                set value_list(890,BE3702)  [expr $value_list(890,BE3702) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3704)  [expr $value_list(890,BE3704) + $tmp_call_counts            ]
                                	set value_list(890,BE3705)  [expr $value_list(890,BE3705) + $tmp_call_duration_m        ]
 						  }
             }
#==============================
          if {$tmp_opposite_id == 5 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BE3611)  [expr $value_list(890,BE3611) + $tmp_call_counts            ]
                                set value_list(890,BE3612)  [expr $value_list(890,BE3612) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3614)  [expr $value_list(890,BE3614) + $tmp_call_counts            ]
                                	set value_list(890,BE3615)  [expr $value_list(890,BE3615) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BE3621)  [expr $value_list(890,BE3621) + $tmp_call_counts            ]
                                set value_list(890,BE3622)  [expr $value_list(890,BE3622) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3624)  [expr $value_list(890,BE3624) + $tmp_call_counts            ]
                                	set value_list(890,BE3625)  [expr $value_list(890,BE3625) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BE3631)  [expr $value_list(890,BE3631) + $tmp_call_counts            ]
                                set value_list(890,BE3632)  [expr $value_list(890,BE3632) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3634)  [expr $value_list(890,BE3634) + $tmp_call_counts            ]
                                	set value_list(890,BE3635)  [expr $value_list(890,BE3635) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国电信移动合计========
                                set value_list(890,BE3601)  [expr $value_list(890,BE3601) + $tmp_call_counts            ]
                                set value_list(890,BE3602)  [expr $value_list(890,BE3602) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3604)  [expr $value_list(890,BE3604) + $tmp_call_counts            ]
                                	set value_list(890,BE3605)  [expr $value_list(890,BE3605) + $tmp_call_duration_m        ]
 						  }
             }
#================================
          if {$tmp_opposite_id == 6 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BE3411)  [expr $value_list(890,BE3411) + $tmp_call_counts            ]
                                set value_list(890,BE3412)  [expr $value_list(890,BE3412) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3414)  [expr $value_list(890,BE3414) + $tmp_call_counts            ]
                                	set value_list(890,BE3415)  [expr $value_list(890,BE3415) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BE3421)  [expr $value_list(890,BE3421) + $tmp_call_counts            ]
                                set value_list(890,BE3422)  [expr $value_list(890,BE3422) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3424)  [expr $value_list(890,BE3424) + $tmp_call_counts            ]
                                	set value_list(890,BE3425)  [expr $value_list(890,BE3425) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BE3431)  [expr $value_list(890,BE3431) + $tmp_call_counts            ]
                                set value_list(890,BE3432)  [expr $value_list(890,BE3432) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3434)  [expr $value_list(890,BE3434) + $tmp_call_counts            ]
                                	set value_list(890,BE3435)  [expr $value_list(890,BE3435) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国电信固定合计========
                                set value_list(890,BE3401)  [expr $value_list(890,BE3401) + $tmp_call_counts            ]
                                set value_list(890,BE3402)  [expr $value_list(890,BE3402) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3404)  [expr $value_list(890,BE3404) + $tmp_call_counts            ]
                                	set value_list(890,BE3405)  [expr $value_list(890,BE3405) + $tmp_call_duration_m        ]
 						  }
             }
#================================
          if {$tmp_opposite_id  == 0 } {
                                set value_list(890,BE3901)  [expr $value_list(890,BE3901) + $tmp_call_counts            ]
                                set value_list(890,BE3902)  [expr $value_list(890,BE3902) + $tmp_call_duration_m        ]
                                set value_list(890,BE3903)  [expr $value_list(890,BE3903) + $tmp_call_duration_s        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE3904)  [expr $value_list(890,BE3904) + $tmp_call_counts            ]
                                	set value_list(890,BE3905)  [expr $value_list(890,BE3905) + $tmp_call_duration_m        ]
                                	set value_list(890,BE3906)  [expr $value_list(890,BE3906) + $tmp_call_duration_s        ]
 						  }
          }
          set value_list(890,BE3201)  [expr $value_list(890,BE3201) + $tmp_call_counts            ]
          set value_list(890,BE3202)  [expr $value_list(890,BE3202) + $tmp_call_duration_m        ]
          set value_list(890,BE3203)  [expr $value_list(890,BE3203) + $tmp_call_duration_s        ]
 		if {$tmp_td_mark == 1} {
          	set value_list(890,BE3204)  [expr $value_list(890,BE3204) + $tmp_call_counts            ]
          	set value_list(890,BE3205)  [expr $value_list(890,BE3205) + $tmp_call_duration_m        ]
          	set value_list(890,BE3206)  [expr $value_list(890,BE3206) + $tmp_call_duration_s        ]
 		}

          set value_list(890,BE3001)  [expr $value_list(890,BE3001) + $tmp_call_counts            ]
          set value_list(890,BE3002)  [expr $value_list(890,BE3002) + $tmp_call_duration_m        ]
          set value_list(890,BE3003)  [expr $value_list(890,BE3003) + $tmp_call_duration_s        ]
 		if {$tmp_td_mark == 1} {
          	set value_list(890,BE3004)  [expr $value_list(890,BE3004) + $tmp_call_counts            ]
          	set value_list(890,BE3005)  [expr $value_list(890,BE3005) + $tmp_call_duration_m        ]
          	set value_list(890,BE3006)  [expr $value_list(890,BE3006) + $tmp_call_duration_s        ]
 		}
     }
  }
###=====================================神州行客户国内漫游通话情况==================================
#2、主叫构成合计	BF3201	BF3204	BF3202	BF3205	BF3203	BF3206
#去本公司移动合计	BF3301	BF3304	BF3302	BF3305	-	-
#其中：本地通话	  BF3311	BF3314	BF3312	BF3315	-	-
#      省内长途	  BF3321	BF3324	BF3322	BF3325	-	-
#      省际长途	  BF3331	BF3334	BF3332	BF3335	-	-
#去中国铁通固定合计	BF3801	BF3804	BF3802	BF3805	-	-
#其中：本地通话	    BF3811	BF3814	BF3812	BF3815	-	-
#      省内长途	    BF3821	BF3824	BF3822	BF3825	-	-
#      省际长途	    BF3831	BF3834	BF3832	BF3835	-	-
#去中国联通移动合计	BF3501	BF3504	BF3502	BF3505	-	-
#其中：本地通话	    BF3511	BF3514	BF3512	BF3515	-	-
#      省内长途	    BF3521	BF3524	BF3522	BF3525	-	-
#      省际长途	    BF3531	BF3534	BF3532	BF3535	-	-
#去中国联通固定合计	BF3701	BF3704	BF3702	BF3705	-	-
#其中：本地通话	    BF3711	BF3714	BF3712	BF3715	-	-
#      省内长途	    BF3721	BF3724	BF3722	BF3725	-	-
#      省际长途	    BF3731	BF3734	BF3732	BF3735	-	-
#去中国电信移动合计	BF3601	BF3604	BF3602	BF3605	-	-
#其中：本地通话	    BF3611	BF3614	BF3612	BF3615	-	-
#      省内长途	    BF3621	BF3624	BF3622	BF3625	-	-
#      省际长途	    BF3631	BF3634	BF3632	BF3635	-	-
#去中国电信固定合计	BF3401	BF3404	BF3402	BF3405	-	-
#其中：本地通话	    BF3411	BF3414	BF3412	BF3415	-	-
#      省内长途	    BF3421	BF3424	BF3422	BF3425	-	-
#      省际长途	    BF3431	BF3434	BF3432	BF3435	-	-
#其他去话	          BF3901	BF3904	BF3902	BF3905	BF3903	BF3906

     foreach china_all $brandid_china_all {
             if {$tmp_brand_id == $china_all } {
              if {$tmp_opposite_id == 1 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BF3311)  [expr $value_list(890,BF3311) + $tmp_call_counts            ]
                                set value_list(890,BF3312)  [expr $value_list(890,BF3312) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3314)  [expr $value_list(890,BF3314) + $tmp_call_counts            ]
                                	set value_list(890,BF3315)  [expr $value_list(890,BF3315) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BF3321)  [expr $value_list(890,BF3321) + $tmp_call_counts            ]
                                set value_list(890,BF3322)  [expr $value_list(890,BF3322) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3324)  [expr $value_list(890,BF3324) + $tmp_call_counts            ]
                                	set value_list(890,BF3325)  [expr $value_list(890,BF3325) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BF3331)  [expr $value_list(890,BF3331) + $tmp_call_counts            ]
                                set value_list(890,BF3332)  [expr $value_list(890,BF3332) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3334)  [expr $value_list(890,BF3334) + $tmp_call_counts            ]
                                	set value_list(890,BF3335)  [expr $value_list(890,BF3335) + $tmp_call_duration_m        ]
 						  }
                            }
#======去本公司移动合计========
                                set value_list(890,BF3301)  [expr $value_list(890,BF3301) + $tmp_call_counts            ]
                                set value_list(890,BF3302)  [expr $value_list(890,BF3302) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3304)  [expr $value_list(890,BF3304) + $tmp_call_counts            ]
                                	set value_list(890,BF3305)  [expr $value_list(890,BF3305) + $tmp_call_duration_m        ]
 						  }
#==============================
             }
          if {$tmp_opposite_id == 2 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BF3811)  [expr $value_list(890,BF3811) + $tmp_call_counts            ]
                                set value_list(890,BF3812)  [expr $value_list(890,BF3812) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3814)  [expr $value_list(890,BF3814) + $tmp_call_counts            ]
                                	set value_list(890,BF3815)  [expr $value_list(890,BF3815) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BF3821)  [expr $value_list(890,BF3821) + $tmp_call_counts            ]
                                set value_list(890,BF3822)  [expr $value_list(890,BF3822) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3824)  [expr $value_list(890,BF3824) + $tmp_call_counts            ]
                                	set value_list(890,BF3825)  [expr $value_list(890,BF3825) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BF3831)  [expr $value_list(890,BF3831) + $tmp_call_counts            ]
                                set value_list(890,BF3832)  [expr $value_list(890,BF3832) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3834)  [expr $value_list(890,BF3834) + $tmp_call_counts            ]
                                	set value_list(890,BF3835)  [expr $value_list(890,BF3835) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国铁通固定合计========
                                set value_list(890,BF3801)  [expr $value_list(890,BF3801) + $tmp_call_counts            ]
                                set value_list(890,BF3802)  [expr $value_list(890,BF3802) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3804)  [expr $value_list(890,BF3804) + $tmp_call_counts            ]
                                	set value_list(890,BF3805)  [expr $value_list(890,BF3805) + $tmp_call_duration_m        ]
 						  }
#================================
            }
          if {$tmp_opposite_id == 3 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BF3511)  [expr $value_list(890,BF3511) + $tmp_call_counts            ]
                                set value_list(890,BF3512)  [expr $value_list(890,BF3512) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3514)  [expr $value_list(890,BF3514) + $tmp_call_counts            ]
                                	set value_list(890,BF3515)  [expr $value_list(890,BF3515) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BF3521)  [expr $value_list(890,BF3521) + $tmp_call_counts            ]
                                set value_list(890,BF3522)  [expr $value_list(890,BF3522) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3524)  [expr $value_list(890,BF3524) + $tmp_call_counts            ]
                                	set value_list(890,BF3525)  [expr $value_list(890,BF3525) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BF3531)  [expr $value_list(890,BF3531) + $tmp_call_counts            ]
                                set value_list(890,BF3532)  [expr $value_list(890,BF3532) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3534)  [expr $value_list(890,BF3534) + $tmp_call_counts            ]
                                	set value_list(890,BF3535)  [expr $value_list(890,BF3535) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国联通移动合计========
                                set value_list(890,BF3501)  [expr $value_list(890,BF3501) + $tmp_call_counts            ]
                                set value_list(890,BF3502)  [expr $value_list(890,BF3502) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3504)  [expr $value_list(890,BF3504) + $tmp_call_counts            ]
                                	set value_list(890,BF3505)  [expr $value_list(890,BF3505) + $tmp_call_duration_m        ]
 						  }
             }
#==============================
          if {$tmp_opposite_id == 4 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BF3711)  [expr $value_list(890,BF3711) + $tmp_call_counts            ]
                                set value_list(890,BF3712)  [expr $value_list(890,BF3712) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3714)  [expr $value_list(890,BF3714) + $tmp_call_counts            ]
                                	set value_list(890,BF3715)  [expr $value_list(890,BF3715) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BF3721)  [expr $value_list(890,BF3721) + $tmp_call_counts            ]
                                set value_list(890,BF3722)  [expr $value_list(890,BF3722) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3724)  [expr $value_list(890,BF3724) + $tmp_call_counts            ]
                                	set value_list(890,BF3725)  [expr $value_list(890,BF3725) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BF3731)  [expr $value_list(890,BF3731) + $tmp_call_counts            ]
                                set value_list(890,BF3732)  [expr $value_list(890,BF3732) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3734)  [expr $value_list(890,BF3734) + $tmp_call_counts            ]
                                	set value_list(890,BF3735)  [expr $value_list(890,BF3735) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国联通固定合计========
                                set value_list(890,BF3701)  [expr $value_list(890,BF3701) + $tmp_call_counts            ]
                                set value_list(890,BF3702)  [expr $value_list(890,BF3702) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3704)  [expr $value_list(890,BF3704) + $tmp_call_counts            ]
                                	set value_list(890,BF3705)  [expr $value_list(890,BF3705) + $tmp_call_duration_m        ]
 						  }
             }
#==============================
          if {$tmp_opposite_id == 5 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BF3611)  [expr $value_list(890,BF3611) + $tmp_call_counts            ]
                                set value_list(890,BF3612)  [expr $value_list(890,BF3612) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3614)  [expr $value_list(890,BF3614) + $tmp_call_counts            ]
                                	set value_list(890,BF3615)  [expr $value_list(890,BF3615) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BF3621)  [expr $value_list(890,BF3621) + $tmp_call_counts            ]
                                set value_list(890,BF3622)  [expr $value_list(890,BF3622) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3624)  [expr $value_list(890,BF3624) + $tmp_call_counts            ]
                                	set value_list(890,BF3625)  [expr $value_list(890,BF3625) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BF3631)  [expr $value_list(890,BF3631) + $tmp_call_counts            ]
                                set value_list(890,BF3632)  [expr $value_list(890,BF3632) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3634)  [expr $value_list(890,BF3634) + $tmp_call_counts            ]
                                	set value_list(890,BF3635)  [expr $value_list(890,BF3635) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国电信移动合计========
                                set value_list(890,BF3601)  [expr $value_list(890,BF3601) + $tmp_call_counts            ]
                                set value_list(890,BF3602)  [expr $value_list(890,BF3602) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3604)  [expr $value_list(890,BF3604) + $tmp_call_counts            ]
                                	set value_list(890,BF3605)  [expr $value_list(890,BF3605) + $tmp_call_duration_m        ]
 						  }
             }
#================================
          if {$tmp_opposite_id == 6 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BF3411)  [expr $value_list(890,BF3411) + $tmp_call_counts            ]
                                set value_list(890,BF3412)  [expr $value_list(890,BF3412) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3414)  [expr $value_list(890,BF3414) + $tmp_call_counts            ]
                                	set value_list(890,BF3415)  [expr $value_list(890,BF3415) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BF3421)  [expr $value_list(890,BF3421) + $tmp_call_counts            ]
                                set value_list(890,BF3422)  [expr $value_list(890,BF3422) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3424)  [expr $value_list(890,BF3424) + $tmp_call_counts            ]
                                	set value_list(890,BF3425)  [expr $value_list(890,BF3425) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BF3431)  [expr $value_list(890,BF3431) + $tmp_call_counts            ]
                                set value_list(890,BF3432)  [expr $value_list(890,BF3432) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3434)  [expr $value_list(890,BF3434) + $tmp_call_counts            ]
                                	set value_list(890,BF3435)  [expr $value_list(890,BF3435) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国电信固定合计========
                                set value_list(890,BF3401)  [expr $value_list(890,BF3401) + $tmp_call_counts            ]
                                set value_list(890,BF3402)  [expr $value_list(890,BF3402) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3404)  [expr $value_list(890,BF3404) + $tmp_call_counts            ]
                                	set value_list(890,BF3405)  [expr $value_list(890,BF3405) + $tmp_call_duration_m        ]
 						  }
             }
#=======其他去话=================
          if {$tmp_opposite_id  == 0 } {
                                set value_list(890,BF3901)  [expr $value_list(890,BF3901) + $tmp_call_counts            ]
                                set value_list(890,BF3902)  [expr $value_list(890,BF3902) + $tmp_call_duration_m        ]
                                set value_list(890,BF3903)  [expr $value_list(890,BF3903) + $tmp_call_duration_s        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3904)  [expr $value_list(890,BF3904) + $tmp_call_counts            ]
                                	set value_list(890,BF3905)  [expr $value_list(890,BF3905) + $tmp_call_duration_m        ]
                                	set value_list(890,BF3906)  [expr $value_list(890,BF3906) + $tmp_call_duration_s        ]
 						  }
          }
          set value_list(890,BF3201)  [expr $value_list(890,BF3201) + $tmp_call_counts            ]
          set value_list(890,BF3202)  [expr $value_list(890,BF3202) + $tmp_call_duration_m        ]
          set value_list(890,BF3203)  [expr $value_list(890,BF3203) + $tmp_call_duration_s        ]
		if {$tmp_td_mark == 1} {
          	set value_list(890,BF3204)  [expr $value_list(890,BF3204) + $tmp_call_counts            ]
          	set value_list(890,BF3205)  [expr $value_list(890,BF3205) + $tmp_call_duration_m        ]
          	set value_list(890,BF3206)  [expr $value_list(890,BF3206) + $tmp_call_duration_s        ]
		}

          set value_list(890,BF3001)  [expr $value_list(890,BF3001) + $tmp_call_counts            ]
          set value_list(890,BF3002)  [expr $value_list(890,BF3002) + $tmp_call_duration_m        ]
          set value_list(890,BF3003)  [expr $value_list(890,BF3003) + $tmp_call_duration_s        ]
		if {$tmp_td_mark == 1} {
          	set value_list(890,BF3004)  [expr $value_list(890,BF3004) + $tmp_call_counts            ]
          	set value_list(890,BF3005)  [expr $value_list(890,BF3005) + $tmp_call_duration_m        ]
          	set value_list(890,BF3006)  [expr $value_list(890,BF3006) + $tmp_call_duration_s        ]
		}
     }
  }
#====================================================================================================
#其中：本地品牌客户数
       foreach   other4	 $brandid_other4  {
             if {$tmp_brand_id == $other4 } {
               if {$tmp_opposite_id == 1 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BF3311)  [expr $value_list(890,BF3311) + $tmp_call_counts            ]
                                set value_list(890,BF3312)  [expr $value_list(890,BF3312) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3314)  [expr $value_list(890,BF3314) + $tmp_call_counts            ]
                                	set value_list(890,BF3315)  [expr $value_list(890,BF3315) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BF3321)  [expr $value_list(890,BF3321) + $tmp_call_counts            ]
                                set value_list(890,BF3322)  [expr $value_list(890,BF3322) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3324)  [expr $value_list(890,BF3324) + $tmp_call_counts            ]
                                	set value_list(890,BF3325)  [expr $value_list(890,BF3325) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BF3331)  [expr $value_list(890,BF3331) + $tmp_call_counts            ]
                                set value_list(890,BF3332)  [expr $value_list(890,BF3332) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3334)  [expr $value_list(890,BF3334) + $tmp_call_counts            ]
                                	set value_list(890,BF3335)  [expr $value_list(890,BF3335) + $tmp_call_duration_m        ]
 						  }
                            }
#======去本公司移动合计========
                                set value_list(890,BF3301)  [expr $value_list(890,BF3301) + $tmp_call_counts            ]
                                set value_list(890,BF3302)  [expr $value_list(890,BF3302) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3304)  [expr $value_list(890,BF3304) + $tmp_call_counts            ]
                                	set value_list(890,BF3305)  [expr $value_list(890,BF3305) + $tmp_call_duration_m        ]
 						  }
#==============================
             }
          if {$tmp_opposite_id == 2 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BF3811)  [expr $value_list(890,BF3811) + $tmp_call_counts            ]
                                set value_list(890,BF3812)  [expr $value_list(890,BF3812) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3814)  [expr $value_list(890,BF3814) + $tmp_call_counts            ]
                                	set value_list(890,BF3815)  [expr $value_list(890,BF3815) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BF3821)  [expr $value_list(890,BF3821) + $tmp_call_counts            ]
                                set value_list(890,BF3822)  [expr $value_list(890,BF3822) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3824)  [expr $value_list(890,BF3824) + $tmp_call_counts            ]
                                	set value_list(890,BF3825)  [expr $value_list(890,BF3825) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BF3831)  [expr $value_list(890,BF3831) + $tmp_call_counts            ]
                                set value_list(890,BF3832)  [expr $value_list(890,BF3832) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3834)  [expr $value_list(890,BF3834) + $tmp_call_counts            ]
                                	set value_list(890,BF3835)  [expr $value_list(890,BF3835) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国铁通固定合计========
                                set value_list(890,BF3801)  [expr $value_list(890,BF3801) + $tmp_call_counts            ]
                                set value_list(890,BF3802)  [expr $value_list(890,BF3802) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3804)  [expr $value_list(890,BF3804) + $tmp_call_counts            ]
                                	set value_list(890,BF3805)  [expr $value_list(890,BF3805) + $tmp_call_duration_m        ]
 						  }
#================================
            }
          if {$tmp_opposite_id == 3 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BF3511)  [expr $value_list(890,BF3511) + $tmp_call_counts            ]
                                set value_list(890,BF3512)  [expr $value_list(890,BF3512) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3514)  [expr $value_list(890,BF3514) + $tmp_call_counts            ]
                                	set value_list(890,BF3515)  [expr $value_list(890,BF3515) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BF3521)  [expr $value_list(890,BF3521) + $tmp_call_counts            ]
                                set value_list(890,BF3522)  [expr $value_list(890,BF3522) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3524)  [expr $value_list(890,BF3524) + $tmp_call_counts            ]
                                	set value_list(890,BF3525)  [expr $value_list(890,BF3525) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BF3531)  [expr $value_list(890,BF3531) + $tmp_call_counts            ]
                                set value_list(890,BF3532)  [expr $value_list(890,BF3532) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3534)  [expr $value_list(890,BF3534) + $tmp_call_counts            ]
                                	set value_list(890,BF3535)  [expr $value_list(890,BF3535) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国联通移动合计========
                                set value_list(890,BF3501)  [expr $value_list(890,BF3501) + $tmp_call_counts            ]
                                set value_list(890,BF3502)  [expr $value_list(890,BF3502) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3504)  [expr $value_list(890,BF3504) + $tmp_call_counts            ]
                                	set value_list(890,BF3505)  [expr $value_list(890,BF3505) + $tmp_call_duration_m        ]
 						  }
             }
#==============================
          if {$tmp_opposite_id == 4 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BF3711)  [expr $value_list(890,BF3711) + $tmp_call_counts            ]
                                set value_list(890,BF3712)  [expr $value_list(890,BF3712) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3714)  [expr $value_list(890,BF3714) + $tmp_call_counts            ]
                                	set value_list(890,BF3715)  [expr $value_list(890,BF3715) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BF3721)  [expr $value_list(890,BF3721) + $tmp_call_counts            ]
                                set value_list(890,BF3722)  [expr $value_list(890,BF3722) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3724)  [expr $value_list(890,BF3724) + $tmp_call_counts            ]
                                	set value_list(890,BF3725)  [expr $value_list(890,BF3725) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BF3731)  [expr $value_list(890,BF3731) + $tmp_call_counts            ]
                                set value_list(890,BF3732)  [expr $value_list(890,BF3732) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3734)  [expr $value_list(890,BF3734) + $tmp_call_counts            ]
                                	set value_list(890,BF3735)  [expr $value_list(890,BF3735) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国联通固定合计========
                                set value_list(890,BF3701)  [expr $value_list(890,BF3701) + $tmp_call_counts            ]
                                set value_list(890,BF3702)  [expr $value_list(890,BF3702) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3704)  [expr $value_list(890,BF3704) + $tmp_call_counts            ]
                                	set value_list(890,BF3705)  [expr $value_list(890,BF3705) + $tmp_call_duration_m        ]
 						  }
             }
#==============================
          if {$tmp_opposite_id == 5 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BF3611)  [expr $value_list(890,BF3611) + $tmp_call_counts            ]
                                set value_list(890,BF3612)  [expr $value_list(890,BF3612) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3614)  [expr $value_list(890,BF3614) + $tmp_call_counts            ]
                                	set value_list(890,BF3615)  [expr $value_list(890,BF3615) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BF3621)  [expr $value_list(890,BF3621) + $tmp_call_counts            ]
                                set value_list(890,BF3622)  [expr $value_list(890,BF3622) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3624)  [expr $value_list(890,BF3624) + $tmp_call_counts            ]
                                	set value_list(890,BF3625)  [expr $value_list(890,BF3625) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BF3631)  [expr $value_list(890,BF3631) + $tmp_call_counts            ]
                                set value_list(890,BF3632)  [expr $value_list(890,BF3632) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3634)  [expr $value_list(890,BF3634) + $tmp_call_counts            ]
                                	set value_list(890,BF3635)  [expr $value_list(890,BF3635) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国电信移动合计========
                                set value_list(890,BF3601)  [expr $value_list(890,BF3601) + $tmp_call_counts            ]
                                set value_list(890,BF3602)  [expr $value_list(890,BF3602) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3604)  [expr $value_list(890,BF3604) + $tmp_call_counts            ]
                                	set value_list(890,BF3605)  [expr $value_list(890,BF3605) + $tmp_call_duration_m        ]
 						  }
             }
#================================
          if {$tmp_opposite_id == 6 } {
                         if {$tmp_tolltype_id == 1 } {
                                set value_list(890,BF3411)  [expr $value_list(890,BF3411) + $tmp_call_counts            ]
                                set value_list(890,BF3412)  [expr $value_list(890,BF3412) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3414)  [expr $value_list(890,BF3414) + $tmp_call_counts            ]
                                	set value_list(890,BF3415)  [expr $value_list(890,BF3415) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 2 } {
                                set value_list(890,BF3421)  [expr $value_list(890,BF3421) + $tmp_call_counts            ]
                                set value_list(890,BF3422)  [expr $value_list(890,BF3422) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3424)  [expr $value_list(890,BF3424) + $tmp_call_counts            ]
                                	set value_list(890,BF3425)  [expr $value_list(890,BF3425) + $tmp_call_duration_m        ]
 						  }
                            }
                         if {$tmp_tolltype_id == 3 } {
                                set value_list(890,BF3431)  [expr $value_list(890,BF3431) + $tmp_call_counts            ]
                                set value_list(890,BF3432)  [expr $value_list(890,BF3432) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3434)  [expr $value_list(890,BF3434) + $tmp_call_counts            ]
                                	set value_list(890,BF3435)  [expr $value_list(890,BF3435) + $tmp_call_duration_m        ]
 						  }
                            }
#======去中国电信固定合计========
                                set value_list(890,BF3401)  [expr $value_list(890,BF3401) + $tmp_call_counts            ]
                                set value_list(890,BF3402)  [expr $value_list(890,BF3402) + $tmp_call_duration_m        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3404)  [expr $value_list(890,BF3404) + $tmp_call_counts            ]
                                	set value_list(890,BF3405)  [expr $value_list(890,BF3405) + $tmp_call_duration_m        ]
 						  }
             }
#================================
          if {$tmp_opposite_id  == 0 } {
                                set value_list(890,BF3901)  [expr $value_list(890,BF3901) + $tmp_call_counts            ]
                                set value_list(890,BF3902)  [expr $value_list(890,BF3902) + $tmp_call_duration_m        ]
                                set value_list(890,BF3903)  [expr $value_list(890,BF3903) + $tmp_call_duration_s        ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF3904)  [expr $value_list(890,BF3904) + $tmp_call_counts            ]
                                	set value_list(890,BF3905)  [expr $value_list(890,BF3905) + $tmp_call_duration_m        ]
                                	set value_list(890,BF3906)  [expr $value_list(890,BF3906) + $tmp_call_duration_s        ]
 						  }
          }

          set value_list(890,BF3201)  [expr $value_list(890,BF3201) + $tmp_call_counts            ]
          set value_list(890,BF3202)  [expr $value_list(890,BF3202) + $tmp_call_duration_m        ]
          set value_list(890,BF3203)  [expr $value_list(890,BF3203) + $tmp_call_duration_s        ]
 		if {$tmp_td_mark == 1} {
          	set value_list(890,BF3204)  [expr $value_list(890,BF3204) + $tmp_call_counts            ]
          	set value_list(890,BF3205)  [expr $value_list(890,BF3205) + $tmp_call_duration_m        ]
          	set value_list(890,BF3206)  [expr $value_list(890,BF3206) + $tmp_call_duration_s        ]
 		}

          set value_list(890,BF3001)  [expr $value_list(890,BF3001) + $tmp_call_counts            ]
          set value_list(890,BF3002)  [expr $value_list(890,BF3002) + $tmp_call_duration_m        ]
          set value_list(890,BF3003)  [expr $value_list(890,BF3003) + $tmp_call_duration_s        ]
 		if {$tmp_td_mark == 1} {
          	set value_list(890,BF3004)  [expr $value_list(890,BF3004) + $tmp_call_counts            ]
          	set value_list(890,BF3005)  [expr $value_list(890,BF3005) + $tmp_call_duration_m        ]
          	set value_list(890,BF3006)  [expr $value_list(890,BF3006) + $tmp_call_duration_s        ]
 		}
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
###=====================================全球通客户国内漫游出访通话情况(被叫)=========================
#3、被叫合计  	          BB4101	BB4104	BB4102	BB4105	-	-
#其中：本地来话合计	      BB4151	BB4154	BB4152	BB4155	-	-
#      省内长途来话合计	  BB4161	BB4164	BB4162	BB4165	-	-
#省际长途来话合计     	  BB4171	BB4174	BB4172	BB4175	-	-
#      国际长途来话合计	  BB4181	BB4184	BB4182	BB4185	-	-
#      港澳台长途来话合计	BB4191	BB4194	BB4192	BB4195	-	-

 	set sql_buf01 "select       b.crm_brand_id3,
                              case when a.TOLLTYPE_ID in (0)  then 1
                                   when a.TOLLTYPE_ID in (1)  then 2
                                   when a.TOLLTYPE_ID in (2)  then 3
                                   when a.TOLLTYPE_ID in (3,4,5)  then 5
                                   else 4
                              end,a.mns_type,
                  	          value(sum(a.CALL_COUNTS)-sum(case when a.rate_prod_id is null then a.CALL_COUNTS else 0 end)*2,0),
                  	          value(sum(a.call_duration_m),0)
                 from dw_call_$year$month a ,dw_product_$year$month b
                 where a.user_id=b.user_id
                 and a.roamtype_id in (1,4,6,8) and a.CALLTYPE_ID in (1) and a.TOLLTYPE_ID < 100
                 group by     b.crm_brand_id3,
                              case when a.TOLLTYPE_ID in (0)  then 1
                                   when a.TOLLTYPE_ID in (1)  then 2
                                   when a.TOLLTYPE_ID in (2)  then 3
                                   when a.TOLLTYPE_ID in (3,4,5)  then 5
                                   else 4
                              end,a.mns_type"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id                   [lindex $this_row 0]
	        set     tmp_tolltype_id                [lindex $this_row 1]
	        set     tmp_td_mark                	   [lindex $this_row 2]
	        set     tmp_call_counts                [lindex $this_row 3]
		      set	    tmp_call_duration_m            [lindex $this_row 4]

#其中：全球通
   foreach global_all $brandid_global_all {
          if {$tmp_brand_id == $global_all } {
                          if {$tmp_tolltype_id == 1 } {
                                         set value_list(890,BB4151)  [expr $value_list(890,BB4151) + $tmp_call_counts       ]
                                         set value_list(890,BB4152)  [expr $value_list(890,BB4152) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BB4154)  [expr $value_list(890,BB4154) + $tmp_call_counts       ]
                                         	set value_list(890,BB4155)  [expr $value_list(890,BB4155) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 2 } {
                                         set value_list(890,BB4161)  [expr $value_list(890,BB4161) + $tmp_call_counts       ]
                                         set value_list(890,BB4162)  [expr $value_list(890,BB4162) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BB4164)  [expr $value_list(890,BB4164) + $tmp_call_counts       ]
                                         	set value_list(890,BB4165)  [expr $value_list(890,BB4165) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 3 } {
                                         set value_list(890,BB4171)  [expr $value_list(890,BB4171) + $tmp_call_counts       ]
                                         set value_list(890,BB4172)  [expr $value_list(890,BB4172) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BB4174)  [expr $value_list(890,BB4174) + $tmp_call_counts       ]
                                         	set value_list(890,BB4175)  [expr $value_list(890,BB4175) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 4 } {
                                         set value_list(890,BB4181)  [expr $value_list(890,BB4181) + $tmp_call_counts       ]
                                         set value_list(890,BB4182)  [expr $value_list(890,BB4182) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BB4184)  [expr $value_list(890,BB4184) + $tmp_call_counts       ]
                                         	set value_list(890,BB4185)  [expr $value_list(890,BB4185) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 5 } {
                                         set value_list(890,BB4191)  [expr $value_list(890,BB4191) + $tmp_call_counts       ]
                                         set value_list(890,BB4192)  [expr $value_list(890,BB4192) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BB4194)  [expr $value_list(890,BB4194) + $tmp_call_counts       ]
                                         	set value_list(890,BB4195)  [expr $value_list(890,BB4195) + $tmp_call_duration_m   ]
 						  		 }
                             }
             set value_list(890,BB4101)  [expr $value_list(890,BB4101) + $tmp_call_counts            ]
             set value_list(890,BB4102)  [expr $value_list(890,BB4102) + $tmp_call_duration_m        ]
 		   if {$tmp_td_mark == 1} {
             		set value_list(890,BB4104)  [expr $value_list(890,BB4104) + $tmp_call_counts            ]
             		set value_list(890,BB4105)  [expr $value_list(890,BB4105) + $tmp_call_duration_m        ]
 		   }
           }
    }
#其中：动感地带
   foreach mzone_all $brandid_mzone_all {
          if {$tmp_brand_id == $mzone_all } {
                          if {$tmp_tolltype_id == 1 } {
                                         set value_list(890,BE4151)  [expr $value_list(890,BE4151) + $tmp_call_counts       ]
                                         set value_list(890,BE4152)  [expr $value_list(890,BE4152) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BE4154)  [expr $value_list(890,BE4154) + $tmp_call_counts       ]
                                         	set value_list(890,BE4155)  [expr $value_list(890,BE4155) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 2 } {
                                         set value_list(890,BE4161)  [expr $value_list(890,BE4161) + $tmp_call_counts       ]
                                         set value_list(890,BE4162)  [expr $value_list(890,BE4162) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BE4164)  [expr $value_list(890,BE4164) + $tmp_call_counts       ]
                                         	set value_list(890,BE4165)  [expr $value_list(890,BE4165) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 3 } {
                                         set value_list(890,BE4171)  [expr $value_list(890,BE4171) + $tmp_call_counts       ]
                                         set value_list(890,BE4172)  [expr $value_list(890,BE4172) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BE4174)  [expr $value_list(890,BE4174) + $tmp_call_counts       ]
                                         	set value_list(890,BE4175)  [expr $value_list(890,BE4175) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 4 } {
                                         set value_list(890,BE4181)  [expr $value_list(890,BE4181) + $tmp_call_counts       ]
                                         set value_list(890,BE4182)  [expr $value_list(890,BE4182) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BE4184)  [expr $value_list(890,BE4184) + $tmp_call_counts       ]
                                         	set value_list(890,BE4185)  [expr $value_list(890,BE4185) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 5 } {
                                         set value_list(890,BE4191)  [expr $value_list(890,BE4191) + $tmp_call_counts       ]
                                         set value_list(890,BE4192)  [expr $value_list(890,BE4192) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BE4194)  [expr $value_list(890,BE4194) + $tmp_call_counts       ]
                                         	set value_list(890,BE4195)  [expr $value_list(890,BE4195) + $tmp_call_duration_m   ]
 						  		 }
                             }
             set value_list(890,BE4101)  [expr $value_list(890,BE4101) + $tmp_call_counts            ]
             set value_list(890,BE4102)  [expr $value_list(890,BE4102) + $tmp_call_duration_m        ]
 		   if {$tmp_td_mark == 1} {
             		set value_list(890,BE4104)  [expr $value_list(890,BE4104) + $tmp_call_counts            ]
             		set value_list(890,BE4105)  [expr $value_list(890,BE4105) + $tmp_call_duration_m        ]
 		   }
           }
    }
#其中：神州行
     foreach china_all $brandid_china_all {
             if {$tmp_brand_id == $china_all } {
                          if {$tmp_tolltype_id == 1 } {
                                         set value_list(890,BF4151)  [expr $value_list(890,BF4151) + $tmp_call_counts       ]
                                         set value_list(890,BF4152)  [expr $value_list(890,BF4152) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF4154)  [expr $value_list(890,BF4154) + $tmp_call_counts       ]
                                         	set value_list(890,BF4155)  [expr $value_list(890,BF4155) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 2 } {
                                         set value_list(890,BF4161)  [expr $value_list(890,BF4161) + $tmp_call_counts       ]
                                         set value_list(890,BF4162)  [expr $value_list(890,BF4162) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF4164)  [expr $value_list(890,BF4164) + $tmp_call_counts       ]
                                         	set value_list(890,BF4165)  [expr $value_list(890,BF4165) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 3 } {
                                         set value_list(890,BF4171)  [expr $value_list(890,BF4171) + $tmp_call_counts       ]
                                         set value_list(890,BF4172)  [expr $value_list(890,BF4172) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF4174)  [expr $value_list(890,BF4174) + $tmp_call_counts       ]
                                         	set value_list(890,BF4175)  [expr $value_list(890,BF4175) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 4 } {
                                         set value_list(890,BF4181)  [expr $value_list(890,BF4181) + $tmp_call_counts       ]
                                         set value_list(890,BF4182)  [expr $value_list(890,BF4182) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF4184)  [expr $value_list(890,BF4184) + $tmp_call_counts       ]
                                         	set value_list(890,BF4185)  [expr $value_list(890,BF4185) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 5 } {
                                         set value_list(890,BF4191)  [expr $value_list(890,BF4191) + $tmp_call_counts       ]
                                         set value_list(890,BF4192)  [expr $value_list(890,BF4192) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF4194)  [expr $value_list(890,BF4194) + $tmp_call_counts       ]
                                         	set value_list(890,BF4195)  [expr $value_list(890,BF4195) + $tmp_call_duration_m   ]
 						  		 }
                             }
             set value_list(890,BF4101)  [expr $value_list(890,BF4101) + $tmp_call_counts            ]
             set value_list(890,BF4102)  [expr $value_list(890,BF4102) + $tmp_call_duration_m        ]
 		   if {$tmp_td_mark == 1} {
             		set value_list(890,BF4104)  [expr $value_list(890,BF4104) + $tmp_call_counts            ]
             		set value_list(890,BF4105)  [expr $value_list(890,BF4105) + $tmp_call_duration_m        ]
 		   }
           }
    }
#其中：本地品牌客户数
       foreach   other4	 $brandid_other4  {
             if {$tmp_brand_id == $other4 } {
                          if {$tmp_tolltype_id == 1 } {
                                         set value_list(890,BF4151)  [expr $value_list(890,BF4151) + $tmp_call_counts       ]
                                         set value_list(890,BF4152)  [expr $value_list(890,BF4152) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF4154)  [expr $value_list(890,BF4154) + $tmp_call_counts       ]
                                         	set value_list(890,BF4155)  [expr $value_list(890,BF4155) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 2 } {
                                         set value_list(890,BF4161)  [expr $value_list(890,BF4161) + $tmp_call_counts       ]
                                         set value_list(890,BF4162)  [expr $value_list(890,BF4162) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF4164)  [expr $value_list(890,BF4164) + $tmp_call_counts       ]
                                         	set value_list(890,BF4165)  [expr $value_list(890,BF4165) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 3 } {
                                         set value_list(890,BF4171)  [expr $value_list(890,BF4171) + $tmp_call_counts       ]
                                         set value_list(890,BF4172)  [expr $value_list(890,BF4172) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF4174)  [expr $value_list(890,BF4174) + $tmp_call_counts       ]
                                         	set value_list(890,BF4175)  [expr $value_list(890,BF4175) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 4 } {
                                         set value_list(890,BF4181)  [expr $value_list(890,BF4181) + $tmp_call_counts       ]
                                         set value_list(890,BF4182)  [expr $value_list(890,BF4182) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF4184)  [expr $value_list(890,BF4184) + $tmp_call_counts       ]
                                         	set value_list(890,BF4185)  [expr $value_list(890,BF4185) + $tmp_call_duration_m   ]
 						  		 }
                             }
                          if {$tmp_tolltype_id == 5 } {
                                         set value_list(890,BF4191)  [expr $value_list(890,BF4191) + $tmp_call_counts       ]
                                         set value_list(890,BF4192)  [expr $value_list(890,BF4192) + $tmp_call_duration_m   ]
 						  		 if {$tmp_td_mark == 1} {
                                         	set value_list(890,BF4194)  [expr $value_list(890,BF4194) + $tmp_call_counts       ]
                                         	set value_list(890,BF4195)  [expr $value_list(890,BF4195) + $tmp_call_duration_m   ]
 						  		 }
                             }
             set value_list(890,BF4101)  [expr $value_list(890,BF4101) + $tmp_call_counts            ]
             set value_list(890,BF4102)  [expr $value_list(890,BF4102) + $tmp_call_duration_m        ]
 		   if {$tmp_td_mark == 1} {
	        		set value_list(890,BF4104)  [expr $value_list(890,BF4104) + $tmp_call_counts            ]
             		set value_list(890,BF4105)  [expr $value_list(890,BF4105) + $tmp_call_duration_m        ]
		   }
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
###===============================全球通客户国内漫游通话情况(被叫构成)===============================
#4、被叫构成合计	          BB4201	BB4204	BB4202	BB4205	-	-
#其中：本公司移动来话合计	  BB4301	BB4304	BB4302	BB4305	-	-
#中国铁通固定来话合计	      BB4801	BB4804	BB4802	BB4805	-	-
#      中国联通移动来话合计	BB4501	BB4504	BB4502	BB4505	-	-
#中国联通固定来话合计	      BB4701	BB4704	BB4702	BB4705	-	-
#      中国电信移动来话合计	BB4601	BB4604	BB4602	BB4605	-	-
#中国电信固定来话合计	      BB4401	BB4404	BB4402	BB4405	-	-
#其他来话	                  BB4901	BB4904	BB4902	BB4905	-	-


 	set sql_buf01 "select      b.crm_brand_id3,
 	                           case
 	                               when a.OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
                                 when a.opposite_id in (3)           then 2
                                 when a.opposite_id in (13)          then 3
                                 when a.opposite_id in (115,2)       then 4
                                 when a.opposite_id in (14)          then 5
                                 when a.opposite_id in (1,4,116)     then 6
                                 else 0
                             end,a.mns_type,
                  	          value(sum(a.CALL_COUNTS)-sum(case when a.rate_prod_id is null then a.CALL_COUNTS else 0 end)*2,0),
                  	          value(sum(a.call_duration_m),0)
                 from dw_call_$year$month a ,dw_product_$year$month b
                 where a.user_id=b.user_id
                 and a.roamtype_id in (1,4,6,8) and a.CALLTYPE_ID in (1) and a.TOLLTYPE_ID < 100
                 group by b.crm_brand_id3,
 	                           case
 	                               when a.OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
                                 when a.opposite_id in (3)           then 2
                                 when a.opposite_id in (13)          then 3
                                 when a.opposite_id in (115,2)       then 4
                                 when a.opposite_id in (14)          then 5
                                 when a.opposite_id in (1,4,116)     then 6
                                 else 0
                             end,a.mns_type"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}
	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id                   [lindex $this_row 0]
	        set     tmp_opposite_id                [lindex $this_row 1]
	        set     tmp_td_mark                	  [lindex $this_row 2]
	        set     tmp_call_counts                [lindex $this_row 3]
		   set	 tmp_call_duration_m            [lindex $this_row 4]


#其中：全球通
   foreach global_all $brandid_global_all {
          if {$tmp_brand_id == $global_all } {
               if {$tmp_opposite_id == 1 } {
                                set value_list(890,BB4301)  [expr $value_list(890,BB4301) + $tmp_call_counts             ]
                                set value_list(890,BB4302)  [expr $value_list(890,BB4302) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB4304)  [expr $value_list(890,BB4304) + $tmp_call_counts             ]
                                	set value_list(890,BB4305)  [expr $value_list(890,BB4305) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 2 } {
                                set value_list(890,BB4801)  [expr $value_list(890,BB4801) + $tmp_call_counts             ]
                                set value_list(890,BB4802)  [expr $value_list(890,BB4802) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB4804)  [expr $value_list(890,BB4804) + $tmp_call_counts             ]
                                	set value_list(890,BB4805)  [expr $value_list(890,BB4805) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 3 } {
                                set value_list(890,BB4501)  [expr $value_list(890,BB4501) + $tmp_call_counts             ]
                                set value_list(890,BB4502)  [expr $value_list(890,BB4502) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB4504)  [expr $value_list(890,BB4504) + $tmp_call_counts             ]
                                	set value_list(890,BB4505)  [expr $value_list(890,BB4505) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 4 } {
                                set value_list(890,BB4701)  [expr $value_list(890,BB4701) + $tmp_call_counts             ]
                                set value_list(890,BB4702)  [expr $value_list(890,BB4702) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB4704)  [expr $value_list(890,BB4704) + $tmp_call_counts             ]
                                	set value_list(890,BB4705)  [expr $value_list(890,BB4705) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 5 } {
                                set value_list(890,BB4601)  [expr $value_list(890,BB4601) + $tmp_call_counts             ]
                                set value_list(890,BB4602)  [expr $value_list(890,BB4602) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB4604)  [expr $value_list(890,BB4604) + $tmp_call_counts             ]
                                	set value_list(890,BB4605)  [expr $value_list(890,BB4605) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 6 } {
                                set value_list(890,BB4401)  [expr $value_list(890,BB4401) + $tmp_call_counts             ]
                                set value_list(890,BB4402)  [expr $value_list(890,BB4402) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB4404)  [expr $value_list(890,BB4404) + $tmp_call_counts             ]
                                	set value_list(890,BB4405)  [expr $value_list(890,BB4405) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 0 } {
                                set value_list(890,BB4901)  [expr $value_list(890,BB4901) + $tmp_call_counts             ]
                                set value_list(890,BB4902)  [expr $value_list(890,BB4902) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BB4904)  [expr $value_list(890,BB4904) + $tmp_call_counts             ]
                                	set value_list(890,BB4905)  [expr $value_list(890,BB4905) + $tmp_call_duration_m         ]
 						  }
               }
           set value_list(890,BB4201)  [expr $value_list(890,BB4201) + $tmp_call_counts             ]
           set value_list(890,BB4202)  [expr $value_list(890,BB4202) + $tmp_call_duration_m         ]
 		 if {$tmp_td_mark == 1} {
           	set value_list(890,BB4204)  [expr $value_list(890,BB4204) + $tmp_call_counts             ]
           	set value_list(890,BB4205)  [expr $value_list(890,BB4205) + $tmp_call_duration_m         ]
 		 }

           set value_list(890,BB3001)  [expr $value_list(890,BB3001) + $tmp_call_counts            ]
           set value_list(890,BB3002)  [expr $value_list(890,BB3002) + $tmp_call_duration_m        ]
 		 if {$tmp_td_mark == 1} {
           	set value_list(890,BB3004)  [expr $value_list(890,BB3004) + $tmp_call_counts            ]
           	set value_list(890,BB3005)  [expr $value_list(890,BB3005) + $tmp_call_duration_m        ]
 		 }

#           puts $value_list(890,BB3001)
      }
  }
#====================================================================================================
#其中：动感地带
   foreach mzone_all $brandid_mzone_all {
          if {$tmp_brand_id == $mzone_all } {

               if {$tmp_opposite_id == 1 } {
                                set value_list(890,BE4301)  [expr $value_list(890,BE4301) + $tmp_call_counts             ]
                                set value_list(890,BE4302)  [expr $value_list(890,BE4302) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE4304)  [expr $value_list(890,BE4304) + $tmp_call_counts             ]
                                	set value_list(890,BE4305)  [expr $value_list(890,BE4305) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 2 } {
                                set value_list(890,BE4801)  [expr $value_list(890,BE4801) + $tmp_call_counts             ]
                                set value_list(890,BE4802)  [expr $value_list(890,BE4802) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE4804)  [expr $value_list(890,BE4804) + $tmp_call_counts             ]
                                	set value_list(890,BE4805)  [expr $value_list(890,BE4805) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 3 } {
                                set value_list(890,BE4501)  [expr $value_list(890,BE4501) + $tmp_call_counts             ]
                                set value_list(890,BE4502)  [expr $value_list(890,BE4502) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE4504)  [expr $value_list(890,BE4504) + $tmp_call_counts             ]
                                	set value_list(890,BE4505)  [expr $value_list(890,BE4505) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 4 } {
                                set value_list(890,BE4701)  [expr $value_list(890,BE4701) + $tmp_call_counts             ]
                                set value_list(890,BE4702)  [expr $value_list(890,BE4702) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE4704)  [expr $value_list(890,BE4704) + $tmp_call_counts             ]
                                	set value_list(890,BE4705)  [expr $value_list(890,BE4705) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 5 } {
                                set value_list(890,BE4601)  [expr $value_list(890,BE4601) + $tmp_call_counts             ]
                                set value_list(890,BE4602)  [expr $value_list(890,BE4602) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE4604)  [expr $value_list(890,BE4604) + $tmp_call_counts             ]
                                	set value_list(890,BE4605)  [expr $value_list(890,BE4605) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 6 } {
                                set value_list(890,BE4401)  [expr $value_list(890,BE4401) + $tmp_call_counts             ]
                                set value_list(890,BE4402)  [expr $value_list(890,BE4402) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE4404)  [expr $value_list(890,BE4404) + $tmp_call_counts             ]
                                	set value_list(890,BE4405)  [expr $value_list(890,BE4405) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 0 } {
                                set value_list(890,BE4901)  [expr $value_list(890,BE4901) + $tmp_call_counts             ]
                                set value_list(890,BE4902)  [expr $value_list(890,BE4902) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BE4904)  [expr $value_list(890,BE4904) + $tmp_call_counts             ]
                                	set value_list(890,BE4905)  [expr $value_list(890,BE4905) + $tmp_call_duration_m         ]
 						  }
               }
           set value_list(890,BE4201)  [expr $value_list(890,BE4201) + $tmp_call_counts             ]
           set value_list(890,BE4202)  [expr $value_list(890,BE4202) + $tmp_call_duration_m         ]
 		 if {$tmp_td_mark == 1} {
           	set value_list(890,BE4204)  [expr $value_list(890,BE4204) + $tmp_call_counts             ]
           	set value_list(890,BE4205)  [expr $value_list(890,BE4205) + $tmp_call_duration_m         ]
 		 }

           set value_list(890,BE3001)  [expr $value_list(890,BE3001) + $tmp_call_counts            ]
           set value_list(890,BE3002)  [expr $value_list(890,BE3002) + $tmp_call_duration_m        ]
 		 if {$tmp_td_mark == 1} {
           	set value_list(890,BE3004)  [expr $value_list(890,BE3004) + $tmp_call_counts            ]
           	set value_list(890,BE3005)  [expr $value_list(890,BE3005) + $tmp_call_duration_m        ]
 		 }
          }
  }
#====================================================================================================
#其中：神州行
     foreach china_all $brandid_china_all {
             if {$tmp_brand_id == $china_all } {

               if {$tmp_opposite_id == 1 } {
                                set value_list(890,BF4301)  [expr $value_list(890,BF4301) + $tmp_call_counts             ]
                                set value_list(890,BF4302)  [expr $value_list(890,BF4302) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4304)  [expr $value_list(890,BF4304) + $tmp_call_counts             ]
                                	set value_list(890,BF4305)  [expr $value_list(890,BF4305) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 2 } {
                                set value_list(890,BF4801)  [expr $value_list(890,BF4801) + $tmp_call_counts             ]
                                set value_list(890,BF4802)  [expr $value_list(890,BF4802) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4804)  [expr $value_list(890,BF4804) + $tmp_call_counts             ]
                                	set value_list(890,BF4805)  [expr $value_list(890,BF4805) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 3 } {
                                set value_list(890,BF4501)  [expr $value_list(890,BF4501) + $tmp_call_counts             ]
                                set value_list(890,BF4502)  [expr $value_list(890,BF4502) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4504)  [expr $value_list(890,BF4504) + $tmp_call_counts             ]
                                	set value_list(890,BF4505)  [expr $value_list(890,BF4505) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 4 } {
                                set value_list(890,BF4701)  [expr $value_list(890,BF4701) + $tmp_call_counts             ]
                                set value_list(890,BF4702)  [expr $value_list(890,BF4702) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4704)  [expr $value_list(890,BF4704) + $tmp_call_counts             ]
                                	set value_list(890,BF4705)  [expr $value_list(890,BF4705) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 5 } {
                                set value_list(890,BF4601)  [expr $value_list(890,BF4601) + $tmp_call_counts             ]
                                set value_list(890,BF4602)  [expr $value_list(890,BF4602) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4604)  [expr $value_list(890,BF4604) + $tmp_call_counts             ]
                                	set value_list(890,BF4605)  [expr $value_list(890,BF4605) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 6 } {
                                set value_list(890,BF4401)  [expr $value_list(890,BF4401) + $tmp_call_counts             ]
                                set value_list(890,BF4402)  [expr $value_list(890,BF4402) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4404)  [expr $value_list(890,BF4404) + $tmp_call_counts             ]
                                	set value_list(890,BF4405)  [expr $value_list(890,BF4405) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 0 } {
                                set value_list(890,BF4901)  [expr $value_list(890,BF4901) + $tmp_call_counts             ]
                                set value_list(890,BF4902)  [expr $value_list(890,BF4902) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4904)  [expr $value_list(890,BF4904) + $tmp_call_counts             ]
                                	set value_list(890,BF4905)  [expr $value_list(890,BF4905) + $tmp_call_duration_m         ]
 						  }
               }
           set value_list(890,BF4201)  [expr $value_list(890,BF4201) + $tmp_call_counts             ]
           set value_list(890,BF4202)  [expr $value_list(890,BF4202) + $tmp_call_duration_m         ]
 		 if {$tmp_td_mark == 1} {
           	set value_list(890,BF4204)  [expr $value_list(890,BF4204) + $tmp_call_counts             ]
           	set value_list(890,BF4205)  [expr $value_list(890,BF4205) + $tmp_call_duration_m         ]
 		 }

           set value_list(890,BF3001)  [expr $value_list(890,BF3001) + $tmp_call_counts            ]
           set value_list(890,BF3002)  [expr $value_list(890,BF3002) + $tmp_call_duration_m        ]
 		 if {$tmp_td_mark == 1} {
           	set value_list(890,BF3004)  [expr $value_list(890,BF3004) + $tmp_call_counts            ]
           	set value_list(890,BF3005)  [expr $value_list(890,BF3005) + $tmp_call_duration_m        ]
 		 }
          }
     }
#其中：本地品牌客户数
     foreach other4 $brandid_other4 {
             if {$tmp_brand_id == $other4 } {

               if {$tmp_opposite_id == 1 } {
                                set value_list(890,BF4301)  [expr $value_list(890,BF4301) + $tmp_call_counts             ]
                                set value_list(890,BF4302)  [expr $value_list(890,BF4302) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4304)  [expr $value_list(890,BF4304) + $tmp_call_counts             ]
                                	set value_list(890,BF4305)  [expr $value_list(890,BF4305) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 2 } {
                                set value_list(890,BF4801)  [expr $value_list(890,BF4801) + $tmp_call_counts             ]
                                set value_list(890,BF4802)  [expr $value_list(890,BF4802) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4804)  [expr $value_list(890,BF4804) + $tmp_call_counts             ]
                                	set value_list(890,BF4805)  [expr $value_list(890,BF4805) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 3 } {
                                set value_list(890,BF4501)  [expr $value_list(890,BF4501) + $tmp_call_counts             ]
                                set value_list(890,BF4502)  [expr $value_list(890,BF4502) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4504)  [expr $value_list(890,BF4504) + $tmp_call_counts             ]
                                	set value_list(890,BF4505)  [expr $value_list(890,BF4505) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 4 } {
                                set value_list(890,BF4701)  [expr $value_list(890,BF4701) + $tmp_call_counts             ]
                                set value_list(890,BF4702)  [expr $value_list(890,BF4702) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4704)  [expr $value_list(890,BF4704) + $tmp_call_counts             ]
                                	set value_list(890,BF4705)  [expr $value_list(890,BF4705) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 5 } {
                                set value_list(890,BF4601)  [expr $value_list(890,BF4601) + $tmp_call_counts             ]
                                set value_list(890,BF4602)  [expr $value_list(890,BF4602) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4604)  [expr $value_list(890,BF4604) + $tmp_call_counts             ]
                                	set value_list(890,BF4605)  [expr $value_list(890,BF4605) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 6 } {
                                set value_list(890,BF4401)  [expr $value_list(890,BF4401) + $tmp_call_counts             ]
                                set value_list(890,BF4402)  [expr $value_list(890,BF4402) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4404)  [expr $value_list(890,BF4404) + $tmp_call_counts             ]
                                	set value_list(890,BF4405)  [expr $value_list(890,BF4405) + $tmp_call_duration_m         ]
 						  }
               }
               if {$tmp_opposite_id == 0 } {
                                set value_list(890,BF4901)  [expr $value_list(890,BF4901) + $tmp_call_counts             ]
                                set value_list(890,BF4902)  [expr $value_list(890,BF4902) + $tmp_call_duration_m         ]
 						  if {$tmp_td_mark == 1} {
                                	set value_list(890,BF4904)  [expr $value_list(890,BF4904) + $tmp_call_counts             ]
                                	set value_list(890,BF4905)  [expr $value_list(890,BF4905) + $tmp_call_duration_m         ]
 						  }
               }
           set value_list(890,BF4201)  [expr $value_list(890,BF4201) + $tmp_call_counts             ]
           set value_list(890,BF4202)  [expr $value_list(890,BF4202) + $tmp_call_duration_m         ]
 		 if {$tmp_td_mark == 1} {
           	set value_list(890,BF4204)  [expr $value_list(890,BF4204) + $tmp_call_counts             ]
           	set value_list(890,BF4205)  [expr $value_list(890,BF4205) + $tmp_call_duration_m         ]
 		 }

           set value_list(890,BF3001)  [expr $value_list(890,BF3001) + $tmp_call_counts            ]
           set value_list(890,BF3002)  [expr $value_list(890,BF3002) + $tmp_call_duration_m        ]
 		 if {$tmp_td_mark == 1} {
           	set value_list(890,BF3004)  [expr $value_list(890,BF3004) + $tmp_call_counts            ]
           	set value_list(890,BF3005)  [expr $value_list(890,BF3005) + $tmp_call_duration_m        ]
 		 }
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
##=====================================移动电话客户省际漫游来访通话情况=============================
#省际漫游来访通话量合计	    BT5001	BT5004	BT5002	BT5005	BT5003	BT5006
#1、主叫合计  	            BT5101	BT5104	BT5102	BT5105	BT5103	BT5106
#  主叫漫游地通话合计  	    BT5111	BT5114	BT5112	BT5115	-	-
#  省内主叫通话合计  	      BT5121	BT5124	BT5122	BT5125	-	-
#  省际主叫通话合计  	      BT5131	BT5134	BT5132	BT5135	-	-
#  港澳台长途主叫通话合计  	BT5141	BT5144	BT5142	BT5145	BT5143	BT5146
#  国际长途主叫通话合计 	  BT5151	BT5154	BT5152	BT5155	BT5153	BT5156

 	    set sql_buf01 "select
 	                           case when TOLLTYPE_ID in (0)  then 1
                                  when TOLLTYPE_ID in (1)  then 2
                                  when TOLLTYPE_ID in (2)  then 3
                                  when TOLLTYPE_ID in (3,4,5)  then 4
                                  else 5
                              end,
                  	          sum(CALL_COUNTS),
                  	          sum(call_duration_m),
                  	          sum(case when TOLLTYPE_ID = 0 then 0 else call_duration_s end),
                  	          sum(case when mns_type = 1 then call_counts else 0 end),
                  	          sum(case when mns_type = 1 then call_duration_m else 0 end),
                  	          sum(case when mns_type = 1 and tolltype_id <> 0 then call_duration_s else 0 end)
                 from DW_CALL_ROAMIN_DM_$year$month
                 where  roamtype_id in (2,7) and  CALLTYPE_ID in (0,2,3) and  TOLLTYPE_ID < 100
                 group by
 	                           case when TOLLTYPE_ID in (0)  then 1
                                  when TOLLTYPE_ID in (1)  then 2
                                  when TOLLTYPE_ID in (2)  then 3
                                  when TOLLTYPE_ID in (3,4,5)  then 4
                                  else 5
                              end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_tolltype_id                [lindex $this_row 0]
	        set     tmp_call_counts                [lindex $this_row 1]
		      set	    tmp_call_duration_m            [lindex $this_row 2]
		      set	    tmp_call_duration_s            [lindex $this_row 3]
		      set     tmp_td_call_counts             [lindex $this_row 4]
		      set     tmp_td_call_duration_m         [lindex $this_row 5]
		      set     tmp_td_call_duration_s         [lindex $this_row 6]


                          if {$tmp_tolltype_id == 1 } {
                                         set value_list(890,BT5111)  [expr $value_list(890,BT5111) + $tmp_call_counts           ]
                                         set value_list(890,BT5112)  [expr $value_list(890,BT5112) + $tmp_call_duration_m       ]
                                         set value_list(890,BT5114)  [expr $value_list(890,BT5114) + $tmp_td_call_counts        ]
                                         set value_list(890,BT5115)  [expr $value_list(890,BT5115) + $tmp_td_call_duration_m    ]

                             }
                          if {$tmp_tolltype_id == 2 } {
                                         set value_list(890,BT5121)  [expr $value_list(890,BT5121) + $tmp_call_counts           ]
                                         set value_list(890,BT5122)  [expr $value_list(890,BT5122) + $tmp_call_duration_m       ]
                                         set value_list(890,BT5124)  [expr $value_list(890,BT5124) + $tmp_td_call_counts        ]
                                         set value_list(890,BT5125)  [expr $value_list(890,BT5125) + $tmp_td_call_duration_m    ]

                             }
                          if {$tmp_tolltype_id == 3 } {
                                         set value_list(890,BT5131)  [expr $value_list(890,BT5131) + $tmp_call_counts           ]
                                         set value_list(890,BT5132)  [expr $value_list(890,BT5132) + $tmp_call_duration_m       ]
                                         set value_list(890,BT5134)  [expr $value_list(890,BT5134) + $tmp_td_call_counts        ]
                                         set value_list(890,BT5135)  [expr $value_list(890,BT5135) + $tmp_td_call_duration_m    ]                                         

                             }
                          if {$tmp_tolltype_id == 4 } {
                                         set value_list(890,BT5141)  [expr $value_list(890,BT5141) + $tmp_call_counts       ]
                                         set value_list(890,BT5142)  [expr $value_list(890,BT5142) + $tmp_call_duration_m   ]
                                         set value_list(890,BT5143)  [expr $value_list(890,BT5143) + $tmp_call_duration_s   ]
                                         
                                         set value_list(890,BT5144)  [expr $value_list(890,BT5144) + $tmp_td_call_counts       ]
                                         set value_list(890,BT5145)  [expr $value_list(890,BT5145) + $tmp_td_call_duration_m   ]
                                         set value_list(890,BT5146)  [expr $value_list(890,BT5146) + $tmp_td_call_duration_s   ]

                                         set value_list(890,BT5103)  [expr $value_list(890,BT5103) + $tmp_call_duration_s   ]
                                         set value_list(890,BT5003)  [expr $value_list(890,BT5003) + $tmp_call_duration_s   ]
                                         
                                         set value_list(890,BT5106)  [expr $value_list(890,BT5106) + $tmp_td_call_duration_s   ]
                                         set value_list(890,BT5006)  [expr $value_list(890,BT5006) + $tmp_td_call_duration_s   ]                                        
#平衡
                                         set value_list(890,BT5903)  [expr $value_list(890,BT5903) + $tmp_call_duration_s   ]
                                         set value_list(890,BT5203)  [expr $value_list(890,BT5203) + $tmp_call_duration_s   ]
                                         
                                         set value_list(890,BT5906)  [expr $value_list(890,BT5906) + $tmp_td_call_duration_s   ]
                                         set value_list(890,BT5206)  [expr $value_list(890,BT5206) + $tmp_td_call_duration_s   ]

                             }
                          if {$tmp_tolltype_id == 5 } {
                                         set value_list(890,BT5151)  [expr $value_list(890,BT5151) + $tmp_call_counts       ]
                                         set value_list(890,BT5152)  [expr $value_list(890,BT5152) + $tmp_call_duration_m   ]
                                         set value_list(890,BT5153)  [expr $value_list(890,BT5153) + $tmp_call_duration_s   ]
                                         
                                         set value_list(890,BT5154)  [expr $value_list(890,BT5154) + $tmp_td_call_counts       ]
                                         set value_list(890,BT5155)  [expr $value_list(890,BT5155) + $tmp_td_call_duration_m   ]
                                         set value_list(890,BT5156)  [expr $value_list(890,BT5156) + $tmp_td_call_duration_s   ]

                                         set value_list(890,BT5103)  [expr $value_list(890,BT5103) + $tmp_call_duration_s   ]
                                         set value_list(890,BT5003)  [expr $value_list(890,BT5003) + $tmp_call_duration_s   ]

                                         set value_list(890,BT5106)  [expr $value_list(890,BT5106) + $tmp_td_call_duration_s   ]
                                         set value_list(890,BT5006)  [expr $value_list(890,BT5006) + $tmp_td_call_duration_s   ]                                            
#平衡
                                         set value_list(890,BT5903)  [expr $value_list(890,BT5903) + $tmp_call_duration_s   ]
                                         set value_list(890,BT5203)  [expr $value_list(890,BT5203) + $tmp_call_duration_s   ]
                                         
                                         set value_list(890,BT5906)  [expr $value_list(890,BT5906) + $tmp_td_call_duration_s   ]
                                         set value_list(890,BT5206)  [expr $value_list(890,BT5206) + $tmp_td_call_duration_s   ]                                         

                             }
                          if {$tmp_tolltype_id != "" } {
                                         set value_list(890,BT5101)  [expr $value_list(890,BT5101) + $tmp_call_counts       ]
                                         set value_list(890,BT5102)  [expr $value_list(890,BT5102) + $tmp_call_duration_m   ]
                                         
                                         set value_list(890,BT5104)  [expr $value_list(890,BT5104) + $tmp_td_call_counts       ]
                                         set value_list(890,BT5105)  [expr $value_list(890,BT5105) + $tmp_td_call_duration_m   ]                                         

                             }
                          set value_list(890,BT5001)  [expr $value_list(890,BT5001) + $tmp_call_counts       ]
                          set value_list(890,BT5002)  [expr $value_list(890,BT5002) + $tmp_call_duration_m   ]
                          
                          set value_list(890,BT5004)  [expr $value_list(890,BT5004) + $tmp_td_call_counts       ]
                          set value_list(890,BT5005)  [expr $value_list(890,BT5005) + $tmp_td_call_duration_m   ]
#End Of It
}
#====================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
	
	
#2、主叫构成合计	  BT5201	BT5204	BT5202	BT5205	BT5203	BT5206
#去本公司移动合计	  BT5301	BT5304	BT5302	BT5305	-	-
#其中：本地通话	    BT5311	BT5314	BT5312	BT5315	-	-
#      省内通话	    BT5321	BT5324	BT5322	BT5325	-	-
#      省际通话	    BT5331	BT5334	BT5332	BT5335	-	-
#去中国铁通固定合计	BT5801	BT5804	BT5802	BT5805	-	-
#其中：本地通话	    BT5811	BT5814	BT5812	BT5815	-	-
#      省内通话	    BT5821	BT5824	BT5822	BT5825	-	-
#      省际通话	    BT5831	BT5834	BT5832	BT5835	-	-
#去中国联通移动合计	BT5501	BT5504	BT5502	BT5505	-	-
#其中：本地通话	    BT5511	BT5514	BT5512	BT5515	-	-
#      省内通话	    BT5521	BT5524	BT5522	BT5525	-	-
#      省际通话	    BT5531	BT5534	BT5532	BT5535	-	-
#去中国联通固定合计	BT5701	BT5704	BT5702	BT5705	-	-
#其中：本地通话	    BT5711	BT5714	BT5712	BT5715	-	-
#      省内通话	    BT5721	BT5724	BT5722	BT5725	-	-
#      省际通话	    BT5731	BT5734	BT5732	BT5735	-	-
#去中国电信固定合计	BT5401	BT5404	BT5402	BT5405	-	-
#其中：本地通话	    BT5411	BT5414	BT5412	BT5415	-	-
#      省内通话	    BT5421	BT5424	BT5422	BT5425	-	-
#      省际通话	    BT5431	BT5434	BT5432	BT5435	-	-
#去中国电信移动合计	BT5601	BT5604	BT5602	BT5605	-	-
#其中：本地通话	    BT5611	BT5614	BT5612	BT5615	-	-
#      省内通话	    BT5621	BT5624	BT5622	BT5625	-	-
#      省际通话	    BT5631	BT5634	BT5632	BT5635	-	-
#其他去话	          BT5901	BT5904	BT5902	BT5905	BT5903	BT5906

 	set sql_buf01 "select
 	                            case
 	                                 when OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
                                   when opposite_id in (3)           then 2
                                   when opposite_id in (13)          then 3
                                   when opposite_id in (115,2)       then 4
                                   when opposite_id in (1,4,116)     then 5
                                   when opposite_id in (14)          then 6
                                   else 0
                              end,
                              case when TOLLTYPE_ID in (0) then 1
                                   when TOLLTYPE_ID in (1) then 2
                                   when TOLLTYPE_ID in (2) then 3
                              else 0 end,
                  	          sum(CALL_COUNTS),
                  	          sum(call_duration_m),
                  	          sum(case when mns_type = 1 then call_counts else 0 end),
                  	          sum(case when mns_type = 1 then call_duration_m else 0 end)
                 from DW_CALL_ROAMIN_DM_$year$month
                 where  roamtype_id in (2,7) and CALLTYPE_ID in (0,2,3) and TOLLTYPE_ID < 100
                 group by     case
 	                                 when OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
                                   when opposite_id in (3)           then 2
                                   when opposite_id in (13)          then 3
                                   when opposite_id in (115,2)       then 4
                                   when opposite_id in (1,4,116)     then 5
                                   when opposite_id in (14)          then 6
                                   else 0
                              end,
                              case when TOLLTYPE_ID in (0) then 1
                                   when TOLLTYPE_ID in (1) then 2
                                   when TOLLTYPE_ID in (2) then 3
                              else 0 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_opposite_id                [lindex $this_row 0]
	        set     tmp_tolltype_id                [lindex $this_row 1]
	        set     tmp_call_counts                [lindex $this_row 2]
		      set	    tmp_call_duration_m            [lindex $this_row 3]
		      set     tmp_td_call_counts             [lindex $this_row 4]
		      set     tmp_td_call_duration_m         [lindex $this_row 5]
		      #set	    tmp_call_duration_s            [lindex $this_row 4]
#警告BT5201:1751561 == BT5301+BT7301+BT5401+BT5501+BT5601+BT5701+BT5801+BT5901:1749166

                 if {$tmp_opposite_id == 1 } {
                               if {$tmp_tolltype_id == 1 } {
                                        set value_list(890,BT5311)  [expr $value_list(890,BT5311) + $tmp_call_counts            ]
                                        set value_list(890,BT5312)  [expr $value_list(890,BT5312) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5314)  [expr $value_list(890,BT5314) + $tmp_td_call_counts         ]
                                        set value_list(890,BT5315)  [expr $value_list(890,BT5315) + $tmp_td_call_duration_m     ]                                        
                                  }
                               if {$tmp_tolltype_id == 2 } {
                                        set value_list(890,BT5321)  [expr $value_list(890,BT5321) + $tmp_call_counts            ]
                                        set value_list(890,BT5322)  [expr $value_list(890,BT5322) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5324)  [expr $value_list(890,BT5324) + $tmp_td_call_counts         ]
                                        set value_list(890,BT5325)  [expr $value_list(890,BT5325) + $tmp_td_call_duration_m     ]                                          
                                  }
                               if {$tmp_tolltype_id == 3 } {
                                        set value_list(890,BT5331)  [expr $value_list(890,BT5331) + $tmp_call_counts            ]
                                        set value_list(890,BT5332)  [expr $value_list(890,BT5332) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5334)  [expr $value_list(890,BT5334) + $tmp_td_call_counts         ]
                                        set value_list(890,BT5335)  [expr $value_list(890,BT5335) + $tmp_td_call_duration_m     ]                                          
                                  }
                               set value_list(890,BT5301)  [expr $value_list(890,BT5301) + $tmp_call_counts            ]
                               set value_list(890,BT5302)  [expr $value_list(890,BT5302) + $tmp_call_duration_m        ]
                               set value_list(890,BT5304)  [expr $value_list(890,BT5304) + $tmp_td_call_counts         ]
                               set value_list(890,BT5305)  [expr $value_list(890,BT5305) + $tmp_td_call_duration_m     ]                                 

                               set value_list(890,BT5201)  [expr $value_list(890,BT5201) + $tmp_call_counts            ]
                               set value_list(890,BT5202)  [expr $value_list(890,BT5202) + $tmp_call_duration_m        ]
                               set value_list(890,BT5204)  [expr $value_list(890,BT5204) + $tmp_td_call_counts         ]
                               set value_list(890,BT5205)  [expr $value_list(890,BT5205) + $tmp_td_call_duration_m     ]  
                    }
                 if {$tmp_opposite_id == 2 } {
                               if {$tmp_tolltype_id == 1 } {
                                        set value_list(890,BT5811)  [expr $value_list(890,BT5811) + $tmp_call_counts            ]
                                        set value_list(890,BT5812)  [expr $value_list(890,BT5812) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5814)  [expr $value_list(890,BT5814) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5815)  [expr $value_list(890,BT5815) + $tmp_td_call_duration_m        ]
                                  }
                               if {$tmp_tolltype_id == 2 } {
                                        set value_list(890,BT5821)  [expr $value_list(890,BT5821) + $tmp_call_counts            ]
                                        set value_list(890,BT5822)  [expr $value_list(890,BT5822) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5824)  [expr $value_list(890,BT5824) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5825)  [expr $value_list(890,BT5825) + $tmp_td_call_duration_m        ]
                                  }
                               if {$tmp_tolltype_id == 3 } {
                                        set value_list(890,BT5831)  [expr $value_list(890,BT5831) + $tmp_call_counts            ]
                                        set value_list(890,BT5832)  [expr $value_list(890,BT5832) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5834)  [expr $value_list(890,BT5834) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5835)  [expr $value_list(890,BT5835) + $tmp_td_call_duration_m        ]
                                  }
                               set value_list(890,BT5801)  [expr $value_list(890,BT5801) + $tmp_call_counts            ]
                               set value_list(890,BT5802)  [expr $value_list(890,BT5802) + $tmp_call_duration_m        ]
                               set value_list(890,BT5804)  [expr $value_list(890,BT5804) + $tmp_td_call_counts            ]
                               set value_list(890,BT5805)  [expr $value_list(890,BT5805) + $tmp_td_call_duration_m        ]                               

                               set value_list(890,BT5201)  [expr $value_list(890,BT5201) + $tmp_call_counts            ]
                               set value_list(890,BT5202)  [expr $value_list(890,BT5202) + $tmp_call_duration_m        ]
                               set value_list(890,BT5204)  [expr $value_list(890,BT5204) + $tmp_td_call_counts         ]
                               set value_list(890,BT5205)  [expr $value_list(890,BT5205) + $tmp_td_call_duration_m     ]  
                    }
                 if {$tmp_opposite_id == 3 } {
                               if {$tmp_tolltype_id == 1 } {
                                        set value_list(890,BT5511)  [expr $value_list(890,BT5511) + $tmp_call_counts            ]
                                        set value_list(890,BT5512)  [expr $value_list(890,BT5512) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5514)  [expr $value_list(890,BT5514) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5515)  [expr $value_list(890,BT5515) + $tmp_td_call_duration_m        ]                                        
                                  }
                               if {$tmp_tolltype_id == 2 } {
                                        set value_list(890,BT5521)  [expr $value_list(890,BT5521) + $tmp_call_counts            ]
                                        set value_list(890,BT5522)  [expr $value_list(890,BT5522) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5524)  [expr $value_list(890,BT5524) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5525)  [expr $value_list(890,BT5525) + $tmp_td_call_duration_m        ]                                          
                                  }
                               if {$tmp_tolltype_id == 3 } {
                                        set value_list(890,BT5531)  [expr $value_list(890,BT5531) + $tmp_call_counts            ]
                                        set value_list(890,BT5532)  [expr $value_list(890,BT5532) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5534)  [expr $value_list(890,BT5534) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5535)  [expr $value_list(890,BT5535) + $tmp_td_call_duration_m        ]                                          
                                  }
                                set value_list(890,BT5501)  [expr $value_list(890,BT5501) + $tmp_call_counts            ]
                                set value_list(890,BT5502)  [expr $value_list(890,BT5502) + $tmp_call_duration_m        ]
                                set value_list(890,BT5504)  [expr $value_list(890,BT5504) + $tmp_td_call_counts            ]
                                set value_list(890,BT5505)  [expr $value_list(890,BT5505) + $tmp_td_call_duration_m        ]

                                set value_list(890,BT5201)  [expr $value_list(890,BT5201) + $tmp_call_counts            ]
                                set value_list(890,BT5202)  [expr $value_list(890,BT5202) + $tmp_call_duration_m        ]
                                set value_list(890,BT5204)  [expr $value_list(890,BT5204) + $tmp_td_call_counts            ]
                                set value_list(890,BT5205)  [expr $value_list(890,BT5205) + $tmp_td_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 4 } {
                               if {$tmp_tolltype_id == 1 } {
                                        set value_list(890,BT5711)  [expr $value_list(890,BT5711) + $tmp_call_counts            ]
                                        set value_list(890,BT5712)  [expr $value_list(890,BT5712) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5714)  [expr $value_list(890,BT5714) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5715)  [expr $value_list(890,BT5715) + $tmp_td_call_duration_m        ]
                                  }
                               if {$tmp_tolltype_id == 2 } {
                                        set value_list(890,BT5721)  [expr $value_list(890,BT5721) + $tmp_call_counts            ]
                                        set value_list(890,BT5722)  [expr $value_list(890,BT5722) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5724)  [expr $value_list(890,BT5724) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5725)  [expr $value_list(890,BT5725) + $tmp_td_call_duration_m        ]
                                  }
                               if {$tmp_tolltype_id == 3 } {
                                        set value_list(890,BT5731)  [expr $value_list(890,BT5731) + $tmp_call_counts            ]
                                        set value_list(890,BT5732)  [expr $value_list(890,BT5732) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5734)  [expr $value_list(890,BT5734) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5735)  [expr $value_list(890,BT5735) + $tmp_td_call_duration_m        ]
                                  }
                               set value_list(890,BT5701)  [expr $value_list(890,BT5701) + $tmp_call_counts            ]
                               set value_list(890,BT5702)  [expr $value_list(890,BT5702) + $tmp_call_duration_m        ]
                               set value_list(890,BT5704)  [expr $value_list(890,BT5704) + $tmp_td_call_counts            ]
                               set value_list(890,BT5705)  [expr $value_list(890,BT5705) + $tmp_td_call_duration_m        ]

                               set value_list(890,BT5201)  [expr $value_list(890,BT5201) + $tmp_call_counts            ]
                               set value_list(890,BT5202)  [expr $value_list(890,BT5202) + $tmp_call_duration_m        ]
                               set value_list(890,BT5204)  [expr $value_list(890,BT5204) + $tmp_td_call_counts            ]
                               set value_list(890,BT5205)  [expr $value_list(890,BT5205) + $tmp_td_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 5 } {
                               if {$tmp_tolltype_id == 1 } {
                                        set value_list(890,BT5411)  [expr $value_list(890,BT5411) + $tmp_call_counts            ]
                                        set value_list(890,BT5412)  [expr $value_list(890,BT5412) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5414)  [expr $value_list(890,BT5414) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5415)  [expr $value_list(890,BT5415) + $tmp_td_call_duration_m        ]
                                        
                                  }
                               if {$tmp_tolltype_id == 2 } {
                                        set value_list(890,BT5421)  [expr $value_list(890,BT5421) + $tmp_call_counts            ]
                                        set value_list(890,BT5422)  [expr $value_list(890,BT5422) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5424)  [expr $value_list(890,BT5424) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5425)  [expr $value_list(890,BT5425) + $tmp_td_call_duration_m        ]                                        
                                  }
                               if {$tmp_tolltype_id == 3 } {
                                        set value_list(890,BT5431)  [expr $value_list(890,BT5431) + $tmp_call_counts            ]
                                        set value_list(890,BT5432)  [expr $value_list(890,BT5432) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5434)  [expr $value_list(890,BT5434) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5435)  [expr $value_list(890,BT5435) + $tmp_td_call_duration_m        ]                                        
                                  }
                               set value_list(890,BT5401)  [expr $value_list(890,BT5401) + $tmp_call_counts            ]
                               set value_list(890,BT5402)  [expr $value_list(890,BT5402) + $tmp_call_duration_m        ]
                               set value_list(890,BT5404)  [expr $value_list(890,BT5404) + $tmp_td_call_counts            ]
                               set value_list(890,BT5405)  [expr $value_list(890,BT5405) + $tmp_td_call_duration_m        ]                               

                               set value_list(890,BT5201)  [expr $value_list(890,BT5201) + $tmp_call_counts            ]
                               set value_list(890,BT5202)  [expr $value_list(890,BT5202) + $tmp_call_duration_m        ]
                               set value_list(890,BT5204)  [expr $value_list(890,BT5204) + $tmp_td_call_counts            ]
                               set value_list(890,BT5205)  [expr $value_list(890,BT5205) + $tmp_td_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 6 } {
                               if {$tmp_tolltype_id == 1 } {
                                        set value_list(890,BT5611)  [expr $value_list(890,BT5611) + $tmp_call_counts            ]
                                        set value_list(890,BT5612)  [expr $value_list(890,BT5612) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5614)  [expr $value_list(890,BT5614) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5615)  [expr $value_list(890,BT5615) + $tmp_td_call_duration_m        ]                                        
                                  }
                               if {$tmp_tolltype_id == 2 } {
                                        set value_list(890,BT5621)  [expr $value_list(890,BT5621) + $tmp_call_counts            ]
                                        set value_list(890,BT5622)  [expr $value_list(890,BT5622) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5624)  [expr $value_list(890,BT5624) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5625)  [expr $value_list(890,BT5625) + $tmp_td_call_duration_m        ]   
                                  }
                               if {$tmp_tolltype_id == 3 } {
                                        set value_list(890,BT5631)  [expr $value_list(890,BT5631) + $tmp_call_counts            ]
                                        set value_list(890,BT5632)  [expr $value_list(890,BT5632) + $tmp_call_duration_m        ]
                                        set value_list(890,BT5634)  [expr $value_list(890,BT5634) + $tmp_td_call_counts            ]
                                        set value_list(890,BT5635)  [expr $value_list(890,BT5635) + $tmp_td_call_duration_m        ]   
                                  }
                               set value_list(890,BT5601)  [expr $value_list(890,BT5601) + $tmp_call_counts            ]
                               set value_list(890,BT5602)  [expr $value_list(890,BT5602) + $tmp_call_duration_m        ]
                               set value_list(890,BT5604)  [expr $value_list(890,BT5604) + $tmp_td_call_counts            ]
                               set value_list(890,BT5605)  [expr $value_list(890,BT5605) + $tmp_td_call_duration_m        ]

                               set value_list(890,BT5201)  [expr $value_list(890,BT5201) + $tmp_call_counts            ]
                               set value_list(890,BT5202)  [expr $value_list(890,BT5202) + $tmp_call_duration_m        ]
                               set value_list(890,BT5204)  [expr $value_list(890,BT5204) + $tmp_td_call_counts            ]
                               set value_list(890,BT5205)  [expr $value_list(890,BT5205) + $tmp_td_call_duration_m        ]                               
                    }
                 if {$tmp_opposite_id == 0 } {
                               set value_list(890,BT5901)  [expr $value_list(890,BT5901) + $tmp_call_counts            ]
                               set value_list(890,BT5902)  [expr $value_list(890,BT5902) + $tmp_call_duration_m        ]
                               set value_list(890,BT5904)  [expr $value_list(890,BT5904) + $tmp_td_call_counts            ]
                               set value_list(890,BT5905)  [expr $value_list(890,BT5905) + $tmp_td_call_duration_m        ]                               
                               #set value_list(890,BT5903)  [expr $value_list(890,BT5903) + $tmp_call_duration_s        ]

                               set value_list(890,BT5201)  [expr $value_list(890,BT5201) + $tmp_call_counts            ]
                               set value_list(890,BT5202)  [expr $value_list(890,BT5202) + $tmp_call_duration_m        ]
                               set value_list(890,BT5204)  [expr $value_list(890,BT5204) + $tmp_td_call_counts            ]
                               set value_list(890,BT5205)  [expr $value_list(890,BT5205) + $tmp_td_call_duration_m        ]                                 
                               #set value_list(890,BT5203)  [expr $value_list(890,BT5203) + $tmp_call_duration_s        ]
                    }
}
##==================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#=====================================移动电话客户省际漫游来访通话情况==============================
#省际漫游来访通话量合计	      BT5001	BT5004	BT5002	BT5005	BT5003	BT5006
#3、被叫合计  	              BT6101	BT6104	BT6102	BT6105	-	-
#其中：本地来话合计	          BT6151	BT6154	BT6152	BT6155	-	-
#      省内长途来话合计	      BT6161	BT6164	BT6162	BT6165	-	-
#      省际长途来话合计	      BT6171	BT6174	BT6172	BT6175	-	-
#      国际长途来话合计 	    BT6181	BT6184	BT6182	BT6185	-	-
#      港澳台长途来话合计	    BT6191	BT6194	BT6192	BT6195	-	-

 	    set sql_buf01 "select
 	                           case when TOLLTYPE_ID in (0)  then 1
                                  when TOLLTYPE_ID in (1)  then 2
                                  when TOLLTYPE_ID in (2)  then 3
                                  when TOLLTYPE_ID in (3,4,5)  then 4
                                  else 5
                              end,
                  	          sum(CALL_COUNTS),
                  	          sum(call_duration_m),
                  	          sum(case when TOLLTYPE_ID = 0 then 0 else call_duration_s end),
                  	          sum(case when mns_type = 1 then call_counts else 0 end),
                  	          sum(case when mns_type = 1 then call_duration_m else 0 end),
                  	          sum(case when mns_type = 1 and tolltype_id <> 0 then call_duration_s else 0 end)
                 from DW_CALL_ROAMIN_DM_$year$month
                 where  roamtype_id in (2) and  CALLTYPE_ID in (1) and  TOLLTYPE_ID < 100
                 group by case when TOLLTYPE_ID in (0)  then 1
                               when TOLLTYPE_ID in (1)  then 2
                               when TOLLTYPE_ID in (2)  then 3
                               when TOLLTYPE_ID in (3,4,5)  then 4
                               else 5
                          end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_tolltype_id                [lindex $this_row 0]
	        set     tmp_call_counts                [lindex $this_row 1]
		      set	    tmp_call_duration_m            [lindex $this_row 2]
		      set	    tmp_call_duration_s            [lindex $this_row 3]
		      set     tmp_td_call_counts             [lindex $this_row 4]
		      set     tmp_td_call_duration_m         [lindex $this_row 5]
		      set     tmp_td_call_duration_s         [lindex $this_row 6]

                          if {$tmp_tolltype_id == 1 } {
                                         set value_list(890,BT6151)  [expr $value_list(890,BT6151) + $tmp_call_counts       ]
                                         set value_list(890,BT6152)  [expr $value_list(890,BT6152) + $tmp_call_duration_m   ]
                                         set value_list(890,BT6154)  [expr $value_list(890,BT6154) + $tmp_td_call_counts       ]
                                         set value_list(890,BT6155)  [expr $value_list(890,BT6155) + $tmp_td_call_duration_m   ]                                         
                             }
                          if {$tmp_tolltype_id == 2 } {
                                         set value_list(890,BT6161)  [expr $value_list(890,BT6161) + $tmp_call_counts       ]
                                         set value_list(890,BT6162)  [expr $value_list(890,BT6162) + $tmp_call_duration_m   ]
                                         set value_list(890,BT6164)  [expr $value_list(890,BT6164) + $tmp_td_call_counts       ]
                                         set value_list(890,BT6165)  [expr $value_list(890,BT6165) + $tmp_td_call_duration_m   ]                                        
                             }
                          if {$tmp_tolltype_id == 3 } {
                                         set value_list(890,BT6171)  [expr $value_list(890,BT6171) + $tmp_call_counts       ]
                                         set value_list(890,BT6172)  [expr $value_list(890,BT6172) + $tmp_call_duration_m   ]
                                         set value_list(890,BT6174)  [expr $value_list(890,BT6174) + $tmp_td_call_counts       ]
                                         set value_list(890,BT6175)  [expr $value_list(890,BT6175) + $tmp_td_call_duration_m   ]                                          
                             }

                          if {$tmp_tolltype_id == 5 } {
                                         set value_list(890,BT6181)  [expr $value_list(890,BT6181) + $tmp_call_counts       ]
                                         set value_list(890,BT6182)  [expr $value_list(890,BT6182) + $tmp_call_duration_m   ]
                                         set value_list(890,BT6184)  [expr $value_list(890,BT6184) + $tmp_td_call_counts       ]
                                         set value_list(890,BT6185)  [expr $value_list(890,BT6185) + $tmp_td_call_duration_m   ]                                          
                             }

                          if {$tmp_tolltype_id == 4 } {
                                         set value_list(890,BT6191)  [expr $value_list(890,BT6191) + $tmp_call_counts       ]
                                         set value_list(890,BT6192)  [expr $value_list(890,BT6192) + $tmp_call_duration_m   ]
                                         set value_list(890,BT6194)  [expr $value_list(890,BT6194) + $tmp_td_call_counts       ]
                                         set value_list(890,BT6195)  [expr $value_list(890,BT6195) + $tmp_td_call_duration_m   ]                                          
                             }
                          if {$tmp_tolltype_id != "" } {
                                         set value_list(890,BT6101)  [expr $value_list(890,BT6101) + $tmp_call_counts       ]
                                         set value_list(890,BT6102)  [expr $value_list(890,BT6102) + $tmp_call_duration_m   ]
                                         set value_list(890,BT6104)  [expr $value_list(890,BT6104) + $tmp_td_call_counts       ]
                                         set value_list(890,BT6105)  [expr $value_list(890,BT6105) + $tmp_td_call_duration_m   ]                                         
                             }
                           set value_list(890,BT5001)  [expr $value_list(890,BT5001) + $tmp_call_counts       ]
                           set value_list(890,BT5002)  [expr $value_list(890,BT5002) + $tmp_call_duration_m   ]
                           set value_list(890,BT5004)  [expr $value_list(890,BT5004) + $tmp_td_call_counts       ]
                           set value_list(890,BT5005)  [expr $value_list(890,BT5005) + $tmp_td_call_duration_m   ]                           
#End Of It
}
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
###=====================================移动电话客户省际漫游来访通话情况=============================
#4、被叫构成合计	    BT6201	BT6204	BT6202	BT6205	-	-
#本公司移动来话合计	  BT6301	BT6304	BT6302	BT6305	-	-
#中国铁通固定来话合计	BT6801	BT6804	BT6802	BT6805	-	-
#中国联通移动来话合计	BT6501	BT6504	BT6502	BT6505	-	-
#中国联通固定来话合计	BT6701	BT6704	BT6702	BT6705	-	-
#中国电信移动来话合计	BT6601	BT6604	BT6602	BT6605	-	-
#中国电信固定来话合计	BT6401	BT6404	BT6402	BT6405	-	-
#其他来话	            BT6901	BT6904	BT6902	BT6905	-	-

 	set sql_buf01 "select
 	                           case
 	                                 when OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
                                   when opposite_id in (3)           then 2
                                   when opposite_id in (13)          then 3
                                   when opposite_id in (115,2)       then 4
                                   when opposite_id in (14)          then 5
                                   when opposite_id in (1,4,116)     then 6
                                   else 0
                             end,
                  	         sum(CALL_COUNTS),
                  	         sum(call_duration_m),
                  	         sum(case when mns_type = 1 then call_counts else 0 end),
                  	         sum(case when mns_type = 1 then call_duration_m else 0 end)
                 from DW_CALL_ROAMIN_DM_$year$month
                 where  roamtype_id in (2,7) and CALLTYPE_ID in (1) and TOLLTYPE_ID < 100
                 group by   case
 	                                 when OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
                                   when opposite_id in (3)           then 2
                                   when opposite_id in (13)          then 3
                                   when opposite_id in (115,2)       then 4
                                   when opposite_id in (14)          then 5
                                   when opposite_id in (1,4,116)     then 6
                                   else 0
                             end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_opposite_id                [lindex $this_row 0]
	        set     tmp_call_counts                [lindex $this_row 1]
	        set     tmp_call_duration_m            [lindex $this_row 2]
	        set     tmp_td_call_counts             [lindex $this_row 3]
          set     tmp_td_call_duration_m         [lindex $this_row 4]

                 if {$tmp_opposite_id == 1 } {
                            set value_list(890,BT6301)  [expr $value_list(890,BT6301) + $tmp_call_counts            ]
                            set value_list(890,BT6302)  [expr $value_list(890,BT6302) + $tmp_call_duration_m        ]
                            set value_list(890,BT6304)  [expr $value_list(890,BT6304) + $tmp_td_call_counts         ]
                            set value_list(890,BT6305)  [expr $value_list(890,BT6305) + $tmp_td_call_duration_m     ]                            
                    }
                 if {$tmp_opposite_id == 2 } {
                            set value_list(890,BT6801)  [expr $value_list(890,BT6801) + $tmp_call_counts            ]
                            set value_list(890,BT6802)  [expr $value_list(890,BT6802) + $tmp_call_duration_m        ]
                            set value_list(890,BT6804)  [expr $value_list(890,BT6804) + $tmp_td_call_counts         ]
                            set value_list(890,BT6805)  [expr $value_list(890,BT6805) + $tmp_td_call_duration_m     ]                            
                    }
                 if {$tmp_opposite_id == 3 } {
                            set value_list(890,BT6501)  [expr $value_list(890,BT6501) + $tmp_call_counts            ]
                            set value_list(890,BT6502)  [expr $value_list(890,BT6502) + $tmp_call_duration_m        ]
                            set value_list(890,BT6504)  [expr $value_list(890,BT6504) + $tmp_td_call_counts         ]
                            set value_list(890,BT6505)  [expr $value_list(890,BT6505) + $tmp_td_call_duration_m     ]                             
                    }
                 if {$tmp_opposite_id == 4 } {
                            set value_list(890,BT6701)  [expr $value_list(890,BT6701) + $tmp_call_counts            ]
                            set value_list(890,BT6702)  [expr $value_list(890,BT6702) + $tmp_call_duration_m        ]
                            set value_list(890,BT6704)  [expr $value_list(890,BT6704) + $tmp_td_call_counts         ]
                            set value_list(890,BT6705)  [expr $value_list(890,BT6705) + $tmp_td_call_duration_m     ]  
                    }
                 if {$tmp_opposite_id == 5 } {
                            set value_list(890,BT6601)  [expr $value_list(890,BT6601) + $tmp_call_counts            ]
                            set value_list(890,BT6602)  [expr $value_list(890,BT6602) + $tmp_call_duration_m        ]
                            set value_list(890,BT6604)  [expr $value_list(890,BT6604) + $tmp_td_call_counts         ]
                            set value_list(890,BT6605)  [expr $value_list(890,BT6605) + $tmp_td_call_duration_m     ]                              
                    }
                 if {$tmp_opposite_id == 6 } {
                            set value_list(890,BT6401)  [expr $value_list(890,BT6401) + $tmp_call_counts            ]
                            set value_list(890,BT6402)  [expr $value_list(890,BT6402) + $tmp_call_duration_m        ]
                            set value_list(890,BT6404)  [expr $value_list(890,BT6404) + $tmp_td_call_counts         ]
                            set value_list(890,BT6405)  [expr $value_list(890,BT6405) + $tmp_td_call_duration_m     ]                              
                    }
                 if {$tmp_opposite_id == 0 } {
                            set value_list(890,BT6901)  [expr $value_list(890,BT6901) + $tmp_call_counts            ]
                            set value_list(890,BT6902)  [expr $value_list(890,BT6902) + $tmp_call_duration_m        ]
                            set value_list(890,BT6904)  [expr $value_list(890,BT6904) + $tmp_td_call_counts         ]
                            set value_list(890,BT6905)  [expr $value_list(890,BT6905) + $tmp_td_call_duration_m     ]                              
                    }
###合计
                 if { $tmp_opposite_id !="" } {
                 set value_list(890,BT6201)  [expr $value_list(890,BT6201) + $tmp_call_counts            ]
                 set value_list(890,BT6202)  [expr $value_list(890,BT6202) + $tmp_call_duration_m        ]
                 set value_list(890,BT6204)  [expr $value_list(890,BT6204) + $tmp_td_call_counts         ]
                 set value_list(890,BT6205)  [expr $value_list(890,BT6205) + $tmp_td_call_duration_m     ]                 
                 }
}
#====================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
###=====================================移动电话客户国际及港澳台漫游通话情况=========================
#移动客户国际及港澳台漫游来访通话量合计	BV1001	BV1003
#1、移动客户来访主叫构成合计  	BV1101	BV1103
#其中：去本公司移动	            BV1111	BV1113
#      去中国铁通固定	          BV1241	BV1243
#      去中国联通移动	          BV1131	BV1133
#      去联通固定	              BV1221	BV1223
#      去中国电信移动	          BV1141	BV1143
#      去中国电信固定	          BV1211	BV1213
#      去港澳台通话	            BV1331	BV1333
#      去国际通话	              BV1341	BV1343
#      其他去话	                BV1391	BV1393

 	set sql_buf01 "select
                       case when opposite_id in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
                            when opposite_id in (3)        then 2
                            when opposite_id in (13)       then 3
                            when opposite_id in (115,2)    then 4
                            when opposite_id in (14)       then 5
                            when opposite_id in (1,4,116)  then 6
                            when opposite_id in (5,6,7)    then 7
                            when opposite_id in (8)        then 8
                        else 0 end,
                        value(sum(CALL_COUNTS),0),
                        value(sum(call_duration_m),0)
                  from DW_CALL_ROAMIN_DM_$year$month
                  where  roamtype_id in (3) and CALLTYPE_ID in (0,2,3) and TOLLTYPE_ID < 100
                  group by
                       case when opposite_id in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
                            when opposite_id in (3)        then 2
                            when opposite_id in (13)       then 3
                            when opposite_id in (115,2)    then 4
                            when opposite_id in (14)       then 5
                            when opposite_id in (1,4,116)  then 6
                            when opposite_id in (5,6,7)    then 7
                            when opposite_id in (8)        then 8
                        else 0 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_opposite_id                [lindex $this_row 0]
	        set     tmp_call_counts                [lindex $this_row 1]
		      set	    tmp_call_duration_m            [lindex $this_row 2]


                 if {$tmp_opposite_id == 1 } {
                                        set value_list(890,BV1111)  [expr $value_list(890,BV1111) + $tmp_call_counts            ]
                                        set value_list(890,BV1113)  [expr $value_list(890,BV1113) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 2 } {
                                        set value_list(890,BV1241)  [expr $value_list(890,BV1241) + $tmp_call_counts            ]
                                        set value_list(890,BV1243)  [expr $value_list(890,BV1243) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 3 } {
                                        set value_list(890,BV1131)  [expr $value_list(890,BV1131) + $tmp_call_counts            ]
                                        set value_list(890,BV1133)  [expr $value_list(890,BV1133) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 4 } {
                                        set value_list(890,BV1221)  [expr $value_list(890,BV1221) + $tmp_call_counts            ]
                                        set value_list(890,BV1223)  [expr $value_list(890,BV1223) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 5 } {
                                        set value_list(890,BV1141)  [expr $value_list(890,BV1141) + $tmp_call_counts            ]
                                        set value_list(890,BV1143)  [expr $value_list(890,BV1143) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 6 } {
                                        set value_list(890,BV1211)  [expr $value_list(890,BV1211) + $tmp_call_counts            ]
                                        set value_list(890,BV1213)  [expr $value_list(890,BV1213) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 7 } {
                                        set value_list(890,BV1331)  [expr $value_list(890,BV1331) + $tmp_call_counts            ]
                                        set value_list(890,BV1333)  [expr $value_list(890,BV1333) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 8 } {
                                        set value_list(890,BV1341)  [expr $value_list(890,BV1341) + $tmp_call_counts            ]
                                        set value_list(890,BV1343)  [expr $value_list(890,BV1343) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 0 } {
                                        set value_list(890,BV1391)  [expr $value_list(890,BV1391) + $tmp_call_counts            ]
                                        set value_list(890,BV1393)  [expr $value_list(890,BV1393) + $tmp_call_duration_m        ]
                 }
#1、移动客户来访主叫构成合计
                set value_list(890,BV1101)  [expr $value_list(890,BV1101) + $tmp_call_counts            ]
                set value_list(890,BV1103)  [expr $value_list(890,BV1103) + $tmp_call_duration_m        ]
#移动客户国际及港澳台漫游来访通话量合计
                set value_list(890,BV1001)  [expr $value_list(890,BV1001)  + $tmp_call_counts           ]
                set value_list(890,BV1003)  [expr $value_list(890,BV1003)  + $tmp_call_duration_m       ]
#End Of It
}
#====================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
###=====================================移动电话客户国际及港澳台漫游通话情况=========================
#2、移动客户来访被叫构成合计  	BV2101	BV2103	－	－	－
#其中：本公司移动来话	          BV2111	BV2113	－	－	－72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78
#中国联通移动来话	              BV2121	BV2123	－	－	－13
#中国铁通固定来话	              BV2241	BV2243	－	－	－3
#中国联通固定来话	              BV2221	BV2223	－	－	－2,115
#中国电信移动来话	              BV2131	BV2133	－	－	－14
#中国电信固定来话	              BV2211	BV2213	－	－	－1,4,116
#其他来话	                      BV2291	BV2293	－	－	－

 	set sql_buf01 "select
                       case when opposite_id in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
                            when opposite_id in (13)       then 2
                            when opposite_id in (3)        then 3
                            when opposite_id in (2,115)    then 4
                            when opposite_id in (14)       then 5
                            when opposite_id in (1,4,116)  then 6
                        else 0 end,
                        value(sum(CALL_COUNTS),0),
                        value(sum(call_duration_m),0)
                  from DW_CALL_ROAMIN_DM_$year$month
                  where  roamtype_id in (3) and CALLTYPE_ID in (1) and TOLLTYPE_ID < 100
                  group by case when opposite_id in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78) then 1
                            when opposite_id in (13)       then 2
                            when opposite_id in (3)        then 3
                            when opposite_id in (2,115)    then 4
                            when opposite_id in (14)       then 5
                            when opposite_id in (1,4,116)  then 6
                        else 0 end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_opposite_id                [lindex $this_row 0]
	        set     tmp_call_counts                [lindex $this_row 1]
		      set	    tmp_call_duration_m            [lindex $this_row 2]


                 if {$tmp_opposite_id == 1 } {
                                        set value_list(890,BV2111)  [expr $value_list(890,BV2111) + $tmp_call_counts            ]
                                        set value_list(890,BV2113)  [expr $value_list(890,BV2113) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 2 } {
                                        set value_list(890,BV2121)  [expr $value_list(890,BV2121) + $tmp_call_counts            ]
                                        set value_list(890,BV2123)  [expr $value_list(890,BV2123) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 3 } {
                                        set value_list(890,BV2241)  [expr $value_list(890,BV2241) + $tmp_call_counts            ]
                                        set value_list(890,BV2243)  [expr $value_list(890,BV2243) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 4 } {
                                        set value_list(890,BV2221)  [expr $value_list(890,BV2221) + $tmp_call_counts            ]
                                        set value_list(890,BV2223)  [expr $value_list(890,BV2223) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 5 } {
                                        set value_list(890,BV2131)  [expr $value_list(890,BV2131) + $tmp_call_counts            ]
                                        set value_list(890,BV2133)  [expr $value_list(890,BV2133) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 6 } {
                                        set value_list(890,BV2211)  [expr $value_list(890,BV2211) + $tmp_call_counts            ]
                                        set value_list(890,BV2213)  [expr $value_list(890,BV2213) + $tmp_call_duration_m        ]
                    }
                 if {$tmp_opposite_id == 0 } {
                                        set value_list(890,BV2291)  [expr $value_list(890,BV2291) + $tmp_call_counts            ]
                                        set value_list(890,BV2293)  [expr $value_list(890,BV2293) + $tmp_call_duration_m        ]
                 }
#2、移动客户来访被叫构成合计
                 set value_list(890,BV2101)  [expr $value_list(890,BV2101) + $tmp_call_counts            ]
                 set value_list(890,BV2103)  [expr $value_list(890,BV2103) + $tmp_call_duration_m        ]
#移动客户国际及港澳台漫游来访通话量合计
                 set value_list(890,BV1001)  [expr $value_list(890,BV1001)  + $tmp_call_counts           ]
                 set value_list(890,BV1003)  [expr $value_list(890,BV1003)  + $tmp_call_duration_m       ]
#End Of It
}
#====================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
###=====================================移动电话客户国际及港澳台漫游通话情况=========================
#中国移动客户国际及港澳台漫游出访通话量合计	BV2001	BV2003	BV2002	BV2007	BV2006
#1、中国移动客户出访主叫合计  	            BV3101	BV3103	BV3102	BV3107	BV3106
#其中：国际长途	                            BV3111	BV3113	BV3112	BV3117	BV3116
#      港澳台长途	                          BV3121	BV3123	BV3122	BV3127	BV3126
#其中：港澳台漫游出访主叫合计	              BV3201	BV3203	BV3202	BV3207	BV3206

#警告BA8024:0 < BA1085:0
#警告BA8034:0 < BA1145:0
#
#警告BA1072:1979 == BB3153:1348
#警告BA1076:390  == BF3153:364
#    BA1075      == BE3153
#警告BA1082:690  == BB3143:473
#BA1082=BB3143
#BA1086=BF3143
#BA1085=BE3143
#=====================================中国移动客户国际出访合计=======================================
#2、中国移动客户出访被叫合计  	BV4101	BV4103	BV4102	BV4107	BV4106
#其中：国际长途	                BV4111	BV4113	BV4112	BV4117	BV4116
#          港澳台长途	          BV4121	BV4123	BV4122	BV4127	BV4126
#其中：港澳台漫游出访被叫合计	  BV4201	BV4203	BV4202	BV4207	BV4206
#a.roamtype_id in (5,9)
#5 港澳台国际漫出
#9 非港澳台国际漫出
 	set sql_buf01 "select
                              case
                                  when b.crm_brand_id3 in ($rep_global_brand_id) then 2
                                  when b.crm_brand_id3 in ($rep_mzone_brand_id) then  6
                                  else 7
                              end,
                              case
                                   when a.TOLLTYPE_ID in (3,4,5) then 2
                                   else 1
                              end,
                              case
                                   when a.calltype_id in (1) then 41
                                   else 31
                              end,
                  	          value(sum(a.CALL_COUNTS)-sum(case when a.rate_prod_id is null then a.CALL_COUNTS else 0 end)*2,0),
                  	          value(sum(a.call_duration_m),0),
                  	          value(sum(a.call_duration_s),0)
                 from dw_call_$year$month a ,dw_product_$year$month b
                 where a.roamtype_id in (5,9) and a.TOLLTYPE_ID < 100 and a.user_id = b.user_id
                 group by
                              case
                                  when b.crm_brand_id3 in ($rep_global_brand_id) then 2
                                  when b.crm_brand_id3 in ($rep_mzone_brand_id) then  6
                                  else 7
                              end,
                              case
                                   when a.TOLLTYPE_ID in (3,4,5) then 2
                                   else 1
                              end,
                              case
                                   when a.calltype_id in (1) then 41
                                   else 31
                              end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id                   [lindex $this_row 0]
	        set     tmp_tolltype_id                [lindex $this_row 1]
	        set     tmp_calltype_id                [lindex $this_row 2]
	        set     tmp_call_counts                [lindex $this_row 3]
		      set	    tmp_call_duration_m            [lindex $this_row 4]
		      set	    tmp_call_duration_s            [lindex $this_row 5]

#分品牌、主被叫时长(国际长途/港澳台长途--主被叫共12个指标)
          set value_list(890,BV${tmp_calltype_id}${tmp_tolltype_id}${tmp_brand_id}) [expr $value_list(890,BV${tmp_calltype_id}${tmp_tolltype_id}${tmp_brand_id}) + $tmp_call_duration_m ]
#主被叫分品牌合计时长(共6个指标)
          set value_list(890,BV${tmp_calltype_id}0${tmp_brand_id}) [expr $value_list(890,BV${tmp_calltype_id}0${tmp_brand_id}) + $tmp_call_duration_m ]
#分品牌主被叫合计(共3个指标)
          set value_list(890,BV200${tmp_brand_id}) [expr $value_list(890,BV200${tmp_brand_id}) + $tmp_call_duration_m ]
#主被叫分长途类型(共8个指标)
          set value_list(890,BV${tmp_calltype_id}${tmp_tolltype_id}1) [expr $value_list(890,BV${tmp_calltype_id}${tmp_tolltype_id}1) + $tmp_call_counts ]
          set value_list(890,BV${tmp_calltype_id}${tmp_tolltype_id}3) [expr $value_list(890,BV${tmp_calltype_id}${tmp_tolltype_id}3) + $tmp_call_duration_m ]
#主被叫合计时长(共4个指标)
          set value_list(890,BV${tmp_calltype_id}01) [expr $value_list(890,BV${tmp_calltype_id}01) + $tmp_call_counts ]
          set value_list(890,BV${tmp_calltype_id}03) [expr $value_list(890,BV${tmp_calltype_id}03) + $tmp_call_duration_m ]
#主被叫合计
          set value_list(890,BV2001) [expr $value_list(890,BV2001) + $tmp_call_counts ]
          set value_list(890,BV2003) [expr $value_list(890,BV2003) + $tmp_call_duration_m ]
  }

#====================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
#其中：港澳台漫游出访主叫合计	  BV3201	BV3203	BV3202	BV3207	BV3206
#其中：港澳台漫游出访被叫合计	  BV4201	BV4203	BV4202	BV4207	BV4206
	set sql_buf01 "select
                              case
                                  when b.crm_brand_id3 in ($rep_global_brand_id) then 2
                                  when b.crm_brand_id3 in ($rep_mzone_brand_id) then  6
                                  else 7
                              end,
                              case
                                   when a.calltype_id in (1) then 42
                                   else 32
                              end,
                  	          value(sum(a.CALL_COUNTS)-sum(case when a.rate_prod_id is null then a.CALL_COUNTS else 0 end)*2,0),
                  	          value(sum(a.call_duration_m),0)
                 from dw_call_$year$month a ,dw_product_$year$month b
                 where a.roamtype_id in (5) and a.TOLLTYPE_ID < 100 and a.user_id = b.user_id
                 group by    case
                                  when b.crm_brand_id3 in ($rep_global_brand_id) then 2
                                  when b.crm_brand_id3 in ($rep_mzone_brand_id) then  6
                                  else 7
                              end,
                              case
                                   when a.calltype_id in (1) then 42
                                   else 32
                              end"

	  puts $sql_buf01
	if [catch {aidb_sql $handle $sql_buf01} errmsg] {
		trace_sql $errmsg 1301
		return -1
	}

	while {[set this_row [aidb_fetch $handle]] != ""} {
	        set     tmp_brand_id                   [lindex $this_row 0]
	        set     tmp_calltype_id                [lindex $this_row 1]
	        set     tmp_call_counts                [lindex $this_row 2]
	        set     tmp_call_duration_m            [lindex $this_row 3]

#主被叫分品牌合计时长(共6个指标)
          set value_list(890,BV${tmp_calltype_id}0${tmp_brand_id}) [expr $value_list(890,BV${tmp_calltype_id}0${tmp_brand_id}) + $tmp_call_duration_m ]
#主被叫合计时长(共4个指标)
          set value_list(890,BV${tmp_calltype_id}01) [expr $value_list(890,BV${tmp_calltype_id}01) + $tmp_call_counts ]
          set value_list(890,BV${tmp_calltype_id}03) [expr $value_list(890,BV${tmp_calltype_id}03) + $tmp_call_duration_m ]

  }
#====================================================================================================
	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
		trace_sql $errmsg 1302
		return -1
	}
}
#====================================================================================================
	set   sqlbuf  "delete from stat_rep_content where op_time=$date_optime and rep_no in (657,658,659,660,661,662,663,664,665)"
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
	   	    set	rep_no		"657"
	   }
	   set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
	   if {$ret < 0} {
	   	trace_sql "Failed to insert the value of index 890,$index_name!" 1104
	   	return -1
	   }
  }
  foreach index_name $name_list2 {
	   if {![info exists value_list(890,$index_name)]} {
	   	    trace_sql "The value of index 890,$index_name does not exist!" 1105
	   	    return -1
	   } else {
	   	    set	index_value	$value_list(890,$index_name)
	   	    set	rep_no		"658"
	   }
	   set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
	   if {$ret < 0} {
	   	trace_sql "Failed to insert the value of index 890,$index_name!" 1105
	   	return -1
	   }
  }
  foreach index_name $name_list3 {
	   if {![info exists value_list(890,$index_name)]} {
	   	    trace_sql "The value of index 890,$index_name does not exist!" 1107
	   	    return -1
	   } else {
	   	    set	index_value	$value_list(890,$index_name)
	   	    set	rep_no		"659"
	   }
	   set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
	   if {$ret < 0} {
	   	trace_sql "Failed to insert the value of index 890,$index_name!" 1108
	   	return -1
	   }
  }
  foreach index_name $name_list4 {
	   if {![info exists value_list(890,$index_name)]} {
	   	    trace_sql "The value of index 890,$index_name does not exist!" 1109
	   	    return -1
	   } else {
	   	    set	index_value	$value_list(890,$index_name)
	   	    set	rep_no		"660"
	   }
	   set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
	   if {$ret < 0} {
	   	trace_sql "Failed to insert the value of index 890,$index_name!" 1110
	   	return -1
	   }
  }
  foreach index_name $name_list5 {
	   if {![info exists value_list(890,$index_name)]} {
	   	    trace_sql "The value of index 890,$index_name does not exist!" 1111
	   	    return -1
	   } else {
	   	    set	index_value	$value_list(890,$index_name)
	   	    set	rep_no		"661"
	   }
	   set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
	   if {$ret < 0} {
	   	trace_sql "Failed to insert the value of index 890,$index_name!" 1112
	   	return -1
	   }
  }
  foreach index_name $name_list6 {
	   if {![info exists value_list(890,$index_name)]} {
	   	    trace_sql "The value of index 890,$index_name does not exist!" 1113
	   	    return -1
	   } else {
	   	    set	index_value	$value_list(890,$index_name)
	   	    set	rep_no		"662"
	   }
	   set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
	   if {$ret < 0} {
	   	trace_sql "Failed to insert the value of index 890,$index_name!" 1114
	   	return -1
	   }
  }
  foreach index_name $name_list7 {
	   if {![info exists value_list(890,$index_name)]} {
	   	    trace_sql "The value of index 890,$index_name does not exist!" 1115
	   	    return -1
	   } else {
	   	    set	index_value	$value_list(890,$index_name)
	   	    set	rep_no		"663"
	   }
	   set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
	   if {$ret < 0} {
	   	trace_sql "Failed to insert the value of index 890,$index_name!" 1116
	   	return -1
	   }
  }
  foreach index_name $name_list8 {
	   if {![info exists value_list(890,$index_name)]} {
	   	    trace_sql "The value of index 890,$index_name does not exist!" 1117
	   	    return -1
	   } else {
	   	    set	index_value	$value_list(890,$index_name)
	   	    set	rep_no		"664"
	   }
	   set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
	   if {$ret < 0} {
	   	trace_sql "Failed to insert the value of index 890,$index_name!" 1118
	   	return -1
	   }
  }
  foreach index_name $name_list9 {
	   if {![info exists value_list(890,$index_name)]} {
	   	    trace_sql "The value of index 890,$index_name does not exist!" 1119
	   	    return -1
	   } else {
	   	    set	index_value	$value_list(890,$index_name)
	   	    set	rep_no		"665"
	   }
	   set ret [stat_insert_index $date_optime $rep_no "890,$index_name" $index_value]
	   if {$ret < 0} {
	   	trace_sql "Failed to insert the value of index 890,$index_name!" 1110
	   	return -1
	   }
  }

	return 0
}