#################################################################
# ��������: tcl_round.tcl
# ��������:
# �������:
#	    op_time		����ʱ�䣨���Σ�,��yyyy-mm-dd��
#	    province_id	        ʡ�ݱ���
#	    redo_number	        �ش���š�ֻ�����յ�һ�����붨�������ļ���Ҫ�ش����
#	    trace_fd	        trace�ļ����
#	    temp_data_dir	��ʱ�����ļ��Ĵ��λ�ã�$BASS1FILEDIR/tempdata/�����ڱ���e_transform���ɵ������ļ�
#	    semi_data_dir	�м������ļ��Ĵ��λ�ã�$BASS1FILEDIR/semidata/�����ڱ���.bass1��.bass2��.sample���ļ�
#	    final_data_dir	���������ļ��Ĵ��λ�ã�$BASS1FILEDIR/finaldata/�����ڱ���.dat����һ�����붨�������ļ�
#	    conn		���ݿ�����
#	    src_data	        ����Դ
#	    obj_data	        ����Ŀ��
#�������:
# ����ֵ:   0 �ɹ�; -1 ʧ��
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
	set p_optime $op_time
	set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set this_month [string range $op_time 0 3][string range $op_time 5 6]
	set db_user $env(DB_USER)

        set dNum 0.43542
        set iBit 3
        
        set iZero 0
        set iLen [string length $dNum]
        puts $iLen
        
        set iPos [string first "." $dNum ]
        puts $iPos
        
        set  x1 [format "%.02f" [expr ($dNum)]] 
        puts $x1

        set  x2 [format "%.03f" [expr ($dNum)]] 
        puts $x2
        
#        #����ֵΪ����
#        if { $iPos==-1} {
#          set dResult $dNum.
#          puts $dResult
#
#          for { set iStep 0 } {$iStep <= [expr $iBit -1 ]} {incr iStep} {
#          	set dResult $dResult$iZero
#          	}
#          }
#          puts $dResult      
#        }
#        if { $iLen <= [expr ($iPos+$iBit) ]} {
#           set dResult $dNum
#        } else {
#            if { [string range $dNum [expr ($iPos+$iBit) ] [[expr ($iPos+$iBit+1) ]] >4 } {
#               
#            	set [expr ]
#            }
#        }
#        
        
	return 0
}