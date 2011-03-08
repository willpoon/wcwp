##!/usr/bin/ksh
getxlsdata(){
				if [ $# -ne 2 ];then
				echo getxlsdata [inputfile] [yyyymm]
				return 2
				fi
				inputfile=$1
				tmp_voice_outputfile=fmt_voice_${data_mon}.tmp
				tmp_sms_outputfile=fmt_sms_${data_mon}.tmp
				voice_outputfile=load_voice_${data_mon}.txt
				sms_outputfile=load_sms_${data_mon}.txt
				data_mon=$2
				#step1:copy 28*p  data
				#step2:format data 0.000,save as voice_yyyymm.csv
				#step3:upload to unix
				#step4:run the following program!
				#step5:re-paste -d "," to excel (do some checksum)
				#step6:load data into tmp-table
				#step7:insert data into g_s_05001_month/g_s_05002_month
				#voice
				nawk 'BEGIN{FS=",";OFS=","}{print $1,$2,$4*1000,$3*1000}'  $inputfile >$tmp_voice_outputfile
				nawk 'BEGIN{FS=",";OFS=","}{print $5,$6,$8*1000,$7*1000}'  $inputfile >>$tmp_voice_outputfile
				nawk 'BEGIN{FS=",";OFS=","}{print $9,$10,$12*1000,$11*1000}'  $inputfile >>$tmp_voice_outputfile
				nawk -v v_mon=${data_mon} 'BEGIN{FS="\t";OFS=","}{print v_mon,v_mon,$3,$4,$5,$6,$7}' dim.txt > dim_voice.tmp
				paste -d "," dim_voice.tmp $tmp_voice_outputfile > $voice_outputfile
				#sms
				nawk 'BEGIN{FS=",";OFS=","}{if($14 != 0){print $14,$13,$16*1000,$15*1000}}'  $inputfile >$tmp_sms_outputfile
				nawk -v v_mon=${data_mon} 'BEGIN{FS="\t";OFS=","}{print v_mon,v_mon,$3,$4,$5,$6}' dim_sms.txt > dim_sms.tmp
				paste -d "," dim_sms.tmp $tmp_sms_outputfile > $sms_outputfile
				#rm tmp files
				rm *.tmp
				#print result
				cat $voice_outputfile
				echo " \n"
				echo " \n"
				echo " \n"
				echo " \n"
				echo " \n"
				cat $sms_outputfile
				db2 terminate
				db2 connect to BASSDB56 user bass2 using bass2
				echo db2 "delete from bass1.T_GS05001M where time_id = ${data_mon}"
				db2 "delete from bass1.T_GS05001M where time_id = ${data_mon}"
				echo db2 "load client from /bassapp/bass2/panzw2/bass1/$voice_outputfile of del  insert into  bass1.T_GS05001M"
				db2 "load client from /bassapp/bass2/panzw2/bass1/$voice_outputfile of del  insert into  bass1.T_GS05001M"
				echo db2 "delete from bass1.T_GS05002M where time_id = ${data_mon}"
				db2 "delete from bass1.T_GS05002M where time_id = ${data_mon}"
				echo db2 "load client from /bassapp/bass2/panzw2/bass1/$sms_outputfile of del  insert into  bass1.T_GS05002M"
				db2 "load client from /bassapp/bass2/panzw2/bass1/$sms_outputfile of del  insert into  bass1.T_GS05002M"
				db2 connect reset
				db2 terminate
}
