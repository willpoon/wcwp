
yesterday()
{
	#usage:yesterday yyyymmdd
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`
        day=`echo "$1"|cut -c7-8`

        month=`expr $month + 0`
        day=`expr $day - 1`

        if [ $day -eq 0 ]; then
                month=`expr $month - 1`
                if [ $month -eq 0 ]; then
                        month=12
                        day=31
                        year=`expr $year - 1`
                else
                        case $month in
                                1|3|5|7|8|10|12) day=31;;
                                4|6|9|11) day=30;;
                                2)
                                        if [ `expr $year % 4` -eq 0 ]; then
                                                if [ `expr $year % 400` -eq 0 ]; then
                                                        day=29
                                                elif [ `expr $year % 100` -eq 0 ]; then
                                                        day=28
                                                else
                                                        day=29
                                                fi
                                        else
                                                day=28
                                        fi ;;
                        esac
                fi
        fi

        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi

        if [ $day -lt 10 ] ; then
                day=`echo "0$day"`
        fi
        echo $year$month$day
        return 1
}

#��DB2���ݿ���ִ��SQL
DB2_SQL_EXEC()
{
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="bass2"
    db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
    eval $DB2_SQLCOMM 
		if [ $? -ne 0 ];then 
		echo "error occurs when running DB2_SQLCOMM!"
		db2 connect reset 
		return 1
		fi
    db2 commit 
    db2 connect reset 
    return 0
}

sendalarmsms(){
	if [ $# -ne 1 ];then 
	echo sendalarmsms msg
	exit
	fi
	MESSAGE_CONTENT=$1
	sql_str="insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,MOBILE_NUM) \
	select  ('${MESSAGE_CONTENT}'),MOBILE_NUM from bass1.mon_user_mobile
	"
	DB2_SQLCOMM="db2 \"${sql_str}\""
	DB2_SQL_EXEC > /dev/null
	
  if [ $? -ne 0 ];then 
	echo "error occurs while generating alert msg!"
	return 1
	fi	
	return 0
}



#�����ļ���������
fn_d_file_lvl_ret_cnt(){
	sql_str=" select dummy,cnt from table(bass1.get_flret_cnt()) a
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d_file_lvl_ret_cnt:${filelvl_ret_cnt}"
	return ${filelvl_ret_cnt}
	}


#�����ļ��������� (9��ӿ�)
fn_d9_file_lvl_ret_cnt(){
	sql_str=" select dummy,cnt from table(bass1.fn_dn_flret_cnt(9)) a
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "fn_d9_file_lvl_ret_cnt:error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d9_file_lvl_ret_cnt:${filelvl_ret_cnt}"
	return ${filelvl_ret_cnt}
	}


#�����ļ��������� (11��ӿ�)
fn_d11_file_lvl_ret_cnt(){
	sql_str=" select dummy,cnt from table(bass1.fn_dn_flret_cnt(11)) a
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	#echo ${DB2_SQLCOMM}
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "fn_d11_file_lvl_ret_cnt:error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d11_file_lvl_ret_cnt:${filelvl_ret_cnt}"
	return ${filelvl_ret_cnt}
	}



#�����ļ��������� (13��ӿ�)
fn_d13_file_lvl_ret_cnt(){
	sql_str=" select dummy,cnt from table(bass1.fn_dn_flret_cnt(13)) a
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "fn_d13_file_lvl_ret_cnt:error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d13_file_lvl_ret_cnt:${filelvl_ret_cnt}"
	return ${filelvl_ret_cnt}
	}




#�����ļ��������� (15��ӿ�)
fn_d15_file_lvl_ret_cnt(){
	sql_str="select dummy,cnt from table(bass1.fn_dn_flret_cnt(15)) a"
	DB2_SQLCOMM="db2 \"${sql_str}\""
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "fn_d15_file_lvl_ret_cnt:error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d15_file_lvl_ret_cnt:${filelvl_ret_cnt}"
	return ${filelvl_ret_cnt}
	}




#���ռ�¼��������
fn_d_recordlvl_ret_cnt(){
	sql_str="select 'xxxxx',count(0) from app.g_runlog \
					 where time_id=int(replace(char(current date - 1 days),'-','')) \
					 and return_flag=1 with ur 
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	reclvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	return ${reclvl_ret_cnt}
	}


getunixtime2(){
	#sun os date ������ unixtimestamp , Ψ��������ȡ�ˣ���һ���ľ�����
		if [ $# -ne 0 ];then
		echo "getmtime"
		exit
		fi
		touchfile="."
		touch $touchfile
		in_file=$touchfile
		if [ ! -d ${in_file} ];then 
		echo "getmtime ���������ļ� ${in_file} ������!!"
		return 1
		fi
		tcltimestamp=`echo "puts [file mtime ${in_file}]"|tclsh|awk -F'\r' '{print $1}'`
		echo  $tcltimestamp
		return 0
}


load_sample() {
# no paramter
deal_date=`date +%d`
echo $deal_date
deal_mon=`date +%Y%m`
echo $deal_mon
deal_mo=`date +%Y-%m`
echo $deal_mo

if [ ${deal_date} -eq "01" ];then
	if [ ! -f /bassapp/backapp/bin/bass1_lst/${deal_mon}.done ] ; then 
		/bassapp/backapp/bin/bass1_lst/bass1_lst.sh $deal_mo|grep "���سɹ�"
		if [ $? -eq 0 ] ; then 
		touch /bassapp/backapp/bin/bass1_lst/${deal_mon}.done
		else 
		echo "not load!"
		fi
	else 
	echo "already loaded!"
	fi 
else 
echo "not today!"
fi

}


