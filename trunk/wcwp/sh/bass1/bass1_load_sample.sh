#!/usr/bin/ksh


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
		/bassapp/backapp/bin/bass1_lst/bass1_lst.sh $deal_mo|grep "º”‘ÿ≥…π¶"
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
