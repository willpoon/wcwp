putdatfile(){
FTPHOST=172.16.9.25
REMOTE_DIR=data
LOCAL_DIR=/bassapp/bihome/panzw
HOME=/bassapp/bihome/panzw/config
export HOME

ftp_mac_put_dat_file=${HOME}/put_dat.mac.ftp
ftped_file_list=${HOME}/ftped_dat.lst
#����ftp�����ļ�
echo "cd ${REMOTE_DIR}" > ${ftp_mac_put_dat_file}
echo "bin" > ${ftp_mac_put_dat_file}
echo "prompt off" > ${ftp_mac_put_dat_file}
echo "mput *.dat" > ${ftp_mac_put_dat_file}
echo "ls -lrt" >> ${ftp_mac_put_dat_file}
echo "dir *.dat ${ftped_file_list}" >> ${ftp_mac_put_dat_file}
#�ϴ�
ftp -v ${FTPHOST} < ${ftp_mac_put_dat_file}

#�뱾��У�飺�ļ���|�ļ���|�ļ���С


#�ָ�$HOME
HOME=/bassapp/bass1
export HOME
echo $$
}



putverffile(){
FTPHOST=172.16.9.25
REMOTE_DIR=data
LOCAL_DIR=/bassapp/bihome/panzw
HOME=/bassapp/bihome/panzw
export HOME

ftp_mac_put_verf_file=${HOME}/put_verf.mac.ftp


#����ftp�����ļ�
echo "cd ${REMOTE_DIR}" > ${ftp_mac_put_verf_file}
echo "bin" >> ${ftp_mac_put_verf_file}
echo "prompt off" >> ${ftp_mac_put_verf_file}
echo "mput *.verf" >> ${ftp_mac_put_verf_file}
echo "ls -lrt" >> ${ftp_mac_put_verf_file}
#�ϴ�
if [ -f ${ftp_mac_put_dat_file} ];then 
ftp -v ${FTPHOST} <  ${ftp_mac_put_verf_file}
rm ${ftp_mac_put_dat_file}
else 
echo "dat �ļ�δ�ϴ��������ϴ�dat���ٴ�verf!!"
return 1
fi

#�ָ�$HOME
HOME=/bassapp/bass1
export HOME
echo $$
}

