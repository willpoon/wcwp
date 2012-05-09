30 2 * * * . .profile;$HOME/bin/interface_monitor.sh > $HOME/bin/interface_monitor.log
20 00 * * * . .profile;/bassdb1/etl/E/boss/java/crm_interface/bin/java_crm_interface.sh
00 04 01 * * . .profile;/bassdb1/etl/E/boss/java/crm_interface/bin/java_crm_interfaceMonth.sh
0,5,10,15,20,25,30,35,40,45,50,55 * * * * . .profile;/bassdb1/etl/E/boss/java/crm_interface/bin/monitor_ftp.sh
