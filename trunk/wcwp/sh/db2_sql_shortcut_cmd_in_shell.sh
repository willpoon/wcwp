db2 "CHANGE ISOLATION TO UR";
db2 "CONNECT TO acrm USER zhengfm USING itc@gmcc";
db2 "select * from $1 order by 1 with ur"
