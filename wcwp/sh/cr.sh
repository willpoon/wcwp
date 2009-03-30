SKM=ZHENGFENGMEI
echo DROP TABLE ${SKM}.$1\;>>.cr.tmp
echo CREATE TABLE ${SKM}.$1 \(>>.cr.tmp
echo _field1 >>.cr.tmp
echo _field2>>.cr.tmp
echo \)\;>>.cr.tmp
echo COMMIT\;>>.cr.tmp
cat .cr.tmp
rm .cr.tmp
