echo "结果文件名:">>rsspec.txt
echo 记录数		文件名 >>rsspec.txt
wc -l rs_*.txt>>rsspec.txt
echo " ">>rsspec.txt
echo "格式: ">>rsspec.txt
echo "">>rsspec.txt
echo "字段参数说明:">>rsspec.txt
echo "">>rsspec.txt
echo "其他注意事项:">>rsspec.txt
cat rsspec.txt
rm -rf rsspec.txt
