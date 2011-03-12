nawk -F"\t" '{ printf "\t%-20s\t%-20s%-20s\n", ","$1,$2,"--"$3 }' fmt.txt
