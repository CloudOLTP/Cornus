make -j16 &> temp.out
for i in $(seq 2 1 ${1})
do
	j=$((i-1))
	echo $j
	ssh compute${i} 'cd ~/Sundial/test_storage/ ; ./run_test_network -Gn'"${j}"' &' &
done
./run_test_network -Gn0
        
    
