make -j16 &> temp.out
for i in 2 #3 4 #5 6 7 8
do
	rsync -av --delete /home/kanwu/Sundial/test_storage/ifconfig.txt kanwu@compute${i}:/home/kanwu/Sundial/test_storage/ifconfig.txt
	rsync -av --delete /home/kanwu/Sundial/test_storage/config-std.h kanwu@compute${i}:/home/kanwu/Sundial/test_storage/config-std.h
	#ssh compute${i} 'cd ~/Sundial/test_storage/ ; export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH ; make clean; make -j16 &> temp.out &';
	#echo "compile on node ${i}"
done
for i in 2 #3 4 #5 6 7 8
do
	#ssh compute${i} 'cd ~/Sundial/test_storage/ ; ./run_test_network -Gn${i} &';
done
#./run_test_network -Gn1
        
    
