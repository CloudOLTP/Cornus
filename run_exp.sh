#for exp in read_perc zipf num_nodes #failure_num_nodes
#do
#for st in redis blob #blob_iso blob
#do
#	exp_name="${exp}_${st}"
#	echo ${exp_name}
#	ls experiments/azure-redis/${exp_name}*
#	python3 test_exp.py CONFIG=experiments/azure-redis/${exp_name}.json MODE=release &> log/${exp_name}.log ;
#done
#done
#st="blob_iso"
#exp="num_nodes"
#exp_name="${exp}_${st}"
#echo ${exp_name}
#ls experiments/azure-redis/${exp_name}*
#python3 test_exp.py CONFIG=experiments/azure-redis/${exp_name}.json MODE=release &> log/${exp_name}.log ;
st="redis_repeated"
exp="read_perc"
exp_name="${exp}_${st}"
echo ${exp_name}
ls experiments/azure-redis/${exp_name}*
python3 test_exp.py CONFIG=experiments/azure-redis/${exp_name}.json MODE=release &> log/${exp_name}.log ;


