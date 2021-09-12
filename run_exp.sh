for exp in zipf #read_perc zipf num_nodes failure_num_nodes
do
for st in redis_repeated #iso_blob blob 
do
	exp_name="${exp}_${st}"
	echo ${exp_name}
	ls experiments/azure-redis/${exp_name}*
	python3 test_exp.py CONFIG=experiments/azure-redis/${exp_name}.json MODE=release &> log/${exp_name}.log ;
done
done

