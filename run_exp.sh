for exp in failure_num_nodes num_nodes zipf read_perc 
do
for st in blob iso_blob
do
	exp_name="${exp}_${st}"
	echo ${exp_name}
	python3 test_exp.py CONFIG=experiments/azure-redis/${exp_name}.json MODE=release AZURE_ISOLATION_ENABLE=false &> log/${exp_name}.log ;
done
done

