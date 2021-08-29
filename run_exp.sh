# python3 test_exp.py CONFIG=experiments/ycsb_failure.json MODE=release
# python3 test_exp.py CONFIG=experiments/azure-redis/num_nodes_blob_0828.json MODE=release &> log/num_nodes_blob_0828.log ;
# python3 test_exp.py CONFIG=experiments/azure-redis/num_nodes_redis_0828.json MODE=release &> log/num_nodes_redis_0828.log
python3 test_exp.py CONFIG=experiments/azure-redis/failure_zipf_redis_0829.json MODE=release &> log/failure_zipf_redis_0829.log
python3 test_exp.py CONFIG=experiments/azure-redis/failure_zipf_blob_0829.json MODE=release &> log/failure_zipf_blob_0829.log
python3 test_exp.py CONFIG=experiments/azure-redis/failure_num_nodes_blob_0829.json MODE=release &> log/failure_num_nodes_blob_0829.log
python3 test_exp.py CONFIG=experiments/azure-redis/failure_num_nodes_redis_0829.json MODE=release &> log/failure_num_nodes_redis_0829.log

