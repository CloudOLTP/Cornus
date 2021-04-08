cd ..
list=("ycsb_perc_remote" "ycsb_read_ratio" "ycsb_zipf" "ycsb_log_delay" "tpcc_wh")
for value in ${list[@]}
do
  python3 test_exp.py experiments/$value.json 0
  sleep 5
done
