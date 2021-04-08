import os, sys, re, os.path
import platform
import subprocess, datetime, time, signal, json
import pandas as pd

def produce_avg(df_result, df_list, col, output_cnt):
	sum_val = df_result[col]
	for i in range(1, output_cnt):
		sum_val += df_list[i][col]
	df_result["avg_"+col] = sum_val / output_cnt
	return df_result

if __name__ == "__main__":
	exp_name = sys.argv[1]

	ifconfig = open("ifconfig.txt")
	i = 0
	node_type = 1
	for line in ifconfig:
		if line[0] == '#':
			if line[1] == '=':
				node_type = 2
			continue
		else:
			line = line.split(':')[0]
			line = line.strip('\n')
			if node_type == 1:
				# os.system("ssh LockeZ@{} 'cd Sundial/outputs/; python3 collect_stats.py; mv stats.csv {}.csv; mv stats.json {}.json'".format(line, exp_name, exp_name))
				ret = os.system("scp LockeZ@{}:/users/LockeZ/Sundial/outputs/{}.csv ./{}{}.csv".format(line, exp_name, exp_name, str(i)))
				if ret != 0:
					time.sleep(1)
					os.system("scp LockeZ@{}:/users/LockeZ/Sundial/outputs/{}.csv ./{}{}.csv".format(line, exp_name, exp_name, str(i)))
				i += 1
	ifconfig.close()
	output_cnt = i
	df_list = []
	for i in range(0, output_cnt):
		df = pd.read_csv(exp_name + str(i)+'.csv') 
		df_list.append(df)
	df_concat = pd.concat(df_list, ignore_index=True)
	df_concat.to_csv(exp_name+"_concat.csv", index=False)
	# df_result = df_list[0]
	# sum_thruput = df_result["Throughput"]
	# sum_latency = df_result["average_dist_latency"]
	# for i in range(1, output_cnt):
	# 	sum_thruput += df_list[i]["Throughput"]
	# 	sum_latency += df_list[i]["average_dist_latency"]
	# avg_latency = sum_latency/output_cnt
	# df_result["sum_throughput"] = sum_thruput
	# df_result["avg_avg_dist_latency"] = avg_latency
	# df_result = produce_avg(df_result, df_list, "multi_part_execute_phase (in us)", output_cnt)
	# df_result = produce_avg(df_result, df_list, "multi_part_prepare_phase (in us)", output_cnt)
	# df_result = produce_avg(df_result, df_list, "multi_part_commit_phase (in us)", output_cnt)
	# df_result = produce_avg(df_result, df_list, "multi_part_abort (in us)", output_cnt)
	# df_result = produce_avg(df_result, df_list, "multi_part_cleanup_phase (in us)", output_cnt)
	# df_result = produce_avg(df_result, df_list, "average_latency", output_cnt)
	# df_result.to_csv(exp_name+"_final.csv", index=False)
	# os.system("mv ./*.csv ./outputs")