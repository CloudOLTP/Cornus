import os, sys, re, os.path
import platform
import subprocess, datetime, time, signal, json
import pandas as pd


user = "kanwu"

def produce_avg(df_result, df_list, col, output_cnt):
	sum_val = df_result[col]
	for i in range(1, output_cnt):
		sum_val += df_list[i][col]
	df_result["avg_"+col] = sum_val / output_cnt
	return df_result

if __name__ == "__main__":
    # usage: python3 collect_result_remote.py [exp_name]
	curr_node = 0
	exp_name = sys.argv[1]
	if len(sys.argv) > 2:
		skip_node = int(sys.argv[2])
	else:
		skip_node = -1
	ifconfig = open("../ifconfig.txt")
	i = 0
	node_type = 1
	for line in ifconfig:
		if line[0] == '#':
			continue
		elif line[0] == '=':
			break
		else:
			line = line.split(':')[0].strip()
			if i == skip_node:
				continue
			elif i != curr_node:
				print("collecting from node: {}".format(line))
				ret = os.system("scp {}@{}:/home/{}/Sundial/outputs/{}.csv ./{}{}.csv".format(user, line, user, exp_name, exp_name, str(i)))
				if ret != 0:
					time.sleep(1)
					os.system("scp {}@{}:/home/{}/Sundial/outputs/{}.csv ./{}{}.csv".format(user, line, user, exp_name, exp_name, str(i)))
			else:
				os.system("cp ../outputs/{}.csv ./{}{}.csv".format(exp_name, exp_name, i))
			i += 1
	ifconfig.close()
	output_cnt = i
	df_list = []
	for i in range(0, output_cnt):
		df = pd.read_csv(exp_name + str(i)+'.csv') 
		df_list.append(df)
	df_concat = pd.concat(df_list, ignore_index=True)
	df_concat.to_csv("../outputs/" + exp_name+"_concat.csv", index=False)
	os.system("rm *.csv")
    # clean outputs
	os.system("rm ../outputs/{}.csv".format(exp_name))
