# run all nodes in ifconfig.
# example usage:
# python3 test_distrib.py CONFIG=experiments/[name].json NODE_ID=0 [optional args]
# where NODE_ID specifies the index of current server matched in ifconfig.txt
# and the rest can be configurations you wanna overwrite in config.h
import os, sys, re, os.path
import json
from test import load_job, eval_arg

ifconfig = "ifconfig.txt"

def compress_job(job):
    arg = ""
    for key in job:
        if key == "CONFIG":
            continue
        arg += "{}={} ".format(key, job[key])
    return arg

def start_nodes(arg, curr_node):
    f = open(ifconfig)
    num_nodes = 0
    log_node = "false"
	job = load_job(arg) 
    for addr in f:
        if '#' in addr:
            if addr[1] == 'l' and eval_arg("LOG_DEVICE","LOG_DEVICE_REDIS", default=True): 
                log_node = "true"
            continue
		cmd = "python3 test.py NODE_ID={} LOG_NODE={}".format(num_nodes, log_node)
        if curr_node == num_nodes:
            # start server locally
            os.system("sudo pkill rundb")
            os.system("export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH")
            ret = os.system("{} &".format(cmd))
        else:
            # start server remotely
			addr = addr.split(':')[0]
			os.system("ssh {} 'sudo pkill rundb'".format(addr))
			ret = os.system("ssh {} 'cd ~/Sundial/ ; export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH ; sudo {}' &".format(addr, cmd))
        if ret != 0:
            err_msg = "error executing server"
            job['ERROR'] = err_msg
            print("ERROR: " + err_msg)
        else:
            print("[LOG] start node {}".format(num_nodes))
        num_nodes += 1

def kill_nodes(curr_node):
    f = open(ifconfig)
    num_nodes = 0
    for addr in f:
        if '#' in line:
            continue
        if curr_node == num_nodes:
            continue
        os.system("ssh {} 'sudo pkill rundb'".format(addr))
        print("[LOG] kill node {}".format(num_nodes))
        num_nodes += 1


if __name__ == "__main__":
    arg = sys.argv[1]
    script = "test.py"
    start_nodes(arg, 0)
        
    
