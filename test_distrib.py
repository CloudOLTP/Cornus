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
        if addr[0] == '#':
            continue
        elif addr[0] == '=' and addr[1] == 'l':
            if eval_arg("LOG_DEVICE", "LOG_DVC_REDIS", job, default=True):
                break
            else:
                log_node = "true"
            continue
        job["NODE_ID"] = num_nodes
        job["LOG_NODE"] = log_node
        cmd = "python3 test.py {}".format(compress_job(job))
        if curr_node != num_nodes:
            # start server remotely
            addr = addr.split(':')[0]
            os.system("ssh {} 'sudo pkill rundb'".format(addr))
            ret = os.system("ssh {} 'cd ~/Sundial/ ; export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH ; sudo {} &' ".format(addr, cmd))
            if ret != 0:
                err_msg = "error executing server"
                job['ERROR'] = err_msg
                print("ERROR: " + err_msg)
            else:
                print("[LOG] start node {}".format(num_nodes))
        num_nodes += 1
	# start server locally
    job["NODE_ID"] = curr_node
    job["LOG_NODE"] = False
    cmd = "python3 test.py {}".format(compress_job(job))
    os.system("sudo pkill rundb")
    os.system("export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH")
    ret = os.system(cmd)
    return ret

def kill_nodes(curr_node):
    f = open(ifconfig)
    num_nodes = 0
    for addr in f:
        if '#' in addr:
            continue
        if curr_node == num_nodes:
            continue
        os.system("ssh {} 'sudo pkill rundb'".format(addr))
        print("[LOG] kill node {}".format(num_nodes))
        num_nodes += 1


if __name__ == "__main__":
    script = "test.py"
    start_nodes(sys.argv[1:], 0)
        
    
