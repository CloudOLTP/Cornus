# example usage
# python3 test_exp.py CONFIG=experiments/test.json NODE_ID=0 DEBUG_MODE=debug [optional args]
# the first argument is the path to your exp settings, note that for any params with a list as 
# the values meaning multiple exps will be issued under each value
# the last argument is the index of current node corresponding to the ifconfig.txt
import os, sys, re, os.path
import subprocess, datetime, time, signal, json
from test_distrib import start_nodes, kill_nodes
from test import parse_arg, try_compile, collect_result, load_job, eval_arg

script = "test_distrib.py"

if __name__ == "__main__":
    job = load_job(sys.argv[1:])
    assert("CONFIG" in job) 
    exp_name = job["CONFIG"].split('/')[-1].split('.')[0]
    if "NODE_ID" in job: 
        curr_node = int(job["NODE_ID"])
    else:
        curr_node = 0 
    args = [""]
    for key in job:
        new_args = []
        if key == "CONFIG":
            continue
        if isinstance(job[key], list):
            for i, x in enumerate(job[key]):
                for arg in args:
                    arg = arg + "{}={} ".format(key, x)
                    new_args.append(arg)
        else:
            for arg in args:
                arg = arg + "{}={} ".format(key, job[key])
                new_args.append(arg)
        args = new_args
    for i, arg in enumerate(args):
        print("[LOG] issue exp {}/{}".format(i+1, len(args)))
        print("[LOG] arg = {}".format(arg))
        if script == "test_distrib.py":
            ret = start_nodes(arg.split(), curr_node)
            if ret != 0:
                continue
            print("[LOG] KILLING CURRENT SERVER ... ")
            # kill the remote servers
            kill_nodes(curr_node)
            print("[LOG] FINISH EXECUTION ")
        else:
            main(arg)
    if eval_arg("MODE", "release", job, default=True):
        os.system("cd outputs/; python3 collect_stats.py; mv stats.csv {}.csv; mv stats.json {}.json".format(exp_name, exp_name))
    print("[LOG] FINISH WHOLE EXPERIMENTS")
    print("[LOG] Start collecting results from remote")
    f = open('ifconfig.txt')
    num_nodes = 0
    for addr in f:
        if num_nodes == job["NUM_NODES"]:
            print("[LOG] num_nodes == job[\"NUM_NODES\"]. Stop ssh-ing remote.")
            break
        if '#' in addr:
            continue
        if "=l" in addr:
            print("[LOG] end of ifconfig.txt reached. Stop ssh-ing remote.")
            break
        if curr_node == num_nodes:
            num_nodes += 1
            continue
        # start server
        addr = addr.split(':')[0]
        os.system("ssh {} 'cd Sundial/outputs/; python3 collect_stats.py; mv stats.csv {}.csv; mv stats.json {}.json'".format(addr, exp_name, exp_name))
        num_nodes += 1
    suffix = ""
    if eval_arg("FAILURE_ENABLE", "true", job, default=False):
        suffix = " {}".format(job["FAILURE_NODE"])
    os.system("cd tools; python3 collect_remote_result.py {} {}".format(exp_name, num_nodes)+suffix)
    print("[LOG] FINISH collecting results")
    #os.system("python3 send_email.py {}".format(exp_name))
