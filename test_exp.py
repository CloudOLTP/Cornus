# example usage
# python3 test_exp.py experiments/test.json 0
# the first argument is the path to your exp settings, note that for any params with a list as 
# the values meaning multiple exps will be issued under each value
# the last argument is the index of current node corresponding to the ifconfig.txt
import os, sys, re, os.path
import subprocess, datetime, time, signal, json
from test_distrib import start_nodes, kill_nodes
from test import compile_and_run, parse_arg

script = "test_distrib.py"

if __name__ == "__main__":
    if len(sys.argv) > 3:
        mode = sys.argv[3]
    else:
        debug_mode = "release"
    job = json.load(open(sys.argv[1]))
    exp_name = sys.argv[1].split('.')[0]
    if '/' in exp_name:
        exp_name = exp_name.split("/")[-1]
        curr_node = int(sys.argv[2])
    args = [""]
    for key in job:
        new_args = []
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
    #if os.path.exists("outputs/stats.json"):
    #	os.remove("outputs/stats.json")
    for i, arg in enumerate(args):
        print("[LOG] issue exp {}/{}".format(i+1, len(args)))
        print("[LOG] arg = {}".format(arg))
        if script == "test_distrib.py":
            ret = start_nodes(arg, curr_node, debug_mode)
            if ret != 0:
                continue
            print("[LOG] KILLING REMOTE SERVER ... ")
            # kill the remote servers
            kill_nodes(curr_node)
            os.system("ssh node-1 'sudo pkill rundb'")
            print("[LOG] FINISH EXECUTION ")
        else:
            job = parse_arg(arg)
            if debug_mode == "compile":
                try_compile(job)
            elif debug_mode == "debug":
                compile_and_run(job)
            elif debug_mode == "release":
                compile_and_run(job)
                collect_result(job);
    if debug_mode == "release":
        os.system("cd outputs/; python3 collect_stats.py; mv stats.csv {}.csv; mv stats.json {}.json".format(exp_name, exp_name))
    print("[LOG] FINISH WHOLE EXPERIMENTS")
