# example usage
# python3 run_exp.py CONFIG=exp_profiles/test.json NODE_ID=0 DEBUG_MODE=debug [
# optional args]
# the first argument is the path to your exp settings, note that for any params with a list as 
# the values meaning multiple exps will be issued under each value
# the last argument is the index of current node corresponding to the ifconfig.txt
import os, sys, re, os.path
import subprocess, json
import threading
import multiprocessing
import time

ifconfig = "src/ifconfig.txt"


def run_process(conn, cmd, exit_on_err=False, print_stdout=True):
    print("[run_exp.py] executing remotely on {}: ".format(conn[1]) + cmd, flush=True)
    time.sleep(5)
    if conn[1] is None:
        return exec(cmd)
    ret = os.system("ssh {} \"{}\"".format(conn[1], cmd))
    if ret != 0 and exit_on_err:
        exit(1)
    return ret


def load_environment(fname="info.txt"):
    env = {
        "home": os.path.expanduser('~') + "/"
    }
    if os.path.exists(fname):
        lines = [line.strip() for line in open(fname)]
        env["user"] = lines[0]
        env["repo"] = lines[1]
    else:
        f = open(fname)
        env["user"] = input("[run_exp.py] enter user name: ")
        f.write(env["user"] + "\n")
        env["repo"] = os.getcwd()
        if input("[run_exp.py] confirm home directory : {}, y/n? ".format(
                env["repo"])) != "y":
            env["repo"] = input("[run_exp.py] enter home directory: ")
        if env["repo"][-1] != "/":
            env["repo"] = env["repo"] + "/"
        f.write(env["repo"] + "\n")
    return env


# system methods
def exec(cmd, exit_on_err=False):
    print("[run_exp.py] executing: " + cmd)
    try:
        subprocess.run(cmd, shell=True, check=True)
    except Exception as e:
        print("[run_exp.py] error executing: {}".format(cmd))
        print(e)
        if exit_on_err:
            exit(0)
        return 1
    return 0


def remote_exec(conn, cmd, exit_on_err=False, print_stdout=True,
                skip_warning=False):
    if conn[1] is None:
        return exec(cmd, exit_on_err=exit_on_err)
    print("[run_exp.py] executing remotely on {}: ".format(conn[1]) + cmd)
    ret = os.system("ssh {} \"{}\"".format(conn[1], cmd))
    if ret != 0 and exit_on_err:
        exit(1)
    return ret


# job loading methods
def load_job(args):
    # generate a dictionary based on a string
    # update experiment profile with specified string format configurations
    def parse_arg(arg):
        job = {}
        for item in arg:
            key = item.split("=")[0]
            value = item.split("=")[1]
            job[key] = value
        return job

    job_tmp = parse_arg(args)
    if "CONFIG" in job_tmp:
        print("[run_exp.py] loading config from {} ...".format(job_tmp["CONFIG"]))
        job = json.load(open(job_tmp["CONFIG"]))
        job.update(job_tmp)
    else:
        job = job_tmp
    return job


def generate_args(job):
    # generate a list of string format job through a job containing lists
    # by iterating all the combinations
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
    return args


def compress_job(job):
    arg = ""
    for key in job:
        if key == "CONFIG":
            continue
        arg += "{}={} ".format(key, job[key])
    return arg


def load_ipaddr(curr_node, env):
    import socket
    # EXCLUDED CURRENT NODE
    finish_compute_nodes = False
    start_storage_nodes = False
    myip = socket.gethostbyname(socket.gethostname())
    print("[run_exp.py] local ip is {}".format(myip))
    exec("cd {}; python3 install.py config_local 0".format(env["repo"]))
    storage_nodes = {}
    nodes = {}
    f = open(ifconfig)
    itr = 0
    for addr in f:
        if addr[0] == '#':
            continue
        elif (not finish_compute_nodes) and itr == env["num_nodes"]:
            print("[run_exp.py] finish checking compute node addrs")
            finish_compute_nodes = True
            # do not continue as this line may be =s
        if not finish_compute_nodes:
            if itr == curr_node:
                itr += 1
                continue
            print("[run_exp.py] try to connect to: node {} at {}".format(itr, addr.split(":")[0]))
            remote_exec((itr, addr.split(":")[0]),
                        "cd {}; python3 install.py config_local 0".format(
                            env["repo"]))
            nodes[itr] = (addr.split(":")[0], (itr, addr.split(":")[0]))
            itr += 1
            continue
        elif addr[0] == '=' and addr[1] == 's':
            start_storage_nodes = True
            print("[run_exp.py] start checking storage node addrs")
            itr = 0
            continue
        elif (not finish_compute_nodes) or (not start_storage_nodes):
            continue
        if myip == addr.split(":")[0]:
            print("[run_exp.py] storage node {} is local node".format(itr))
            storage_nodes[itr] = ("local", (itr, None))
        else:
            print(
                "[run_exp.py] try to connect to: storage node {} at {}".format(itr, addr.split(":")[0]))
            remote_exec((itr, addr.split(":")[0]),
                        "cd {}; python3 install.py config_local 0".format(
                            env["repo"]))
            storage_nodes[itr] = (addr.split(":")[0], (itr, addr.split(":")[0]))
        itr += 1
    f.close()
    return nodes, storage_nodes


# execution methods
def build_config(env, job, dest="src/config.h"):
    def replace(filename, pattern, replacement):
        f = open(filename)
        s = f.read()
        f.close()
        s = re.sub(pattern, replacement, s)
        f = open(filename, 'w')
        f.write(s)
        f.close()

    # build configuration file locally
    exec("cp {}{} {}{}".format(env["repo"], "src/config-std.h", env["repo"],
                               dest))
    for (param, value) in job.items():
        pattern = r"\#define\s*" + re.escape(param) + r'.*'
        if "ADDR" in param:
            replacement = "#define " + param + ' \"' + str(value) + '\"'
        else:
            replacement = "#define " + param + ' ' + str(value)
        replace("{}{}".format(env["repo"], dest), pattern, replacement)


def kill_nodes(user, nodes):
    print("[run_exp.py]  Failed. KILLING CURRENT SERVER ... ")
    for itr in nodes:
        remote_exec(nodes[itr][1], "sudo pkill rundb")
        print("[run_exp.py]  kill node {}".format(itr))


def parse_output(env, job, fname):
    output = open(fname)
    phase = 0
    success = False
    for line in output:
        if phase == 0:
            if "=Worker Thread=" in line:
                phase = 1
                success = True
                continue
        elif phase == 1:
            if "=Input/Output Thread=" in line:
                phase = 2
                continue
            line = line.strip()
            if ":" in line:
                # for token in line.strip().split('[summary]')[-1].split(','):
                list = line.split(':')
                # list = re.split(r'\s+|:\s+', line)
                key = list[0].strip()
                list[1] = list[1].strip()
                val = re.split(r'\s+', list[1])[0]
                job[key] = val
        # break
    output.close()
    if success:
        os.system("rm -f " + fname)
        stats = open("{}outputs/stats.json".format(env["repo"]), 'a+')
        stats.write(json.dumps(job) + "\n")
        stats.close()
    return success


def log_to_errors(job, fname):
    # log to errors
    if "EXP_ID" in job:
        i = job["EXP_ID"]
    else:
        i = 0
    if "CONFIG" in job:
        exp_name = job["CONFIG"].split('/')[-1].split('.')[0]
    else:
        exp_name = "unnamed"
    logpath = "../log/"
    logfile = "error_{}.list".format(exp_name)
    os.makedirs(logpath, exist_ok=True)
    error_log = open(logpath + logfile, "a+")
    error_log.write("{}, {}\n".format(i, job))
    error_log.close()
    os.system("cp {} {}error_{}_{}.out".format(fname, logpath, exp_name, i))


def start_nodes(env, job, nodes, storage_nodes, compile_only=True,
                is_debug=True):
    # compile storage node
    if int(job.get("NUM_STORAGE_NODES", 0)) > 0:
        exec("cd {}; python3 install.py sync {} {} {}".format(
            env["repo"],
            env["curr_node"], "100-0", "0-{}".format(int(job.get(
                "NUM_STORAGE_NODES", 0)) - 1)),
            exit_on_err=True)
        print("[run_exp.py] try to compile on storage node")
        # set up configuration
        job["NODE_TYPE"] = "STORAGE_NODE"
        build_config(env, job)
        for itr in storage_nodes:
            if itr == int(job["NUM_STORAGE_NODES"]):
                break
            if storage_nodes[itr][0] != "local":
                exec("scp -r {}src/config.h {}@{}:{}src/config.h".format(
                    env["repo"],
                    env["user"], storage_nodes[itr][0], env["repo"]),
                    exit_on_err=True)
            remote_exec(storage_nodes[itr][1], "cd {}src; make clean; ".format(
                env["repo"]))
            remote_exec(storage_nodes[itr][1], "sudo pkill runstorage; ")
            remote_exec(storage_nodes[itr][1], "{}tools/compile.sh "
                                               "runstorage".format(
                env["repo"], env["repo"]), exit_on_err=True, print_stdout=False,
                        skip_warning=True)
    else:
        exec("cd {}src; make clean; ".format(env["repo"]))

    # try compile locally
    job["NODE_TYPE"] = "COMPUTE_NODE"
    build_config(env, job)
    os.chdir("{}src/".format(env["repo"]))
    exec("sudo pkill rundb; ")
    exec("{}tools/compile.sh rundb > {}temp.out 2>&1".format(env["repo"], env["repo"]),
         exit_on_err=True)
    exec("rm -f {}outputs/temp.out".format(env["repo"]))

    # compile remotely
    for itr in nodes:
        if itr == int(job["NUM_NODES"]):
            break
        # copy config file
        exec("scp -r {}src/config.h {}@{}:{}src/config.h".format(
            env["repo"],
            env["user"], nodes[itr][0], env["repo"]), exit_on_err=True)
        remote_exec(nodes[itr][1], "sudo pkill rundb; ")
        if int(job.get("NUM_STORAGE_NODES", 0)) == 0:
            remote_exec(nodes[itr][1], "cd {}src; make clean; ".format(env["repo"]))
        # compile
        remote_exec(nodes[itr][1], "{}tools/compile.sh rundb".format(
            env["repo"], env["repo"]), exit_on_err=True, print_stdout=False,
                    skip_warning=True)

    if compile_only:
        return

    # start storage node
    storage_threads = []
    if int(job.get("NUM_STORAGE_NODES", 0)) > 0:
        for itr in storage_nodes:
            if itr == int(job["NUM_STORAGE_NODES"]):
                break
            print("[run_exp.py]  starting storage node {}".format(itr))
            # start server remotely
            # use another thread to do it asynchronously
            full_cmd = """cd {}src ; ./runstorage -Gn{}""".format(env["repo"], itr)
            # thread = myThread(storage_nodes[itr][1], full_cmd)
            # thread.start()
            # storage_threads.append(("storage-%d"%itr, thread))
            thread = multiprocessing.Process(target=run_process, args=(storage_nodes[itr][1], full_cmd))
            thread.start()
            storage_threads.append(("storage-%d"%itr, thread))

    # execute
    threads = []
    for itr in nodes:
        if itr == int(job["NUM_NODES"]):
            break
        print("[run_exp.py]  starting node {}".format(itr))
        # start server remotely
        # use another thread to do it asynchronously
        full_cmd = """cd {}tools ; ./run.sh -Gn{} | tee {}outputs/temp.out""".format(
            env["repo"], itr, env["repo"])
        # thread = myThread(nodes[itr][1], full_cmd)
        # thread.start()
        thread = multiprocessing.Process(target=run_process,
                                         args=(nodes[itr][1], full_cmd))
        thread.start()
        threads.append(("compute-%d"%itr, thread))
    # ret = remote_exec(nodes[itr][1], full_cmd)

    # start server locally
    os.chdir(env["repo"] + "tools")
    ret = exec("""./run.sh -Gn{} | tee {}outputs/temp-{}.out""".format(
        0, env["repo"], 0))
    if ret != 0:
        kill_nodes(env["user"], nodes)
        exit(0)

    # wait for completion
    for i, t in enumerate(threads):
        print("[run_exp.py] waiting for {} to join".format(t[0]))
        t[1].join()
        print("[run_exp.py] {} has joined".format(t[0]))

    for i, t in enumerate(storage_threads):
        print("[run_exp.py] waiting for {} to join".format(t[0]))
        t[1].terminate()
        print("[run_exp.py] {} has joined".format(t[0]))

    if is_debug:
        return
    # process results
    # copy temp from every non-failed node and rename it
    for itr in nodes:
        if itr == int(job["NUM_NODES"]):
            break
        addr = nodes[itr][0]
        # copy config file
        exec("scp {}@{}:{}outputs/temp.out {}outputs/temp-{}.out".format(
            env["user"], addr, env["repo"], env["repo"], itr))
    # then execute process command for each one.
    for itr in nodes:
        if itr == int(job["NUM_NODES"]):
            break
        job["NODE_ID"] = itr
        # if not successfully parsing, write to log
        if not parse_output(env, job, "{}outputs/temp-{}.out".format(
                env["repo"], itr)):
            log_to_errors(job, "{}outputs/temp-{}.out".format(env["repo"], itr))
    # process self results
    if not parse_output(env, job, "{}outputs/temp-{}.out".format(
            env["repo"], env["curr_node"])):
        log_to_errors(job, "{}outputs/temp-{}.out".format(env["repo"],
                                                          env["curr_node"]))


def test(env, nodes, storage_nodes, job):
    mode = job.get("MODE", "compile")
    if mode == "release" or mode == "debug":
        start_nodes(env, job, nodes, storage_nodes, compile_only=False,
                    is_debug=(mode == "debug"))
    elif mode == "compile":
        start_nodes(env, job, nodes, storage_nodes, compile_only=True,
                    is_debug=True)


def test_exp(env, nodes, storage_nodes, job):
    # parse commands
    exp_name = job["CONFIG"].split('/')[-1].split('.')[0]

    # generate config by iterating all the combinations of setups
    args = generate_args(job)

    # sync codebase
    print("[run_exp.py] syncing codebase with all nodes")
    if env["num_nodes"] > 1:
        exec("python3 install.py sync {} {}".format(
            env["curr_node"], "0-{}".format(env["num_nodes"]-1)),
            exit_on_err=True)

    # execute experiments
    mode = job.get("MODE", "debug")
    for i, arg in enumerate(args):
        arg += " EXP_ID={}".format(i)
        print("[run_exp.py] issue exp {}/{}".format(i + 1, len(args)))
        print("[run_exp.py] arg = {}".format(arg), flush=True)
        start_nodes(env, load_job(arg.split()), nodes, storage_nodes,
                    compile_only=(mode == "compile"),
                    is_debug=(mode == "debug"))
    print("[run_exp.py] FINISH WHOLE EXPERIMENTS", flush=True)

    if mode != "release":
        sys.exit()

    # process result on current node
    exec("cd {}outputs/; python3 collect_stats.py; mv stats.csv {}.csv; mv "
         "stats.json {}.json".format(env["repo"], exp_name, exp_name))
    print("[run_exp.py] FINISH processing results", flush=True)


if __name__ == "__main__":
    env = load_environment()
    # load job and start exp
    job = load_job(sys.argv[1:])
    # get current node and num_nodes
    env["curr_node"] = int(job.get("NODE_ID", "0"))
    num_nodes = job.get("NUM_NODES", 2)
    if isinstance(num_nodes, list):
        env["num_nodes"] = max([int(i) for i in num_nodes])
    else:
        env["num_nodes"] = int(num_nodes)
    # establish connections to all remote nodes
    nodes, storage_nodes = load_ipaddr(env["curr_node"], env)
    if "CONFIG" in job:
        test_exp(env, nodes, storage_nodes, job)
    else:
        test(env, nodes, storage_nodes, job)
    sys.exit()
