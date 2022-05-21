# example usage
# python3 run_exp.py CONFIG=exp_profiles/test.json NODE_ID=0 DEBUG_MODE=debug [
# optional args]
# the first argument is the path to your exp settings, note that for any params with a list as 
# the values meaning multiple exps will be issued under each value
# the last argument is the index of current node corresponding to the ifconfig.txt
import os, sys, re, os.path
import subprocess, datetime, time, signal, json, paramiko


ifconfig = "src/ifconfig.txt"


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
		env["user"] = input("enter user name: ")
		f.write(env["user"] + "\n")
		env["repo"] = os.getcwd()
		if input("confirm home directory : {}, y/n? ".format(env["repo"])) != "y":
			env["repo"] = input("enter home directory: ")
		if env["repo"][-1] != "/":
			env["repo"] = env["repo"] + "/"
		f.write(env["repo"] + "\n")
	return env

# system methods
def exec(cmd, exit_on_err=False):
	try:
		subprocess.run(cmd, shell=True, check=True)
	except Exception as e:
		print ("error executing: {}".format(cmd))
		print(e)
		if exit_on_err:
			exit(0)
		return 1
	return 0


def remote_exec(conn, cmd, exit_on_err=False):
	stdin, stdout, stderr = conn.exec_command(cmd)
	if stderr.read() == b'':
		for line in stdout.readlines():
			print(line.strip()) # strip the trailing line breaks
	else:
		print ("error executing: {}".format(cmd))
		print(stderr.read())
		if exit_on_err:
			exit(0)
		return 1
	return 0
	# except Exception as e:
	# 	print ("error executing: {}".format(cmd))
	# 	print(e)
	# 	if exit_on_err:
	# 		exit(0)
	# 	return 1
	#return exec("ssh {} '{}'".format(addr, cmd), exit_on_err)


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
		print("loading config from {} ...".format(job_tmp["CONFIG"]))
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
	# EXCLUDED CURRENT NODE
	nodes = {}
	f = open(ifconfig)
	itr = 0
	for addr in f:
		if addr[0] == '#':
			continue
		elif itr == curr_node:
			itr += 1
			continue
		elif addr[0] == '=' and addr[1] == 'l':
			break
		print("try to connect to: node {} at {}".format(itr, addr.split(":")[
			0]))
		con = paramiko.SSHClient()
		con.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		con.load_system_host_keys()
		con.connect(addr.split(":")[0], username=env["user"],key_filename="{}.ssh/id_ed25519".format(env["home"]))
		nodes[itr] = (addr.split(":")[0], con)
		itr += 1
	f.close()
	return nodes


# execution methods
def build_config(env, job):
	def replace(filename, pattern, replacement):
		f = open(filename)
		s = f.read()
		f.close()
		s = re.sub(pattern,replacement,s)
		f = open(filename,'w')
		f.write(s)
		f.close()
	# build configuration file locally
	exec("cp {}{} {}{}".format(env["repo"], "src/config-std.h", env["repo"], "src/config.h"))
	for (param, value) in job.items():
		pattern = r"\#define\s*" + re.escape(param) + r'.*'
		if "ADDR" in param:
			replacement = "#define " + param + ' \"' + str(value) + '\"'
		else:
			replacement = "#define " + param + ' ' + str(value)
		replace("{}src/config.h".format(env["repo"]), pattern, replacement)


def kill_nodes(user, nodes):
	print("[LOG] Failed. KILLING CURRENT SERVER ... ")
	for itr in nodes:
		remote_exec(nodes[itr][1], "sudo pkill rundb")
		print("[LOG] kill node {}".format(itr))


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
		stats.write(json.dumps(job)+"\n")
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
	error_log = open("log/error_{}.list".format(exp_name), "a+")
	error_log.write("{}, {}\n".format(i, arg))
	error_log.close()
	os.system("cp {} log/error_{}_{}.out".format(fname, exp_name, i))


def start_nodes(env, job, nodes, curr_node, compile_only=True):
	# build configuration
	build_config(env, job)

	# try compile locally
	os.chdir("{}src/".format(env["repo"]))
	exec("sudo pkill rundb;")
	exec("make clean > {}temp.out 2>&1".format(env["repo"]), exit_on_err=True)
	exec("{}tools/compile.sh > {}temp.out 2>&1".format(env["repo"]),
		 env["repo"], exit_on_err=True)
	exec("rm -f {}outputs/temp.out".format(env["repo"]))

	# compile remotely
	for itr in nodes:
		# copy config file
		exec("scp -r config.h {}@{}:{}src/config.h".format(
			env["user"], nodes[itr][0], env["repo"]), exit_on_err=True)
		# compile
		remote_exec(nodes[itr][1], "sudo pkill rundb; ")
		remote_exec(nodes[itr][1], "cd {}src; {}tools/compile.sh".format(
			env["repo"], env["repo"]), exit_on_err=True)
	if compile_only:
		return

	# execute
	for itr in nodes:
		print("[LOG] starting node {}".format(itr))
		# start server remotely
		full_cmd = "cd {}tools ; ./run.sh -Gn{} | tee {}outputs/temp.out".format(
			env["repo"], itr, env["repo"])
		ret = remote_exec(nodes[itr][1], full_cmd)
		if ret != 0:
			kill_nodes(env["user"], nodes)
			exit(0)
	# start server locally
	os.chdir(env["repo"]+"tools")
	exec("./run.sh -Gn{} | tee {}outputs/temp-{}.out".format(
		curr_node, env["repo"], curr_node))

	# process results
	# copy temp from every non-failed node and rename it
	for itr in nodes:
		addr = nodes[itr][0]
		# copy config file
		exec("scp {}@{}:{}temp.out {}outputs/temp-{}.out".format(
			env["user"], addr, env["repo"], env["repo"], itr))
	# then execute process command for each one.
	for itr in nodes:
		job["NODE_ID"] = itr
		if job.get("MODE", "compile") != "release":
			continue
		# if not successfully parsing, write to log
		if not parse_output(env, job, "{}outputs/temp-{}.out".format(
				env["repo"], itr)):
			log_to_errors(job, "{}outputs/temp-{}.out".format(env["repo"],itr))


def test(env, nodes, curr_node, job):
	mode = job.get("MODE", "compile")
	if mode == "release" or mode == "debug":
		start_nodes(env, job, nodes, curr_node, compile_only=False)
	elif mode == "compile":
		start_nodes(env, job, nodes, curr_node, compile_only=True)


def test_exp(env, nodes, curr_node, job):
	# parse commands
	exp_name = job["CONFIG"].split('/')[-1].split('.')[0]
	num_nodes = job["NUM_NODES"]
	if isinstance(num_nodes, list):
		num_nodes = max(num_nodes)

	# generate config by iterating all the combinations of setups
	args = generate_args(job)

	# execute experiments
	for i, arg in enumerate(args):
		arg += " EXP_ID={}".format(i)
		print("[LOG] issue exp {}/{}".format(i+1, len(args)))
		print("[LOG] arg = {}".format(arg), flush=True)
		start_nodes(env, load_job(arg.split()), nodes, curr_node)
	print("[LOG] FINISH WHOLE EXPERIMENTS", flush=True)

	if job.get("MODE", "debug") != "release":
		exit(0)

	# process result on current node
	exec("cd {}outputs/; python3 collect_stats.py; mv stats.csv {}.csv; mv "
	  "stats.json {}.json".format(env["repo"], exp_name, exp_name))

	# process results on remote nodes
	print("[LOG] Start processing results on remote node", flush=True)
	for itr in nodes:
		# skip failed node
		if job.get("FAILURE_ENABLE", "false") == "true" and itr == job["FAILURE_NODE"]:
			continue
		# process result on each server if not a failure node
		remote_exec(nodes[itr][1], "cd {}outputs/; python3 collect_stats.py; "
			 "mv stats.csv {}.csv; mv stats.json {}.json".format(env["repo"],
																 exp_name,
																 exp_name))
	print("[LOG] FINISH processing results", flush=True)

	# collect results from remote nodes
	print("[LOG] Start collecting results on remote node", flush=True)
	suffix = ""
	if job.get("FAILURE_ENABLE", "false") == "true":
		suffix = " {}".format(job["FAILURE_NODE"])
	exec("cd {}tools; python3 remote_collect.py {} {}".format(env["repo"],
															  exp_name,
															num_nodes) + suffix)
	print("[LOG] FINISH collecting results")


if __name__ == "__main__":
	env = load_environment()
	# load job and start exp
	job = load_job(sys.argv[1:])
	# get current node
	curr_node = int(job.get("NODE_ID", "0"))
	# establish connections to all remote nodes
	nodes = load_ipaddr(curr_node, env)
	if "CONFIG" in job:
		test_exp(env, nodes, curr_node, job)
	else:
		test(env, nodes, curr_node, job)
