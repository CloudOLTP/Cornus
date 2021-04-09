import os, sys, re, os.path
import subprocess, datetime, time, signal, json


dbms_cfg = ["config-std.h", "config.h"]

def replace(filename, pattern, replacement):
    f = open(filename)
    s = f.read()
    f.close()
    s = re.sub(pattern,replacement,s)
    f = open(filename,'w')
    f.write(s)
    f.close()


def try_compile(job):
    os.system("cp {} {}".format(dbms_cfg[0], dbms_cfg[1]))
    # define workload
    for (param, value) in job.items():
        pattern = r"\#define\s*" + re.escape(param) + r'.*'
        if "ADDR" in param:
            replacement = "#define " + param + ' \"' + str(value) + '\"'
        else:
            replacement = "#define " + param + ' ' + str(value)
        replace(dbms_cfg[1], pattern, replacement)
    os.system("make clean > temp.out 2>&1")
    ret = os.system("make -j8 > temp.out 2>&1")
    if ret != 0:
        print("ERROR in compiling, output saved in temp.out")
        exit(0)
    else:
        os.system("rm -f temp.out")

def run(job=None):
    app_flags = ""
    if "NODE_ID" in job:
        app_flags += "-Gn{} ".format(job['NODE_ID'])
    else:
        app_flags += "-Gn0 "
    os.system("./rundb %s | tee temp.out" % app_flags)

def parse_output(job):
	output = open("temp.out")
	phase = 0
	for line in output:
		if phase == 0:
			if "=Worker Thread=" in line:
				phase = 1
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
	os.system("rm -f temp.out")
	return job

def parse_arg(arg):
    job = {}
    for item in arg:
        key = item.split("=")[0]
        value = item.split("=")[1]
        job[key] = value
    return job

def load_job(arg):
    job_tmp = parse_arg(arg)
    if "CONFIG" in job_tmp:
        print("loading config from {} ...".format(job_tmp["CONFIG"]))
        job = json.load(open(job_tmp["CONFIG"]))
        job.update(job_tmp)
    else:
        job = job_tmp
    return job

def collect_result(job):
    job = parse_output(job)
    stats = open("outputs/stats.json", 'a+')
    stats.write(json.dumps(job)+"\n")
    stats.close()

def eval_arg(arg, val, job, default=False):
    if arg not in job:
        return default
    elif job[arg] == val:
        return True
    else:
        return False

def main(arg):
    job = load_job(arg)
    if eval_arg("MODE", "release", job, default=True): 
        try_compile(job)
        run(job)
        collect_result(job)
    elif job["MODE"] == "debug":
        try_compile(job)
        run(job)
    elif job["MODE"] == "compile":
        try_compile(job)
    

if __name__ == "__main__":
    main(sys.argv[1:])
