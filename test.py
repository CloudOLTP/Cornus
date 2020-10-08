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

def run(test = '', job=None):
    app_flags = ""
    if "NODE_ID" in job:
        app_flags += "-Gn{} ".format(job['NODE_ID'])
    os.system("./rundb %s" % app_flags)


def compile_and_run(job) :
    try_compile(job)
    run('', job)

def parse_output(job):
    output = open("temp.out")
    for line in output:
        line = line.strip()
        if "[summary]" in line:
            for token in line.strip().split('[summary]')[-1].split(','):
                key, val = token.strip().split('=')
                job[key] = val
            break
    output.close()
    return job

def parse_arg(arg):
    job = {}
    for item in arg:
        key = item.split("=")[0]
        value = item.split("=")[1]
        job[key] = value
    return job

if __name__ == "__main__":
    job = parse_arg(sys.argv[1:])
    print(json.dumps(job)+"\n")
    compile_and_run(job)

