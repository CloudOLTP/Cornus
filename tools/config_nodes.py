import os, sys, re, os.path
import platform
import subprocess, datetime, time, signal, json
import time
import _thread
import threading

cmds = {
    "node_setup": "git clone https://github.com/ScarletGuo/Sundial.git; cd Sundial; git checkout grpc-1pc-redis; sudo ./setup_grpc.sh;",
    "server_setup": "sudo ./tools/conf.sh; sudo ./run_proto.sh",
    "pull": "cd Sundial; git pull",
    "kill": "cd Sundial; sudo pkill -f rundb",
    "clean_output": "cd Sundial/outputs; rm stats.json",
    "clean_log": "cd Sundial; rm -f log_*"
}

class myThread (threading.Thread):
	def __init__(self, usr, line, setup):
		threading.Thread.__init__(self)
        self.usr = usr
		self.line = line
		self.setup = setup

	def run(self):
		ret = os.system("ssh -l {} {} '{}' ".format(self.usr, self.line, self.setup))
		while ret != 0:
			time.sleep(1)
			ret = os.system("ssh -l {} {} '{}' ".format(self.usr, self.line, self.setup))

if __name__ == "__main__":
	ifconfig = open("ifconfig.txt")
	threads = []
	for line in ifconfig:
		if line[0] == '#':
			continue
		if line[0] == '=' and line[1] == 'l':
            break
		else:
			line = line.split(':')[0]
			line = line.strip()
			thread1 = myThread(usr, line, cmds[sys.argv[1]])
			thread1.start()
			threads.append(thread1)
			time.sleep(0.5)
	for t in threads:
		t.join()
	ifconfig.close()
