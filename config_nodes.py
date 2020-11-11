import os, sys, re, os.path
import platform
import subprocess, datetime, time, signal, json
import time
import _thread
import threading

server_setup = "git clone https://github.com/ScarletGuo/Sundial.git; cd Sundial; git checkout grpc-1110; sudo ./setup_grpc.sh; sudo ./conf.sh; sudo ./run_proto.sh"
# storage_setup = "git clone https://github.com/ScarletGuo/Sundial.git; cd Sundial; git checkout 1pc-log; cp ./storage_setup.sh ../; cd ..; sudo ./storage_setup.sh"
cmd = "cd Sundial; sudo ./run_proto.sh"
sync = "cd Sundial; sudo ./pull.sh"
kill = "cd Sundial; sudo pkill -f rundb"
clean_output = "cd Sundial/output; rm stats.json"

class myThread (threading.Thread):
	def __init__(self, line, setup):
		threading.Thread.__init__(self)
		self.line = line
		self.setup = setup

	def run(self):
		ret = os.system("ssh -l LockeZ {} '{}' ".format(self.line, self.setup))
		while ret != 0:
			time.sleep(1)
			ret = os.system("ssh -l LockeZ {} '{}' ".format(self.line, self.setup))

if __name__ == "__main__":
	cmds = {}
	cmds['server_setup'] = server_setup
	cmds['cmd'] = cmd
	cmds['sync'] = sync
	cmds['kill'] = kill
	cmds['clean_output'] = clean_output
	ifconfig = open("ifconfig.txt")
	node_type = -1
	threads = []
	for line in ifconfig:
		if line[0] == '#':
			continue
		if line[0] == '=':
			if line[1] == 's':
				node_type = 1
			if line[1] == 'l':
				node_type = 2
		else:
			line = line.split(':')[0]
			line = line.strip('\n')
			# if node_type == 1:
			# 	ret = os.system("ssh -l LockeZ {} '{}' &".format(line, server_setup))
			# 	if ret != 0:
			# 		err_msg = "error setup server"
			# 		print("ERROR: " + err_msg)
			# if node_type == 2:
			# 	ret = os.system("ssh -l LockeZ {} '{}' &".format(line, storage_setup))
			# 	if ret != 0:
			# 		err_msg = "error setup storage"
			# 		print("ERROR: " + err_msg)
			thread1 = myThread(line, cmds[sys.argv[1]])
			# if node_type == 2:
			# 	thread1 = myThread(line, storage_setup)
			thread1.start()
			threads.append(thread1)
			time.sleep(1)
	for t in threads:
		t.join()
	ifconfig.close()