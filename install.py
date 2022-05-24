import os, sys, re, os.path
import time
import threading


# TODO: change to dynamically calculate current node id
curr_node_id = 0

def load_environment(fname="info.txt"):
	env = {
		"home": os.path.expanduser('~')
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

def load_ipaddr(ifconfig):
	nodes = []
	f = open(ifconfig)
	itr = 0
	for addr in f:
		if addr[0] == '#':
			continue
		elif addr[0] == '=' and addr[1] == 'l':
			break
		nodes.append(addr.split(":")[0])
		itr += 1
	f.close()
	return nodes

class myThread (threading.Thread):

	def __init__(self, env, ipaddr, cmd, node_id, all_addrs):
		threading.Thread.__init__(self)
		self.usr = env["user"]
		self.ipaddr = ipaddr
		self.homedir = env["repo"]
		self.cmd = cmd
		self.node_id = node_id
		self.all_addrs = all_addrs
		self.root = env["home"]

	def exec(self, cmd):
		ret = os.system(cmd)
		if ret != 0:
			print("Error executing: {}".format(cmd))
			exit(0)
		else:
			print("Success executing: {}".format(cmd))

	def remote_exec(self, cmd):
		self.exec("ssh -l {} {} '{}' ".format(self.usr, self.ipaddr, cmd))

	def run(self):
		if self.cmd == "install_local":
			if self.node_id != curr_node_id:
				return
			self.exec("cd tools; ./setup_basic.sh; source setup_grpc.sh; "
					  "./setup_redis.sh; ")
		elif self.cmd == "install_remote":
			# remote command
			if self.node_id == curr_node_id:
				return
			self.exec("scp -r {} {}@{}:{};".format(self.homedir, self.usr,
												self.ipaddr, self.homedir))
			self.remote_exec("cd Sundial-Private; cd tools; "
							 "./setup_basic.sh; source setup_grpc.sh; "
							 "./setup_redis.sh; ")
		elif self.cmd == "config_local":
			if self.node_id != curr_node_id:
				return
			self.exec("sudo {}tools/setup_conf.sh {}src ; ".format(
				self.homedir, self.homedir))
			self.exec("{}tools/setup_proto.sh {}src/proto; ".format(
				self.homedir, self.homedir))
		elif self.cmd == "config_remote":
			if self.node_id == curr_node_id:
				return
			self.remote_exec("sudo {}tools/setup_conf.sh {}src ; ".format(
				self.homedir, self.homedir))
			self.remote_exec("{}tools/setup_proto.sh {}src/proto; ".format(
				self.homedir, self.homedir))
		elif self.cmd == "setkey_remote":
			if self.node_id == curr_node_id:
				return
			self.remote_exec("cd {}; sudo python3 install.py "
							 "setkey_local".format(self.homedir))
		elif self.cmd == "setkey_local":
			if self.node_id != curr_node_id:
				return
			for itr, addr in enumerate(self.all_addrs):
				if itr == self.node_id:
					continue
				# add ssh key to each node's authorized keys
				self.exec("sudo cat {}.ssh/id_ed25519.pub " \
							"| sudo ssh {} \"cat >> {}.ssh/authorized_keys\"".format(
					self.root, addr, self.root))
		elif self.cmd == "config_local":
			if self.node_id != curr_node_id:
				return
			self.exec("sudo ./tools/setup_conf.sh {}src ; ".format(
				self.homedir))
			self.exec("./tools/setup_proto.sh; ")
		elif self.cmd == "sync":
			if self.node_id == curr_node_id:
				return
			self.exec("rsync -av --exclude 'outputs' --delete {} {}@{}:{}".format(self.homedir, self.usr, self.ipaddr, self.homedir))
		elif self.cmd == "kill":
			if self.node_id == curr_node_id:
				return
			self.remote_exec("cd Sundial-Private; sudo pkill -f rundb")
		elif self.cmd == "clean_outputs":
			if self.node_id == curr_node_id:
				return
			self.remote_exec("cd Sundial-Private/outputs; rm stats.json")
		elif self.cmd == "clean_logs":
			if self.node_id == curr_node_id:
				return
			self.remote_exec("cd Sundial-Private; rm -f log_*")

if __name__ == "__main__":
	# parse commands
	cmd = sys.argv[1]
	env = load_environment()
	# set current node
	if input("current node id = 0, y/n? ") != "y":
		curr_node_id = int(input("enter a different id: "))
	# go through each node to complete the task
	threads = []
	addrs = load_ipaddr("src/ifconfig.txt")
	for itr, addr in enumerate(addrs):
		thread1 = myThread(env, addr, cmd, itr, addrs)
		thread1.start()
		threads.append(thread1)
		time.sleep(0.5)
	for t in threads:
		t.join()