# run all nodes in ifconfig.
# example usage:
# python3 test_distrib.py CONFIG=experiments/[name].json NODE_ID=0 [optional args]
# where NODE_ID specifies the index of current server matched in ifconfig.txt
# and the rest can be configurations you wanna overwrite in config.h
import os, sys, re, os.path

ifconfig = "ifconfig.txt"


def start_nodes(curr_node):
    os.system("sudo pkill run_test_network")
    os.system("export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH")
    os.system("make clean; make -j16;")
    f = open(ifconfig)
    num_nodes = 0
    for addr in f:
        if addr[0] == '#':
            continue
        elif addr[0] == '=' and addr[1] == 'l':
            continue
        elif num_nodes == 4:
            break
        cmd = "./test.sh -Gn{}".format(num_nodes)
        if curr_node != num_nodes:
            # start server remotely
            addr = addr.split(':')[0]
            os.system("ssh {} 'sudo pkill run_test_network'".format(addr))
            ret = os.system("ssh {} 'cd ~/Sundial/test_network/ ; export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH ; sudo {}' & ".format(addr, cmd))
            if ret != 0:
                err_msg = "error executing server"
                print("ERROR: " + err_msg)
            else:
                print("[LOG] start node {}".format(num_nodes))
        num_nodes += 1
    # start server locally
    cmd = "./test.sh -Gn{}".format(curr_node)
    ret = os.system(cmd)
    f.close()
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
    start_nodes(0)
        
    
