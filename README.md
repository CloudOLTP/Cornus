Sundial
=======

Sundial is a distributed OLTP database management system (DBMS). It supports a number of traditional/modern distributed concurrency control protocols, including WAIT_DIE, NO_WAIT, F1 (Google), MaaT, and TicToc. Sundial is implemented on top of [DBx1000](https://github.com/yxymit/DBx1000). 

The following two papers describe Sundial and DBx1000, respectively: 

[Sundial Paper](http://xiangyaoyu.net/pubs/sundial.pdf)  
Xiangyao Yu, Yu Xia, Andrew Pavlo, Daniel Sanchez, Larry Rudolph, Srinivas Devadas  
Sundial: Harmonizing Concurrency Control and Caching in a Distributed OLTP Database Management System  
VLDB 2018
    
[DBx1000 Paper](http://www.vldb.org/pvldb/vol8/p209-yu.pdf)  
Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker  
Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores  
VLDB 2014

Install dependency
------------------
- install locally: ```python3 install.py install_local```
- copy current directory to other compute nodes and install remotely ```python3 
  install.py install_remote```

Setup SSH Key Authorization
---------------------------
- assume there's a public key at <root>/.ssh/id_ed25519.pub
  - generate ssh key [link](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)
- make sure each node can access the other node through sudo and you have 
  already tried so that there will not have pop-up question 
  asking about whether to add the ip address
- setup locally: ```python3 install.py setkey_local```
- copy current directory to other compute nodes and install remotely ```python3
  install.py setkey_remote```

Setup Storage Service
----------------------

Edit ifconfig.txt on master ***compute node***, after "=l", use the ip of the (master) storage node (if you use multiple replicas)

### Customized Redis Setup

Install redis
```
git clone https://github.com/redis/redis.git
cd redis
make
cd
mkdir redis_data/
```

Open ```redis.conf``` to setup of Redis:

on Master
- change bind to bind 0.0.0.0 -::1 OR comment out bind to use default
- set up ```protected-mode``` (e.g. password), you can use ```protected-mode no``` but it is dangerous for public cloud
- set ```appendonly yes```
- set ```fsync always```
- set ```dir <your desired path for log>```
- set ```requirepass sundial-dev```
    
on Replica
- set ```replicaof <ip of the master> <port>```

On Master, start the service
```
cd src/
./redis-server ../redis.conf
```

Setup Compute Node
-------------------

Edit ifconfig.txt, before "=l", one line corresponds to one compute instance.

To use automatic test script, please make sure:
- compute nodes have ssh access to each other (i.e. added to each other's authorized keys)

To set up library path and proto on all compute nodes:
```python3 install.py config_local```
```python3 install.py config_remote```

Configuration & Execution
--------------------------

### Automatic

Notes on MODE: 
- if MODE=compile, it will just compile on all nodes without execution
- if MODE=debug, it will compile and run, but not write output file
- if MODE=release, it will write output file stats.json to outputs/ after execution

#### Single Local Test
template for starting current compute node
```
python3 test.py CONFIG=experiments/<name of config file>.json NODE_ID=<current node id> <optional args overwriting config.h>
```

example usage:
```
python3 test.py CONFIG=experiments/ycsb_debug.json NODE_ID=0 FAILURE_ENABLE=false MODE=compile
```

#### Single Distributed Test

template for starting all compute nodes:
```
python3 test_distrib.py CONFIG=experiments/<name of config file>.json NODE_ID=<current node id> <optional args overwriting config.h>
```

example usage:
```
python3 test_distrib.py CONFIG=experiments/ycsb_debug.json NODE_ID=0 FAILURE_ENABLE=false MODE=debug
```

#### Batch Tests for Experiments

example usage:
```
python3 test_exp.py CONFIG=experiments/ycsb_zipf.json NODE_ID=0 FAILURE_ENABLE=false MODE=debug
```
this script allows for list type for each config in .json, and it will enumerate all the possible combinations of the config. 
i.e. the number of tests in total will be the product of the length of every list. 

collect results from all nodes:
go to tools/collect_result_remote.py and change the user to your cloudlab user name
```
cd tools/
python3 collect_result_remote.py <exp_name> # exp_name will be name of your json file, in this case it's ycsb_zipf
```

### Manual
For each compute node, do the follow:
```
make -j16
./rundb -Gn<node_id> 
```

Output 
------

The output file is stored in ```outputs/```. The output data should be mostly self-explanatory. More details can be found in system/stats.cpp.
