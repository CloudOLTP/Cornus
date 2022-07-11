#!/bin/bash
source $HOME/Sundial-Private/tools/setup_env.sh
cd $HOME/Sundial-Private/cornus_storage || exit
make clean
make -j

