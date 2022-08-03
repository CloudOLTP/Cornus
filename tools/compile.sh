#!/bin/bash
source $HOME/Sundial-Private/tools/setup_env.sh
mkdir -p $HOME/Sundial-Private/outputs/
cd $HOME/Sundial-Private/src
make -j16 $1

