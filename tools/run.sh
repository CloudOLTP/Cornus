#!/bin/bash
source setup_env.sh
cd $HOME/Sundial-Private/src
mkdir -p ${HOME}/Sundial-Private/outputs
./rundb $1
