#!/bin/bash

CONDA_HOME=~/anaconda3
#$CONDA_HOME/bin/conda deactivate
#$CONDA_HOME/bin/conda activate py37

############ Added by Huang Jianwei at 2018-04-03
# To solve the problem about Javascript runtime
export PATH=$PATH:/usr/local/bin
############ Ended

BASE_PATH=$(cd `dirname $0`; pwd)
echo $BASE_PATH
cd `dirname $0`
PROGRAM_NAME=./run_service.py

$CONDA_HOME/envs/py37/bin/python $PROGRAM_NAME >$BASE_PATH/log/service.log 2>>$BASE_PATH/log/service-error.log &
