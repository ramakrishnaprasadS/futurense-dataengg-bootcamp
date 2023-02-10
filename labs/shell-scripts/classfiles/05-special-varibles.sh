#!/bin/bash
echo " File name is $0"
echo " First Arg is $1"
echo "Second Arg is $2"
echo "$# are the number of args" 
echo "passed args are $*"
msg1=$1
msg2=$3
echo $msg1
echo $@
echo $1
