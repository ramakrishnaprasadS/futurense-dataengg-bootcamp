#!/bin/bash
a=$1
b=$2
chk=[ $a -eq $b ] 
echo "sum of $a and $b is `expr $a + $b`"
echo "product of $a and $b is `expr $a \* $b`"
echo "arg1 == arg2 is $chk"
