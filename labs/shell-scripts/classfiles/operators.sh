#!/bin/bash
a=$1
b=$2
echo "sum of $a and $b is `expr $a + $b`"
echo "product of $a and $b is `expr $a \* $b`"
echo "arg1 == arg2 is `[$a == $b]`"
