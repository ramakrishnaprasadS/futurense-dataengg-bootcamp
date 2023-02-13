#!/bin/bash

echo "Please enter 2 numbers"
read num1
read num2
echo " please enter any of the option\n a  for addition \n s for subtraction \n m for multiplication \n d for division \n "
read ip
if [ $ip != 'a' ]
	then echo `expr $num1 + $ num2`
fi
