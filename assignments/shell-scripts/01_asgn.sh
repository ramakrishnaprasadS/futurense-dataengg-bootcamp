#!/bin/sh
mkdir assignments
cd assignments
touch cities.txt
echo "Enter 4 city names"
read city1
read city2
read city3
read city4
echo "$city1\n$city2\n$city3\n$city4" > cities.txt
cat cities.txt | grep -i new |sed  's/new/old/gi' > old-cities.txt
echo "task done pls check old-cities.txt"
