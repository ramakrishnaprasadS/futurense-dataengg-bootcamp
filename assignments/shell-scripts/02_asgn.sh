#!/bin/sh
mkdir assignments
cd assignments
touch cities.txt
echo "How many cities you want ot enter?"
read n
echo "enter $n cities below"
while [ $n -gt 0 ]
do
   read cityname
   echo "$cityname\n" >> cities.txt
   n=`expr $n - 1`
done
cat cities.txt | grep -i new |sed  's/new/old/gi' > old-cities.txt
echo "task done pls check old-cities.txt"
