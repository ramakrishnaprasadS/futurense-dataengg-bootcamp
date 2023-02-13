#!/bin/bash

ls -R

rm *.txt
if [ ls -l ]
	then echo "NO FILE PRESENT WITH THE .TXT EXTENSION"
else
       ls >> OtherData.tar


