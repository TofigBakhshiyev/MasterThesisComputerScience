#!/bin/bash


counter=1

while [ $counter -le 22 ]
do
    python3 main.py -p $1 -q $counter -f $2
    ((counter++))
done

echo All done

