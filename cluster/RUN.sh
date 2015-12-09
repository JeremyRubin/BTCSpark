#!/bin/bash
/root/spark/bin/spark-submit --master $(cat /root/spark-ec2/cluster-url) --py-files /root/src/Bitcoin/dist/BTCLib-0.1-py2.7.egg --num-executors 5  /root/src/App/app.py 
