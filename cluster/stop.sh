#!/bin/bash
source common.sh
$script_dir/spark-ec2 -k $kp -i $pem -r $region stop $cname

