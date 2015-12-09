#!/bin/bash
source common.sh
$script_dir/spark-ec2 --key-pair=$kp --identity-file=$pem -s $n_servers --region=$region\
            --zone=$region$zone  --spark-version=1.5.1 --copy-aws-credentials\
            --spot-price $price\
            --ami $ami\
			--resume\
            --instance-type $instance\
            launch $cname
