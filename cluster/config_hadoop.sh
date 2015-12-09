#!/bin/bash
PAT="s,/mnt/ephemeral-hdfs,/media/ephemeral0,g"
sed -i $PAT /root/ephemeral-hdfs/conf/*
sed -i "s,<value>3</value>,<value>1</value>,g" /root/ephemeral-hdfs/conf/hdfs-site.xml
yes Y|/root/ephemeral-hdfs/bin/hadoop namenode -format
exit
