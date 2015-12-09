script_dir=/usr/local/Cellar/apache-spark/1.5.1/libexec/ec2
AWS_SECRET_ACCESS_KEY=$(csvSelect -i ~/Downloads/spark-cred.csv -d , 2 | tail -n 1)
AWS_ACCESS_KEY_ID=$(csvSelect -i ~/Downloads/spark-cred.csv -d , 1 | tail -n 1)
./spark-ec2 --key-pair=spark --identity-file=$HOME/Downloads/spark.pem -s 5 --region=us-west-2 --zone=us-west-2c  --spark-version=1.5.1 launch 6.s897-spark-clusterCD
