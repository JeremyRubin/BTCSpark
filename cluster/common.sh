
#    Copyright 2015 Jeremy Rubin
#
#    This program is free software: you can redistribute it and/or modify it
#    under the terms of the Affero GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or (at your
#    option) any later version.
#
#    This program is distributed in the hope that it will be useful, but WITHOUT
#    ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
#    FITNESS FOR A PARTICULAR PURPOSE.  See the Affero GNU General Public
#    License for more details.
#
#    You should have received a copy of the Affero GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
export script_dir=/usr/local/Cellar/apache-spark/1.5.1/libexec/ec2
export AWS_SECRET_ACCESS_KEY=$(csvSelect -i spark-cred.csv -d , 2 | tail -n 1)
export AWS_ACCESS_KEY_ID=$(csvSelect -i spark-cred.csv -d , 1 | tail -n 1)
export pem=spark.pem
export cname=bitcoin-spark-cluster
export region=us-west-2
export zone=c
export kp=spark
export n_servers=5
export price=0.1
export ami=ami-f0091d91
export instance="m3.large"
export u=ec2-user
export dir=/home/ec2-user 
