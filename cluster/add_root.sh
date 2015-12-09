#!/bin/bash
sudo sed -i s/PermitRootLogin/\#PermitRootLogin/g /etc/ssh/sshd_config
sudo sed -i s/\#\#PermitRootLogin/PermitRootLogin/g /etc/ssh/sshd_config
sudo perl -i -pe 's/.*(ssh-rsa .*)/\1/' /root/.ssh/authorized_keys
sudo /etc/init.d/sshd restart
yes | sudo yum install git
exit
