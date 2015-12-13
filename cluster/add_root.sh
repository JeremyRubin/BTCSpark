#!/bin/bash
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
sudo sed -i s/PermitRootLogin/\#PermitRootLogin/g /etc/ssh/sshd_config
sudo sed -i s/\#\#PermitRootLogin/PermitRootLogin/g /etc/ssh/sshd_config
sudo perl -i -pe 's/.*(ssh-rsa .*)/\1/' /root/.ssh/authorized_keys
sudo /etc/init.d/sshd restart
yes | sudo yum install git
exit
