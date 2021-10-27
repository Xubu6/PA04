#!/bin/sh

# Bootstrapping steps. Here we create needed directories on the guest
mkdir -p ~/.ssh
mkdir -p ~/.ansible
mkdir -p ~/.config
mkdir -p ~/.config/openstack

apt-get update
apt-get install -y git
apt-get install -y default-jdk
apt-get install -y python3-pip

apt-get install -y software-properties-common
apt-add-repository ppa:ansible/ansible
apt-get install -y ansible

python3 -m pip install --ignore-installed --upgrade setuptools wheel
pip3 install --upgrade openstacksdk
ansible-galaxy collection install openstack.cloud