#! /bin/bash

# Packages
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  sudo apt-get update
  sudo apt-get install python3-pip virtualenv git
fi

# venv
virtualenv venv --python=python3
source ./venv/bin/activate
pip3 install -r requirements.txt

echo "alias curvenv=./venv/bin/activate" >> ~/.bashrc