#!/bin/bash

PACKAGE_KEY=$1

cd /home/ssm-user
echo $PACKAGE_KEY >> "`date +%s`.txt"
