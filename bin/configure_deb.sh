#!/bin/bash
# 1. Python3.9
add-apt-repository ppa:deadsnakes/ppa
apt install software-properties-common \
            git \
            python3.9 \
            pipenv

# 2. git-lfs
curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash
apt install git-lfs

# 3. Install pipenv from Pipfile
pipenv install
