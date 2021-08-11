# py-simulator
Trade simulator for BIST in Python (>=3.9)
To install the necessary dependencies on Ubuntu (16:04 and 18:04) please run the following script
which is also located in the repo at folder `bin` as `configure_deb.sh`:
```
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
```