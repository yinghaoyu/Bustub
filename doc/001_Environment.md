# Environment Setup

## Ubuntu 20.04

### Install and deploy docker

Update the package and install the necessary dependencies to add a new HTTPS repository:

```bash
sudo apt update
sudo apt install apt-transport-https ca-certificates curl gnupg-agent software-properties-common
```

Import the GPG key of the source repository:

```bash
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
```

Add the Docker APT repositories to your machine:

```bash
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
```

Install Docker APT to your machine：

```bash
sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io
```

Run docker:

```bash
sudo service docker start
```

Add docker source:

`sudo vim /ext/docker/daemon.json`

```json
{
  "experimental": false,
  "registry-mirrors": [
    "https://docker.mirrors.ustc.edu.cn"
  ],
  "features": {
    "buildkit": true
  }
}
```

```bash
sudo docker info
```

### Copy container

``` bash
docker build . -t bustub
docker create -t -i --name bustub -v $(pwd):/bustub bustub bash
```

Synchronize when modify in docker:

```bash
docker container run -it -v /mnt/d/src/Bustub:/bustub --name=bustub_env bustub /bin/bash
```

Run the container next time:

```bash
docker exec -it containerId /bin/bash
```
