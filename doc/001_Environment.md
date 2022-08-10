# 配置环境

## Ubuntu 20.04

### 安装配置docker

更新软件包索引，并且安装必要的依赖软件，来添加一个新的 HTTPS 软件源：

```bash
sudo apt update
sudo apt install apt-transport-https ca-certificates curl gnupg-agent software-properties-common
```

使用下面的 curl 导入源仓库的 GPG key：

```bash
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
```

将 Docker APT 软件源添加到你的系统：

```bash
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
```

将 Docker APT 软件源添加到你的系统：

```bash
sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io
```

运行docker：

```bash
sudo service docker start
```

添加docker源：

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

### 复制docker容器

``` bash
docker build . -t bustub
docker create -t -i --name bustub -v $(pwd):/bustub bustub bash
```

docker和本地目录的挂载来实现在本地修改在docker中自动同步:

```bash
docker container run -it -v /mnt/d/src/Bustub:/bustub --name=bustub_env bustub /bin/bash
```

下次运行该容器：

```bash
docker exec -it 容器id /bin/bash
```
