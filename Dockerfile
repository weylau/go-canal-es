#源镜像
FROM golang:1.13

#设置工作目录
WORKDIR $GOPATH/src

#拉取项目代码
RUN git clone https://github.com/weylau/go-canal-es.git
#切换工作目录
WORKDIR $GOPATH/src/go-canal-es

#设置代理
ENV GOPROXY https://goproxy.io

#go构建可执行文件
RUN go build -o go-canal-es .


#最终运行docker的命令
ENTRYPOINT  ["nohup","./go-canal-es","&"]