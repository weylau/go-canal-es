# go-canal-es
> go-canal-es是用于处理和消费从canal中同步解析mysql的binlog文件后写入到kafka中的消息，消费kafka中json,并解析成es-bulk数据格式批量更新数据到es中的中间件


# 配置文件说明

```$xslt
# es连接地址
es_addr = "172.16.57.112:9200"

# 日志级别
log_level = "debug"

# debug 为true时，日志直接输出在控制台，false时写入文件
debug = true

# kafka配置
kafka_version = "1.1.0"
consumer_brocks = "172.16.57.110:9095"
consumer_group = "example_group"
consumer_topics = "example"

[[rule]]
# mysql数据库
database = "db_user"
# mysql数据库
table = "user"
# es对应的index
index = "db_user"
# es对应的type
type = "user"
# mysql表主键
id = "id"

[rule.field]
id = "id"
username = "username"
password = "password"
realname = "realname"
mobile = "mobile"
email = "email"
intro = "intro"
```