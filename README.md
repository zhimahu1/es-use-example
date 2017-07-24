jpcloud zip包下载：http://jpcloud.jd.com/pages/viewpage.action?pageId=19932663

||======================================================================================||
||es-example大家有问题或者建议，或者好的example，欢迎发邮件到zhouzhichao@jd.com, jes@jd.com ||
||======================================================================================||


ElasticSearch使用java API例子，部分附HTTP API。
本项目中的API版本：2.1

参考文档：https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.1/java-api.html
          https://www.elastic.co/guide/en/elasticsearch/reference/2.1/getting-started.html

Environment:
Elasticsearch 2.1.2
JDK 1.8
Maven 3.1.1


                                    开始
==================================================================================
Tool是一个工具类，里面配置了一些连接ES，进行测试的参数，可以修改，但一般不用修改
CLUSTER_NAME = "jiesi-1";//每个集群都有一个固定的集群名，不是随便指定的，jiesi-1是测试环境的测试集群
INDEX_NAME = "agg_index";//索引，你可以指定自己的索引进行测试
TYPE_NAME = "agg_type";//类型，你可以指定自己的类型进行测试
IP = "192.168.200.190";//连接的es节点的ip
TcpPort = "9303";//连接的es节点的 tcp 端口
HttpPort = "9203";//连接的es节点的 http 端口

测试的索引是 agg_index,如果索引不存在或者数据被更改了。可以执行以下几个方法进行测试索引和测试文档的创建，以便运行例子
DeleteIndex.testDeleteIndex(Tool.INDEX_NAME);//删除索引
CreateIndex.createIndex(Tool.INDEX_NAME, Tool.TYPE_NAME);//创建索引，mapping
IndexDoc.prepareTestDoc();//准备测试数据
