package com.jd.es.example.common;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO 写一个根据version来更新文档的demo
 * Title: Version 对一个文档进行覆盖，修改，删除，它的版本值都会增加。好好体会一下。
 *                新文档的版本从1开始递增。如果删除一个文档后，再重新插入相同id的文档，那么它的版本并不是从1开始。注意。看下面的例子。
 * Description: Version
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2017/2/24
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class Version {

    private static Logger logger = LoggerFactory.getLogger(Version.class);

    private static String IndexName = "version_test";
    private static String TypeName = "type_name";

    public static void main(String[] args){

        try{
            //1.创建索引 mapping
            /**
            {
                "mappings": {
                "type_name": {
                    "properties": {
                        "age": {
                            "type": "integer"
                        }
                    }
                }
            }
            }
             */
            CreateIndex.createIndexWithMapping(IndexName,"{\"mappings\":{\"type_name\":{\"properties\":{\"age\":{\"type\":\"integer\"}}}}}");

            //2.插入一个id为1的新文档
            IndexDoc.indexWithStr(IndexName, TypeName,"1","{\"age\":1}");
            GetResponse response = Tool.CLIENT.prepareGet(IndexName,TypeName,"1").get();
            logger.info("插入新文档的版本：" + response.getVersion());

            //3.更新这个文档
            Tool.CLIENT.prepareUpdate(IndexName,TypeName,"1").setDoc("{\"age\":2}").get();
            response = Tool.CLIENT.prepareGet(IndexName,TypeName,"1").get();
            logger.info("修改文档后的版本：" + response.getVersion());

            //4.覆盖这个id为1的文档
            IndexDoc.indexWithStr(IndexName, TypeName,"1","{\"age\":3}");
            logger.info("覆盖文档后的版本：" + currentVersion(IndexName,TypeName,"1"));

            //5.删除文档
            DeleteResponse dResponse = Tool.CLIENT.prepareDelete(IndexName,TypeName,"1").get();
            logger.info("删除文档，版本值也会增加。返回版本：" + dResponse.getVersion());

            //6.重复删除文档
            dResponse = Tool.CLIENT.prepareDelete(IndexName,TypeName,"1").get();
            logger.info("重复删除文档，版本值仍然增加。返回版本：" + dResponse.getVersion());

            //7.重复删除文档
            dResponse = Tool.CLIENT.prepareDelete(IndexName,TypeName,"1").get();
            logger.info("重复删除文档，版本值仍然增加。返回版本：" + dResponse.getVersion());

            logger.info("文档不存在，版本：" + currentVersion(IndexName,TypeName,"1"));

            //7.再次插入id为1的文档
            IndexDoc.indexWithStr(IndexName, TypeName,"1","{\"age\":3}");
            logger.info("删除文档后,重新插入文档，版本并不是从1开始。之前对该文档进行的插入，覆盖，修改，删除的版本都有记录。现在的版本：" + currentVersion(IndexName,TypeName,"1"));

            logger.info("不存在的文档的版本：" + currentVersion(IndexName,TypeName,"100"));//并没有id为100的文档

            //删除索引
            DeleteIndex.testDeleteIndex(IndexName);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 如果文档不存在，版本号返回-1
     * @param indexName
     * @param typeName
     * @param docId
     * @return
     */
    //获取某个文档当前的版本号
    private static long currentVersion(String indexName,String typeName,String docId){
            return Tool.CLIENT.prepareGet(indexName,typeName,docId).get().getVersion();
    }

}
