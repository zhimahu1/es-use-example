package com.jd.es.example.common;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Title: UpdateDoc 更新文档
 * Description: UpdateDoc  https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.1/java-docs-update.html
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2017/2/24
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class UpdateDoc {

    private static Logger logger = LoggerFactory.getLogger(UpdateDoc.class);

    private static String IndexName = "update_doc";
    private static String TypeName = "type_name";

    public static void main(String[] args){

        try{
            //1.创建索引 mapping
            /**
            {
                "mappings": {
                "type_name": {
                    "properties": {
                        "name": {
                            "type": "string"
                        },
                        "age": {
                            "type": "integer"
                        }
                    }
                }
            }
            }
             */
            CreateIndex.createIndexWithMapping(IndexName,"{\"mappings\":{\"type_name\":{\"properties\":{\"name\":{\"type\":\"string\"},\"age\":{\"type\":\"integer\"}}}}}");

            //2.插入一个id为1的文档
            IndexDoc.indexWithStr(IndexName, TypeName,"1","{\"name\":\"xiaoming\",\"age\":10}");
            GetResponse getResponse = Tool.CLIENT.prepareGet(IndexName,TypeName,"1").get();
            logger.info("返回内容：" + getResponse.getSourceAsString());

            //3.更新这个文档 age改为18
            Tool.CLIENT.prepareUpdate(IndexName,TypeName,"1").setDoc("{\"age\":18}").get();
            getResponse = Tool.CLIENT.prepareGet(IndexName,TypeName,"1").get();
            logger.info("修改后：" + getResponse.getSourceAsString());

            //4.更新这个文档
            Tool.CLIENT.prepareUpdate(IndexName,TypeName,"1").setDoc("{\"name\":\"hanmeimei\",\"age\":21}").get();
            getResponse = Tool.CLIENT.prepareGet(IndexName,TypeName,"1").get();
            logger.info("修改后：" + getResponse.getSourceAsString());

            //5.批量更新这个文档
            BulkRequestBuilder bulkRequestBuilder = Tool.CLIENT.prepareBulk();
            bulkRequestBuilder.add(new UpdateRequest(IndexName,TypeName,"1").doc("{\"name\":\"hanmeimei1\",\"age\":20}"));
            bulkRequestBuilder.add(new UpdateRequest(IndexName,TypeName,"1").doc("{\"name\":\"hanmeimei2\",\"age\":30}"));
            bulkRequestBuilder.add(new UpdateRequest(IndexName,TypeName,"1").doc("{\"name\":\"hanmeimei3\",\"age\":40}"));
            BulkResponse bulkResponse = bulkRequestBuilder.get();
            BulkItemResponse[] bulkItemResponseArr = bulkResponse.getItems();
            for (BulkItemResponse bulkItemResponse :bulkItemResponseArr) {
                logger.info("操作是否失败:"+ bulkItemResponse.isFailed() + " 文档版本：" + bulkItemResponse.getVersion());
            }
            logger.info("文档内容："+Tool.CLIENT.prepareGet(IndexName,TypeName,"1").get().getSourceAsString());

        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
