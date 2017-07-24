package com.jd.es.example.common;

import com.jd.es.example.deleteByQuery.DeleteByQuery;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;

import java.io.IOException;


/**
 * Title: Bulk 批量操作
 * Description: Bulk
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/8/10
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class Bulk {

    private static Logger logger = Logger.getLogger(Bulk.class);

    public static void main(String[] agrs){

        //注意，每一行都需要\n，不要忘记了最后的\n
        String System_line_separator = System.getProperty("line.separator","\n");
        String request = "{\"index\":{}}" + System_line_separator +
                "{\"pid\":1,\"clusterName\":\"jiesi-3\"}"  + System_line_separator +
                "{\"index\":{}}"  + System_line_separator +
                "{\"pid\":2,\"clusterName\":\"jiesi-3\"}"  + System_line_separator +
                "{\"index\":{}}"  + System_line_separator +
                "{\"pid\":3,\"clusterName\":\"jiesi-3\",\"nodeName\":\"192.168.200.189\"}" + System_line_separator ;//不要忘了这个

        try {
            //DeleteByQuery.deleteAll("agg_index","agg_type");
            String response = SimpleHttpClient.post("http://192.168.200.189:9203/agg_index/agg_type/_bulk",request);
            System.out.println(response);
            System.out.println(SimpleHttpClient.get("http://192.168.200.189:9203/agg_index/_search","GET"));

        } catch (Exception e) {
            logger.error(e.getMessage(),e);
        }


        //bulkDeleteDoc(Tool.INDEX_NAME,Tool.TYPE_NAME,new String[]{"9","10"});//批量删除两个文档

        //bulkIndexDoc(Tool.INDEX_NAME,Tool.TYPE_NAME,new String[]{"9","10"});//批量添加或者更新两个文档
    }

    //批量删除文档
    public static void bulkDeleteDoc(String indexName, String typeName, String[] docIdArr){

        BulkRequestBuilder bulkRequestBuilder = Tool.CLIENT.prepareBulk();

        for (String docId : docIdArr) {
            bulkRequestBuilder.add(new DeleteRequest(indexName,typeName, docId));
        }
        BulkResponse responses = bulkRequestBuilder.get();
        logger.info(responses);
    }

    //批量添加文档,update文档
    public static void bulkIndexDoc(String indexName, String typeName, String[] docIdArr){
        BulkRequestBuilder bulkRequestBuilder = Tool.CLIENT.prepareBulk();

        for (String docId : docIdArr) {
            String json = "{" +
                    "\"name\":\"a\"" +
                    "}";
            bulkRequestBuilder.add(new IndexRequest(indexName,typeName, docId).source(json));
        }
        BulkResponse responses = bulkRequestBuilder.get();
        logger.info(responses);
    }


    //https://www.elastic.co/guide/en/elasticsearch/reference/2.1/docs-bulk.html#bulk-refresh
    public static void httpBulkRequest(){

    }

}
