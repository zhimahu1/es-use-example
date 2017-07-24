package com.jd.es.example.deleteByQuery;

import com.jd.es.example.common.SimpleHttpClient;
import com.jd.es.example.common.IndexDoc;
import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Title: DeleteByQuery  delete-by-query插件使用
 * Description: DeleteByQuery
 * 参考文档：https://www.elastic.co/guide/en/elasticsearch/plugins/2.1/plugins-delete-by-query.html
 * <p>
 * delete-by-query插件支持删除所有满足特定查询条件的文档（一个或者多个索引）。
 * 老版本的Elasticsearch提供的delete-by-query功能因为会有问题，已经从
 * Elasticsearch核心代码中移除掉了，delete-by-query插件是它的替代品。
 * <p>
 * 提示：
 * 由于每个文档都需要单独被删除，查询大量文档可能需要很长的时间。
 * 不要使用delete-by-query来删除一个索引下的全部或者大部分文档,
 * 确实需要的话，可以创建一个新的索引，然后将需要保留的文档重新
 * 索引到新的索引中去，这样你就可以直接删掉旧索引。
 * <p>
 * 重要：
 * delete-by-query只会删除请求执行时查询可见的文档版本（delete-by-query
 * 内部实现使用Scroll查询和Bulk APIs）。在请求执行期间被重新索引或者更新
 * 的文档不会被删除。
 *
 * TODO  注意：要保证client为单例，不要构造多个client
 *
 * <p>
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/8/5
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class DeleteByQuery {

    private static Logger logger = Logger.getLogger(DeleteByQuery.class);

    private static Client DeleteByQuery_Client;

    static {
        try{
            //设置集群的名字
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", "jiesi-1")
                    .put("client.transport.sniff", false)
                    .build();

            DeleteByQuery_Client = TransportClient.builder().settings(settings)
                    .addPlugin(DeleteByQueryPlugin.class)//添加插件
                    .build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.200.190"),9303));
            //.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("ip"),port));//可以连接多个地址

        }catch(Exception e){
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        try {

            createDoc("delete_by_query_test", "myType");//创建6个文档
            Thread.sleep(1000);
            logger.info("文档数：" + queryAll("delete_by_query_test", "myType"));

            testDeleteByQuery("delete_by_query_test", "myType");//消灭菲律宾,删除两个文档
            Thread.sleep(1000);
            logger.info("文档数：" + queryAll("delete_by_query_test", "myType"));

            testDeleteByQueryHttpApi("delete_by_query_test", "myType");//消灭日本，删除两个文档
            Thread.sleep(1000);
            logger.info("文档数：" + queryAll("delete_by_query_test", "myType"));

            deleteAll("delete_by_query_test", "myType");//删除所有文档
            Thread.sleep(1000);
            logger.info("文档数：" + queryAll("delete_by_query_test", "myType"));

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    //消灭菲律宾
    public static void testDeleteByQuery(String indexName, String typeName) {
        try {
            QueryBuilder qb = QueryBuilders.matchQuery("country", "Philippines");//查询菲律宾
            DeleteByQueryResponse deleteByQueryResponse = new DeleteByQueryRequestBuilder(DeleteByQuery_Client, DeleteByQueryAction.INSTANCE)
                    .setIndices(indexName)
                    .setTypes(typeName)
                    .setQuery(qb)
                    .get();

            logger.info(deleteByQueryResponse);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * URL:  delete_by_query_test/_search
     * TYPE: DELETE
     * BODY:
     * {
     * "query":{
     * "match":{
     * "country":"Japan"
     * }
     * }
     * }
     */
    public static void testDeleteByQueryHttpApi(String indexName, String typeName) {
        try {
            //消灭日本
            String queryBody = "{\n" +
                    "  \"query\":{\n" +
                    "    \"match\":{\n" +
                    "      \"country\":\"Japan\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";
            String url = Tool.URL + "/" + indexName + "/" + typeName + "/_query";
            String res = SimpleHttpClient.delete(url, queryBody);
            logger.info(res);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    //初始化数据
    //2个中国人，2个日本人，2个菲律宾人
    private static void createDoc(String indexName, String typeName) {

        String json1 = "{" +
                "\"name\":\"c1\"," +
                "\"country\":\"China\"" +
                "}";

        String json2 = "{" +
                "\"name\":\"c2\"," +
                "\"country\":\"China\"" +
                "}";

        String json3 = "{" +
                "\"name\":\"j3\"," +
                "\"country\":\"Japan\"" +
                "}";

        String json4 = "{" +
                "\"name\":\"j4\"," +
                "\"country\":\"Japan\"" +
                "}";

        String json5 = "{" +
                "\"name\":\"p5\"," +
                "\"country\":\"Philippines\"" +
                "}";

        String json6 = "{" +
                "\"name\":\"p6\"," +
                "\"country\":\"Philippines\"" +
                "}";

        IndexDoc.indexWithStr(indexName, typeName, "1", json1);
        IndexDoc.indexWithStr(indexName, typeName, "2", json2);
        IndexDoc.indexWithStr(indexName, typeName, "3", json3);
        IndexDoc.indexWithStr(indexName, typeName, "4", json4);
        IndexDoc.indexWithStr(indexName, typeName, "5", json5);
        IndexDoc.indexWithStr(indexName, typeName, "6", json6);
    }


    //查询索引的所有文档
    private static long queryAll(String indexName, String typeName) {
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchRequestBuilder srb = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb);
        SearchResponse sResponse = srb.get();
        //logger.info(sResponse);
        return sResponse.getHits().getTotalHits();
    }


    //删除所有文档
    public static void deleteAll(String indexName, String typeName) {
        try {
            DeleteByQueryResponse deleteByQueryResponse = new DeleteByQueryRequestBuilder(DeleteByQuery_Client, DeleteByQueryAction.INSTANCE)
                    .setIndices(indexName)
                    .setTypes(typeName)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .get();

            logger.info("删除所有文档。请不要这样做，删除所有文档请从esm.jd.com提交删索引申请。");

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


    /*delete by query shell
    #!/bin/bash

    INDEX_NAME="delete_by_query_test2"
    TYPE_NAME="my_type"
    IP="192.168.200.190"
    HTTP_PORT="9203"

    QUERY_BODY='{"query":{"range":{"time":{"lt":"now-5d"}}}}'

    #query test
    #curl -X POST "http://$IP:$HTTP_PORT/$INDEX_NAME/$TYPE_NAME/_search" -d "$QUERY_BODY"

    #delete
    curl -X DELETE "http://$IP:$HTTP_PORT/$INDEX_NAME/$TYPE_NAME/_query" -d "$QUERY_BODY"
    */
}