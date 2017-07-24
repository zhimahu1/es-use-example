package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * Title: MatchAllQuery  查询所有文档
 * Description: MatchAllQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/10/14
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class MatchAllQuery {


    private static Logger logger = LoggerFactory.getLogger(MatchAllQuery.class);

    public static void main(String[] args) {
        testMatchAllQuery(Tool.INDEX_NAME, Tool.TYPE_NAME);
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size":100,
              "query": {
                "match_all": {}
              }
            }
     */
    public static void testMatchAllQuery(String indexName, String typeName) {
        try{
            QueryBuilder qb = QueryBuilders.matchAllQuery();
            SearchRequestBuilder srb = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).setSize(10);
            //.setFetchSource(false)//不返回source内容
            logger.info(srb.toString());
            SearchResponse sResponse = srb.get();//setSize(3) 可以设置查询的文档数的大小，默认10
            logger.info(sResponse.toString());

        }catch (Exception e){
            logger.error(e.getMessage(),e);
        }
    }


    /**
     * 如果不想返回source内容，可以在query中指定如下，查询结果就不会返回source了
     *
    {
        "_source": [
        null
        ]
    }
     */
}
