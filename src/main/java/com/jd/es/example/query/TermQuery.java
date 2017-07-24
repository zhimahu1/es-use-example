package com.jd.es.example.query;

import com.jd.es.example.common.SimpleHttpClient;
import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;

/**
 * Title: TermQuery
 * Description: TermQuery term对于你查询的内容不进行分词处理，这不同于match
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class TermQuery {

    private static Logger logger = Logger.getLogger(TermQuery.class);

    public static void main(String[] args) {
        testTermQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "name", "c1");
        testTermQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "age", "2");
        try {
            logger.info(SimpleHttpClient.get(Tool.URL + "/" + Tool.INDEX_NAME + "?pretty" ,"GET"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "term": {
                  "name": "c1"
                }
              }
            }
     */
    public static void testTermQuery(String indexName, String typeName, String fieldName, String fieldValue) {

        QueryBuilder qb = QueryBuilders.termQuery(fieldName, fieldValue);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}