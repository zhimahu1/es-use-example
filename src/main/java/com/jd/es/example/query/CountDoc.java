package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * Title: CountDoc
 * Description: CountDoc
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/26
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class CountDoc {

    private static Logger logger = Logger.getLogger(CountDoc.class);

    public static void main(String[] args) {
        testCountDoc(Tool.INDEX_NAME, Tool.TYPE_NAME);
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
            }
     */
    public static long testCountDoc(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchRequestBuilder srb = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb);
        //logger.info(srb);
        SearchResponse searchResponse = srb.get();
        //logger.info(searchResponse);
        //logger.info("匹配的文档数：" + searchResponse.getHits().getTotalHits());

        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHists = hits.getHits();
        for (SearchHit hit : searchHists) {
            //logger.info(hit.getId() + " " + hit.getIndex() + " " + hit.getType() + " " + hit.getSourceAsString());
        }

        return searchResponse.getHits().getTotalHits();
    }
}