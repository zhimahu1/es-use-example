package com.jd.es.example.query;

import com.jd.es.example.common.CreateIndex;
import com.jd.es.example.common.IndexDoc;
import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortParseElement;

import java.io.IOException;

/**
 * Scrolling is not intended for real time user requests, but rather for processing large amounts of data,
 * e.g. in order to reindex the contents of one index into a new index with a different configuration.
 * 翻译：Scrolling并不是用于实时请求，而是用于处理大量的数据。例如：把一个索引中的数据导入到一个新索引中去
 * <p>
 * The results that are returned from a scroll request reflect the state of the index at the time that the initial search request was made, like a snapshot in time.
 * Subsequent changes to documents (index, update or delete) will only affect later search requests.
 * 翻译：scroll请求返回的结果反映的是初始请求当时索引的状态，就像一个快照。之后对于文档的更改（增，删，改）只会影响之后的请求，而不会影响这个scroll查询。
 */


/**
 * Title: ScrollDoc
 * Description: ScrollDoc
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/22
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class ScrollDoc {

    private static Logger logger = Logger.getLogger(ScrollDoc.class);

    public static void main(String[] args) {
        String indexName = "index10000doc";
        String typeName = "myType";
        //prepareData(indexName,typeName);//写入10000条数据

        testScrollDoc(indexName, typeName);

        //testScrollId("c2NhbjsxOzIxODAwNDg2OTp0Ry1ScVBCZVRHLWV6X2ttRFpUTUpnOzE7dG90YWxfaGl0czoxMDAwMDs=");
    }

    /*  HTTP 接口
        URL:index10000doc/myType/_search?scroll=1m
        TYPE：GET
        BODY:
            {
              "query": {
                "match_all": {}
              }
            }

        URL:_search/scroll
        TYPE：GET
        BODY:
            {
              "scroll": "1m",
              "scroll_id": "cXVlcnlUaGVuRmV0Y2g7NTsxMjgwNDY4MTpXZWE4WmVqN1NQMnZqUnZWYUw3Q2tROzEzMTg1OTI3Okd0UzY1dk5aVHFDNm05WFZXalljY3c7MTI3ODcyNDM6ZFJ2eFB6Ml9SS3V6MTlIbkE0QW9GZzsxMzI5NDIxOTpZOFlGbl9VWVJvU0haNW5aVjdqUjB3OzIyMDQ0MDc5OjE2WElLZl9NU0UtdXBaV0NlZVU0QVE7MDs="
            }

        URL:index10000doc/myType/_search?scroll=1m
        TYPE：GET
        BODY:
            {
              "sort": [
                "_doc"
              ]
            }
     */
    private static void testScrollDoc(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.matchAllQuery();//termQuery("multi", "test");
        //QueryBuilder qb = QueryBuilders.rangeQuery("id").from(1000).to(2000);

        //.setSearchType(SearchType.SCAN)已经不推荐使用，将在2.1.2后面的版本中移除，应该在你的查询中根据_doc排序
        //SearchType.SCAN will be removed in 3.0, you should do a regular scroll instead, ordered by `_doc`
        //在你的查询中根据_doc排序  {"sort": ["_doc"]}

        //初始请求提交
        //If the request specifies aggregations, only the initial search response will contain the aggregations results.
        SearchResponse scrollResp = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName)
                .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))//Its value does not need to be long enough to process all data — it just needs to be long enough to process the previous batch of results. Each scroll request (with the scroll parameter) sets a new expiry time.
                .setQuery(qb)
                .setSize(100).execute().actionGet(); //100 hits per shard will be returned for each scroll

        //changeData(indexName,typeName);//在scroll初始请求提交，后面更改文档（增，删，改），不会影响scroll的结果

        int times = 0;
        int docGet = 0;
        //Scroll until no hits are returned
        while (true) {
            SearchHit[] searchHits = scrollResp.getHits().getHits();

            logger.info("执行次数：" + ++times + " 获取的文档数：" + searchHits.length);
            docGet += searchHits.length;

            for (SearchHit hit : searchHits) {
                //Handle the hit...
                logger.info("id:" + hit.getId() + " " + hit.getSourceAsString());
            }

            logger.info("scrollId:" + scrollResp.getScrollId());//根据scrollResp.getScrollId() 取数据之后，该id就失效了，必须根据返回的新的id继续去取数据

            //如果处理时间比较长，scrollId过期后，可能只会查出部分数据
/*            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            //根据新的id继续取数据
            scrollResp = Tool.CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
        logger.info("总共获取文档数：" + docGet);
    }

    //更改数据
    public static void changeData(String indexName, String typeName) {

        //在scroll初始请求提交后，后面更改文档（增，删，改），不会影响scroll的结果
        //虽然下面把文档9000-9999改成了{"id":9999,"name":"zz"}，
        //但是查出来的依然是{"id":9999,"name":"cc"}
        //it's funny \(^o^)/~
        try {
            for (int i = 9000; i < 10000; i++) {
                String json = "{" +
                        "\"id\":" + i + "," +
                        "\"name\":\"zz\"" +
                        "}";
                IndexDoc.indexWithStr(indexName, typeName, Integer.toString(i), json);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //准备数据
    public static void prepareData(String indexName, String typeName) {
        try {
            CreateIndex.createIndexNoMapping(indexName, 5, 1);

            for (int i = 0; i < 10000; i++) {
                String json = "{" +
                        "\"id\":" + i + "," +
                        "\"name\":\"cc\"" +
                        "}";
                IndexDoc.indexWithStr(indexName, typeName, Integer.toString(i), json);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void testScrollId(String scrollId) {

        SearchResponse scrollResp = Tool.CLIENT.prepareSearchScroll(scrollId).setScroll(new TimeValue(60000)).execute().actionGet();

        int times = 0;
        while (true) {
            SearchHit[] searchHits = scrollResp.getHits().getHits();

            logger.info("执行次数：" + ++times + " 获取的文档数：" + searchHits.length);

            for (SearchHit hit : searchHits) {
                //Handle the hit...
                logger.info("id:" + hit.getId() + " " + hit.getSourceAsString());
            }
            logger.info("scrollId:" + scrollResp.getScrollId());

            scrollResp = Tool.CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
    }
}