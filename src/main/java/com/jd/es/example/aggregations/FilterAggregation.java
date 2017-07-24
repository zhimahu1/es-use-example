package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;

/**
 * Title: FilterAggregation 使用filter过滤条件，过滤后的数据同一个桶
 * Description: FilterAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class FilterAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.FilterAggregation.class);

    public static void main(String[] args) {
        //filterAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME ,"gender", "male");
        filterAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME ,"country", "China");
        //filterAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME ,"name", "c1");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size":0,
              "aggs": {
                "ffff": {
                  "filter": {
                    "term": {
                      "gender": "female"
                    }
                  },
                  "aggs" : {
                            "avg_age" : { "avg" : { "field" : "age" } }
                        }
                }
              }
            }
     */
    private static void filterAggregation(String indexName, String typeName, String termField,String termValue) {

        AbstractAggregationBuilder aggregation =
                AggregationBuilders
                .filter("my_filter_agg")
                .filter(QueryBuilders.termQuery(termField, termValue)).subAggregation(AggregationBuilders.avg("my_agg").field("age"));

        SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).setSize(10);
        logger.info(searchRequestBuilder);
        SearchResponse sResponse = searchRequestBuilder.get();

/*        Filter agg = sResponse.getAggregations().get("agg");
        long docCount = agg.getDocCount(); // Doc count
        logger.info("docCount:"+docCount);*/
        logger.info(sResponse);
    }
}
