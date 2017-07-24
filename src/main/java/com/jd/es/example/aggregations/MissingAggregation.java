package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;

/**
 * Title: MissingAggregation
 * Description: MissingAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class MissingAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.MissingAggregation.class);

    public static void main(String[] args) {
        missingAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");
        missingAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "aggs": {
                "products_without_a_age": {
                  "missing": {
                    "field": "age"
                  }
                }
              }
            }
     */
    private static void missingAggregation(String indexName, String typeName, String field) {

        AggregationBuilder aggregation = AggregationBuilders.missing("agg").field(field);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Missing agg = sResponse.getAggregations().get("agg");
        double value = agg.getDocCount();
        logger.info(field + " ：" + value);
    }
}
