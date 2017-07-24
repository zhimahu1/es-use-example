package com.jd.es.example.aggregations;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.children.Children;

/**
 * Title: ChildrenAggregation
 * Description: ChildrenAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class ChildrenAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.ChildrenAggregation.class);

    //TODO 不懂 https://www.elastic.co/guide/en/elasticsearch/reference/2.1/search-aggregations-bucket-children-aggregation.html
    public static void main(String[] args) {
        childrenAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");
        childrenAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void childrenAggregation(String indexName, String typeName, String field) {

        AggregationBuilder aggregation =
                AggregationBuilders
                        .children("agg")
                        .childType(field);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Children agg = sResponse.getAggregations().get("agg");
        double value = agg.getDocCount(); // Doc count
        logger.info(field + " ：" + value);
    }
}
