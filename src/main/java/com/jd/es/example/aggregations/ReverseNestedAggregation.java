package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

/**
 * Title: ReverseNestedAggregation
 * Description: ReverseNestedAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class ReverseNestedAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.ReverseNestedAggregation.class);

    //TODO What a fucking shit...
    public static void main(String[] args) {
        reverseNestedAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");
        reverseNestedAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void reverseNestedAggregation(String indexName, String typeName, String field) {

        AggregationBuilder aggregation =
                AggregationBuilders
                        .nested("agg").path("resellers")
                        .subAggregation(
                                AggregationBuilders
                                        .terms("name").field("resellers.name")
                                        .subAggregation(
                                                AggregationBuilders
                                                        .reverseNested("reseller_to_product")
                                        )
                        );
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Nested agg = sResponse.getAggregations().get("agg");
        Terms name = agg.getAggregations().get("name");
        for (Terms.Bucket bucket : name.getBuckets()) {
            ReverseNested resellerToProduct = bucket.getAggregations().get("reseller_to_product");
            double value = resellerToProduct.getDocCount(); // Doc count
            logger.info(" ：" + value);
        }
    }
}
