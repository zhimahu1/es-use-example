package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

/**
 * Title: TopHitsAggregation
 * Description: TopHitsAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class TopHitsAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.TopHitsAggregation.class);

    public static void main(String[] args) {
        //topHitsAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");
        //topHitsAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");
        topHitsAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "country");
        //topHitsAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "gender");

    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void topHitsAggregation(String indexName, String typeName, String field) {

        AggregationBuilder aggregation =
                AggregationBuilders
                        .terms("agg").field(field)
                        .subAggregation(
                                AggregationBuilders.topHits("top")//默认查询三条
                        );

        //You can use most of the options available for standard search such as from, size, sort, highlight, explain…
        /*AggregationBuilder aggregation =
                AggregationBuilders
                        .terms("agg").field("gender")
                        .subAggregation(
                                AggregationBuilders.topHits("top")
                                        .setExplain(true)
                                        .setSize(1)
                                        .setFrom(10)
                        );*/


        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Terms agg = sResponse.getAggregations().get("agg");

        // For each entry
        for (Terms.Bucket entry : agg.getBuckets()) {
            String key = String.valueOf(entry.getKey()) ;                    // bucket key
            long docCount = entry.getDocCount();            // Doc count
            logger.info("key:"+ key +" doc_count:"+ docCount);

            // We ask for top_hits for each bucket
            TopHits topHits = entry.getAggregations().get("top");
            for (SearchHit hit : topHits.getHits().getHits()) {
                logger.info("id :" +  hit.getId()+" _source:"+ hit.getSourceAsString());
            }
        }
    }

}
