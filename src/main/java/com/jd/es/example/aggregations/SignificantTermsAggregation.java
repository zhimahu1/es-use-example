package com.jd.es.example.aggregations;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;

/**
 * Title: SignificantTermsAggregation
 * Description: SignificantTermsAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class SignificantTermsAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.SignificantTermsAggregation.class);

    //TODO
    public static void main(String[] args) {
        significantTermsAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME,"t1", "address.country");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void significantTermsAggregation(String indexName, String typeName,String termName, String field) {

        AggregationBuilder aggregation =
                AggregationBuilders
                        .significantTerms(termName)
                        .field(field);

        //SearchResponse sResponse = Tool.CLIENT.prepareSearch().setQuery(QueryBuilders.termQuery("gender", "male")).addAggregation(aggregation).get();
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).get();


        SignificantTerms agg = sResponse.getAggregations().get(termName);

        for (SignificantTerms.Bucket entry : agg.getBuckets()) {
            entry.getKey();      // Term
            entry.getDocCount(); // Doc count
            logger.info(entry.getKey() + " ：" + entry.getDocCount());
        }
    }
}
