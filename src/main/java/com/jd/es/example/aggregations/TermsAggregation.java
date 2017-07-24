package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

/**
 * Title: TermsAggregation 注意，如果存在多个桶的话，默认只返回10个桶。可以在terms中指定返回的桶的个数，排序
 * Description: TermsAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class TermsAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.TermsAggregation.class);

    //参考 https://www.elastic.co/guide/en/elasticsearch/reference/2.1/search-aggregations-bucket-terms-aggregation.html
    public static void main(String[] args) {
        termsAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME,"t1", "height");
        termsAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME,"t1",  "gender");
    }


    //聚合分页，貌似只能指定size，不能指定from
    //注意，如果存在多个桶的话，默认只返回10个桶。可以在terms中指定返回的桶的个数，排序
    /*
    "terms": {
                "size": 200,
                "field": "country",
                "order": {
                    "sum_doc": "desc"
                }
    }
    */




    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "aggs": {
                "t1": {
                  "terms": {
                    "field": "gender"
                  }
                }
              }
            }
     */
    private static void termsAggregation(String indexName, String typeName,String termName, String field) {

        AggregationBuilder aggregation = AggregationBuilders
                .terms(termName)
                .field(field);
        //.order(Terms.Order.count(true));//根据doc_count升序排序
        //.order(Terms.Order.term(true));//根据term的字母顺序排列 ,false是降序
        //.order(Terms.Order.aggregation("avg_height", false)).subAggregation(AggregationBuilders.avg("avg_height").field("height"));//根据嵌套的metrics sub-aggregation排序

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Terms genders = sResponse.getAggregations().get(termName);

        // For each entry
        for (Terms.Bucket entry : genders.getBuckets()) {
            entry.getKey();      // Term
            entry.getDocCount(); // Doc count
            logger.info(entry.getKey() + " ：" + entry.getDocCount());
        }

        logger.info(sResponse);

    }
}
