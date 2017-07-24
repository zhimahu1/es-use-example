package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;

/**
 * Title: FiltersAggregation  每一个filter作为一个桶。
 * 下面http api 中"gender": "female"作为一个桶，"country": "China"也作为一个桶，"name": "c1"作为一个桶，共三个桶
 * Description: FiltersAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class FiltersAggregation {
    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.FiltersAggregation.class);

    public static void main(String[] args) {
        filtersAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "gender", "male", "gender", "female");
        filtersAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "country", "China", "name", "c1");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size": 0,
              "aggs": {
                "fffffffffffffffffff": {
                  "filters": {
                    "filters": {
                      "f1": {
                        "term": {
                          "gender": "female"
                        }
                      },
                      "f2": {
                        "term": {
                          "country": "China"
                        }
                      },
                      "f3": {
                        "term": {
                          "name": "c1"
                        }
                      }
                    }
                  },
                  "aggs": {
                    "avg_age": {
                      "avg": {
                        "field": "age"
                      }
                    }
                  }
                }
              }
            }
     */
    private static void filtersAggregation(String indexName, String typeName, String termField1, String termValue1, String termField2, String termValue2) {

        AbstractAggregationBuilder aggregation =
                AggregationBuilders
                        .filters("agg")
                        .filter("f1", QueryBuilders.termQuery(termField1, termValue1))
                        .filter("f2", QueryBuilders.termQuery(termField2, termValue2));

        SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation);
        logger.info("请求：" + searchRequestBuilder);
        SearchResponse sResponse = searchRequestBuilder.get();

        Filters agg = sResponse.getAggregations().get("agg");

        // For each entry
        for (Filters.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();            // bucket key
            long docCount = entry.getDocCount();            // Doc count
            logger.info("key:" + key + " docCount:" + docCount);
        }
        logger.info(sResponse);
    }
}
