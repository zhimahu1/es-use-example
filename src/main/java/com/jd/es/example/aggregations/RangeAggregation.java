package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;

/**
 * Title: RangeAggregation
 * Description: RangeAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class RangeAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.RangeAggregation.class);

    public static void main(String[] args) {
        rangeAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");
        rangeAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size": 0,
              "aggs": {
                "my_age_range": {
                  "range": {
                    "field": "age",
                    "ranges": [
                      {
                        "to": 2
                      },
                      {
                        "from": 2,
                        "to": 4
                      },
                      {
                        "from": 4
                      }
                    ]
                  }
                }
              }
            }

            一样：
            {
              "size": 0,
              "aggregations": {
                "my_age_range": {
                  "range": {
                    "field": "age",
                    "ranges": [
                      {
                        "to": 2
                      },
                      {
                        "from": 2,
                        "to": 4
                      },
                      {
                        "from": 4
                      }
                    ]
                  }
                }
              }
            }
     */
    private static void rangeAggregation(String indexName, String typeName, String field) {

        AggregationBuilder aggregation =
                AggregationBuilders
                        .range("agg")
                        .field(field)
                        .addUnboundedTo(1.0f)               // from -infinity to 1.0 (excluded)
                        .addRange(1.0f, 1.5f)               // from 1.0 to 1.5 (excluded)
                        .addUnboundedFrom(1.5f);            // from 1.5 to +infinity

        SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation);
        logger.info(searchRequestBuilder);

        SearchResponse sResponse = searchRequestBuilder.get();
        logger.info(sResponse);

        Range agg = sResponse.getAggregations().get("agg");
        for (Range.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();             // Range as key
            Number from = (Number) entry.getFrom();          // Bucket from
            Number to = (Number) entry.getTo();              // Bucket to
            long docCount = entry.getDocCount();    // Doc count

            logger.info("key:"+key+" from:"+from+" to:"+to+" doc_count:" +docCount);
        }
    }
}
