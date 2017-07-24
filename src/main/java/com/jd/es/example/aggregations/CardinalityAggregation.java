package com.jd.es.example.aggregations;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;

/**
 * Title: CardinalityAggregation  总共有多少不同的值  相当于SQL中的 select count(distinct clusterId) from table
 *        参考文档:https://www.elastic.co/guide/en/elasticsearch/guide/current/cardinality.html
 *        the cardinality metric is an approximate algorithm 近似统计算法，可能会有误差
 *        可以通过参数 "precision_threshold" : 100
 *        This threshold defines the point under which cardinalities are expected to be very close to accurate.
 *        数据集中不同数据的个数如果在该阈值以下，基本（基本，基本）能够保证结果的正确性
 *        "cardinality" : {
                            "field" : "color",
                            "precision_threshold" : 100
                         }
 *        Although not guaranteed by the algorithm, if a cardinality is under the threshold, it is almost always 100% accurate.
 *        precision_threshold accepts a number from 0–40,000. Larger values are treated as equivalent to 40,000.
 * Description: CardinalityAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class CardinalityAggregation {
    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.CardinalityAggregation.class);

    public static void main(String[] args) {
        cardinalityAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");//总共有多少不同的height
        cardinalityAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");//总共有多少不同的age
        cardinalityAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "gender");//总共有多少不同的gender
        cardinalityAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "country");//总共有多少不同的country

        cardinalityAggregation2(Tool.INDEX_NAME, Tool.TYPE_NAME,"country","name");//每个国家有多少不同名字
        cardinalityAggregation2(Tool.INDEX_NAME, Tool.TYPE_NAME,"country","gender");//每个国家有多少不同的性别
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size": 0,
              "aggregations": {
                "agg": {
                  "cardinality": {
                    "field": "country"
                  }
                }
              }
            }
     */
    private static void cardinalityAggregation(String indexName, String typeName, String field) {

        MetricsAggregationBuilder aggregation = AggregationBuilders.cardinality("agg").field(field);
        SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation);
        logger.info(searchRequestBuilder);

        SearchResponse sResponse = searchRequestBuilder.get();

        Cardinality agg = sResponse.getAggregations().get("agg");
        long value = agg.getValue();
        logger.info(field + " cardinalityAggregation value ：" + value);
    }


    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size": 0,
              "aggregations": {
                "a1": {
                  "terms": {
                    "field": "country"
                  },
                  "aggregations": {
                    "a2": {
                      "cardinality": {
                        "field": "name"
                      }
                    }
                  }
                }
              }
            }
     */
    private static void cardinalityAggregation2(String indexName, String typeName, String field1,String field2) {

        AggregationBuilder aggregation1 = AggregationBuilders.terms("a1").field(field1);
        MetricsAggregationBuilder aggregation2 = AggregationBuilders.cardinality("a2").field(field2);
        aggregation1.subAggregation(aggregation2);

        SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setSize(0).addAggregation(aggregation1);
        logger.info(searchRequestBuilder);

        SearchResponse sResponse = searchRequestBuilder.get();
        logger.info(sResponse);
    }


    //优化聚合性能
    //可以通过指定hash函数，让ES在写入文档的时候，根据指定的hash函数提前对字段计算出一个hash值存储起来。
    //之后通过cardinality进行聚合的时候，就可以直接load这个hash值，不用实时计算hash值，以此提高聚合速度。
    //虽然速度提高了，但是写入文档的速度会相应下降。这样做只是将时间的开销由聚合阶段移到了索引阶段。

    //创建mapping如下
    /**
    index_name
    PUT
    {
        "mappings": {
        "transactions": {
            "properties": {
                "color": {
                    "type": "string",
                            "fields": {
                        "hash": {
                            "type": "murmur3"
                        }
                    }
                }
            }
        }
    }
    }
     */

    //聚合查询
    /*
    /index_name/type_name/_search
    GET
    {
        "size" : 0,
            "aggs" : {
        "distinct_colors" : {
            "cardinality" : {
                "field" : "color.hash"
            }
        }
    }
    }
    */

}