package com.jd.es.example.aggregations;


import com.jd.es.example.common.IndexDoc;
import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;


/**
 * Title: NestedAggregation
 * Description: NestedAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class NestedAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.NestedAggregation.class);

    public static void main(String[] args) {
        nestedAggregation("index_product", "type_product", "resellers","resellers.price");
    }

    /*  HTTP 接口
        URL:index_product/type_product/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "match": {
                  "product_name": "apple"
                }
              },
              "aggs": {
                "resellers": {
                  "nested": {
                    "path": "resellers"
                  },
                  "aggs": {
                    "min_price": {
                      "min": {
                        "field": "resellers.price"
                      }
                    }
                  }
                }
              }
            }
     */
    private static void nestedAggregation(String indexName, String typeName, String path,String subField) {

        //term 和 match有什么区别？看TermAndMatchCompare.java
        TermQueryBuilder qb = QueryBuilders.termQuery("product_name", "clothes");
        //MatchQueryBuilder qb = QueryBuilders.matchQuery("product_name", "clothes");

        AggregationBuilder ab = AggregationBuilders.nested("agg")
                .path(path).subAggregation(AggregationBuilders.min("agg").field(subField));

        //SearchRequestBuilder srb = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(ab);
        SearchRequestBuilder srb = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).addAggregation(ab);
        logger.info("请求："+srb);
        SearchResponse sResponse = srb.execute().actionGet();

        Nested agg = sResponse.getAggregations().get("agg");
        double value = agg.getDocCount(); // Doc count
        logger.info("getDocCount ：" + value);
        logger.info(sResponse);
    }


    /** 创建索引，mapping   index_product/type_product
    PUT  index_product
    {
        "mappings": {
        "type_product": {
            "properties": {
                "resellers": {
                    "type": "nested",
                            "properties": {
                        "name": {
                            "type": "string"
                        },
                        "price": {
                            "type": "double"
                        }
                    }
                },
                "weight": {
                    "type": "integer"
                },
                "product_name": {
                    "type": "string"
                },
                "message": {
                    "type": "string"
                }
            }
        }
    }
    }
     */


    /** 插入文档
    PUT index_product/type_product/2
    {
        "product_name": "apple",
            "weight": 2,
            "message":"red apple",
            "resellers": [
        {
            "name": "jd",
                "price": 4
        },
        {
            "name": "taobao",
                "price": 5
        },
        {
            "name": "amazon",
                "price": 6
        }
        ]
    }
     */


    private static void indexDoc() {
        String json3 = "{\n" +
                "  \"product_name\": \"clothes\",\n" +
                "  \"weight\": 3,\n" +
                "  \"message\": null,\n" +
                "  \"resellers\": [\n" +
                "    {\n" +
                "      \"name\": \"jd\",\n" +
                "      \"price\": 7\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"taobao\",\n" +
                "      \"price\": 8\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"amazon\",\n" +
                "      \"price\": 9\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        IndexDoc.indexWithStr("index_product", "type_product", "3", json3);
    }

}
