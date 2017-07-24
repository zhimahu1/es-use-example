package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;


/**
 * Title: RangeQuery
 * Description: RangeQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class RangeQuery {

    private static Logger logger = Logger.getLogger(RangeQuery.class);

    public static void main(String[] args) {
        testRangeQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "age", 1,4);
        testRangeQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "height", 40,60);
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query":{
                 "range" : {
                    "age" : {
                        "gte" : 1,
                        "lte" : 3
                    }
                }
              }
            }

        BODY2:时间范围
                {
                  "query":{
                     "range" : {
                        "dateOfBirth" : {
                            "gte" : "now-365d/d",
                            "lt" :  "now/d"
                        }
                    }
                  }
                }

        BODY3:时间范围  指定时间格式
            {
              "query":{
                 "range" : {
                    "dateOfBirth" : {
                        "gte": "01/01/2014",
                        "lte": "2015",
                        "format": "dd/MM/yyyy||yyyy"
                    }
                }
              }
            }
     */
    private static void testRangeQuery(String indexName, String typeName, String fieldName,int from,int to) {

        QueryBuilder qb = QueryBuilders.rangeQuery(fieldName).from(from).to(to)
                .includeLower(true)//include lower value means that from is gt when false or gte when true
                .includeUpper(false);//include upper value means that to is lt when false or lte when true

        // A simplified form using gte, gt, lt or lte
        /**
         gte: Greater-than or equal to
         gt:  Greater-than
         lte: Less-than or equal to
         lt:  Less-than
         */
        QueryBuilder qb2 = QueryBuilders.rangeQuery("age")
                .gte("10")
                .lt("20");

        logger.info(qb);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }


    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "bool": {
                  "should": [
                    {
                      "range": {
                        "age": {
                          "lte": 1
                        }
                      }
                    },
                    {
                      "range": {
                        "age": {
                          "gte": 8
                        }
                      }
                    }
                  ],
                  "minimum_should_match": 1
                }
              }
            }
     */
    // "minimum_should_match": 默认就是1

}