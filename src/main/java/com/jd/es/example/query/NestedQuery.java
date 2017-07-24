package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: NestedQuery
 * Description: NestedQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class NestedQuery {

    private static Logger logger = Logger.getLogger(NestedQuery.class);

    public static void main(String[] args) {
        testNestedQuery("example_index", "type1");
    }


    /**
    {
        "settings": {
        "index": {
            "number_of_shards": "4",
                    "number_of_replicas": "0"
        }
    },
        "mappings": {
        "type1": {
            "properties": {
                "message": {
                    "type": "string"
                },
                "age": {
                    "type": "integer"
                },
                "nested_field": {
                    "type": "nested",
                            "properties": {
                        "colour": {
                            "index": "not_analyzed",
                                    "type": "string"
                        },
                        "level": {
                            "type": "long"
                        }
                    }
                }
            }
        }
    },
        "aliases": {},
        "warmers": {}
    }
    */


    /** 嵌套field的mapping如下：
        "nested_field": {
            "type": "nested",
                    "properties": {
                "colour": {
                    "index": "not_analyzed",
                            "type": "string"
                },
                "level": {
                    "type": "long"
                }
            }
        }
     */


    /*  HTTP 接口
        URL:example_index/type1/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "nested": {
                  "path": "nested_field",
                  "score_mode": "avg",
                  "query": {
                    "bool": {
                      "must": [
                        {
                          "match": {
                            "nested_field.colour": "blue"
                          }
                        },
                        {
                          "range": {
                            "nested_field.level": {
                              "gt": 1
                            }
                          }
                        }
                      ]
                    }
                  }
                }
              }
            }
     */
    private static void testNestedQuery(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.nestedQuery(
                "nested_field",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("nested_field.colour", "blue"))//必须使用全路径 nested_field.colour
                        .must(QueryBuilders.rangeQuery("nested_field.level").gt(1))
        )
                .scoreMode("avg");//score mode could be max, total, avg (default) or none
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
