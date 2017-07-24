package com.jd.es.example.query;

import com.jd.es.example.common.DeleteDoc;
import com.jd.es.example.common.IndexDoc;
import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: ExistsQuery 与 MissingQuery 功能相反，查出指定field存在值的文档
 * Description:  可以查出字段存在且不为null的文档
 *              参考文档：https://www.elastic.co/guide/en/elasticsearch/guide/2.x/_dealing_with_null_values.html#_exists_query
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class ExistsQuery {

    private static Logger logger = Logger.getLogger(ExistsQuery.class);

    public static void main(String[] args) {
        //testExistsQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "name");
        //testExistsQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "haha");
        testExists();
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "exists": {
                  "field": "name"
                }
              }
            }
     */
    private static void testExistsQuery(String indexName, String typeName, String fieldName) {

        QueryBuilder qb = QueryBuilders.existsQuery(fieldName);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }

    /**
     HTTP 接口    exists 可以查到"undefined",""这两个，不能查到null和不存在，与missing相对应
     URL:agg_index/agg_type/_search
     TYPE：POST
     BODY:
        {
            "size": 20,
                "query": {
            "bool": {
                "must": [
                {
                    "term": {
                    "country": "Russia"
                }
                },
                {
                    "exists": {
                    "field": "gender"
                }
                }
                ]
            }
        }
        }
     */
    private static void testExists(){

        String json9 = "{" +
                "\"name\":\"r9\"," +
                "\"country\":\"Russia\"," +
                "\"age\":\"9\"," +
                "\"height\":\"80.0\"," +
                "\"dateOfBirth\":\"2015-08-19 09:00:00\"" +
                "}";

        String json10 = "{" +
                "\"name\":\"r10\"," +
                "\"gender\":null," +
                "\"country\":\"Russia\"," +
                "\"age\":\"10\"," +
                "\"height\":\"80.0\"," +
                "\"dateOfBirth\":\"2015-08-19 09:00:00\"" +
                "}";

        String json11 = "{" +
                "\"name\":\"r11\"," +
                "\"gender\":\"undefined\"," +
                "\"country\":\"Russia\"," +
                "\"age\":\"11\"," +
                "\"height\":\"80.0\"," +
                "\"dateOfBirth\":\"2015-08-19 09:00:00\"" +
                "}";

        String json12 = "{" +
                "\"name\":\"r12\"," +
                "\"gender\":\"\"," +
                "\"country\":\"Russia\"," +
                "\"age\":\"12\"," +
                "\"height\":\"80.0\"," +
                "\"dateOfBirth\":\"2015-08-19 09:00:00\"" +
                "}";

        IndexDoc.indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME, "9", json9);
        IndexDoc.indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME, "10", json10);
        IndexDoc.indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME, "11", json11);
        IndexDoc.indexWithStr(Tool.INDEX_NAME, Tool.TYPE_NAME, "12", json12);

        QueryBuilder qb = QueryBuilders.existsQuery("gender");
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(Tool.INDEX_NAME).setTypes(Tool.TYPE_NAME).setQuery(qb).get();
        logger.info(sResponse);


        DeleteDoc.deleteDocWithId(Tool.INDEX_NAME, Tool.TYPE_NAME, "9");
        DeleteDoc.deleteDocWithId(Tool.INDEX_NAME, Tool.TYPE_NAME, "10");
        DeleteDoc.deleteDocWithId(Tool.INDEX_NAME, Tool.TYPE_NAME, "11");
        DeleteDoc.deleteDocWithId(Tool.INDEX_NAME, Tool.TYPE_NAME, "12");
    }

    //测试：
    //1.创建索引exists_test_index
    /*
    exists_test_index
    POST
    {
        "mappings": {
        "my_type": {
            "properties": {
                "name": {
                    "type": "string"
                },
                "id": {
                    "type": "integer"
                }
            }
        }
    }
    }
    */

    //2.写入6个文档
    /*
    exists_test_index/my_type/1
    {"name":["one"]}

    exists_test_index/my_type/2
    {"name":["two1","two2"]}

    exists_test_index/my_type/3
    {"id":1}

    exists_test_index/my_type/4
    {"name":null}

    exists_test_index/my_type/5
    {"name":["value1",null]}

    exists_test_index/my_type/6
    {"name":""}
    */


    //exists查询
    /*
    exists_test_index/my_type/_search
    POST
    {
        "query": {
        "constant_score": {
            "filter": {
                "exists": {
                    "field": "name"
                }
            }
        }
    }
    }
    可以查出四个文档:1,2,5,6
    */

    //missing查询
    /*
    exists_test_index/my_type/_search
    POST
    {
        "query": {
        "constant_score": {
            "filter": {
                "missing": {
                    "field": "name"
                }
            }
        }
    }
    }
    可以查出四个文档:3,4
    */

}