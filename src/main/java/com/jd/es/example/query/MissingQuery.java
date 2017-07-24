package com.jd.es.example.query;

import com.jd.es.example.common.DeleteDoc;
import com.jd.es.example.common.IndexDoc;
import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;


/**
 * Title: MissingQuery  missing query的功能本质上与exists query相反，missing query返回某个field没有值的文档
 * Description:   Returns documents that have only null values or no value in the original field
 *                可以查出字段不存在，或者为null的文档
 *                参考文档：https://www.elastic.co/guide/en/elasticsearch/reference/2.1/query-dsl-missing-query.html
 *
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class MissingQuery {

    private static Logger logger = Logger.getLogger(MissingQuery.class);

    private static String this_index = "missing_test_index";
    private static String this_type = "my_type";

    public static void main(String[] args) {

        testMissingQuery2();

        testMissingQuery(this_index, this_type, "gender");
        testMissingQuery(this_index, this_type, "country");
        testMissingQuery(this_index, this_type, "hahahahahahahahahahahaha");//因为所有文档都没有这个字段，所以所有的文档都查出来了
    }

    /*  HTTP 接口  missing可以查出null和不存在，与exists相对应
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "missing": {
                  "field": "name"
                }
              }
            }
     */
    private static void testMissingQuery(String indexName, String typeName, String fieldName) {

        QueryBuilder qb = QueryBuilders.missingQuery(fieldName)
                .existence(true)//find missing field that doesn’t exist
                .nullValue(true);//find missing field with an explicit null value //需要配合mapping的 null_value 使用，否则，没用

        SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb);
        logger.info(searchRequestBuilder);
        SearchResponse sResponse = searchRequestBuilder.get();
        logger.info("fieldName:" + sResponse.getHits().getTotalHits());
    }


    /*  HTTP 接口  missing可以查出null和不存在，与exists相对应
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size": 20,
              "query": {
                "missing": {
                  "field": "gender",
                  "null_value" : true,
                  "existence" : true
                }
              }
            }
     */
    private static void testMissingQuery2(){

        String json1 = "{" +
                "\"country\":\"Russia\"" +
                "}";

        String json2 = "{\n" +
                "  \"gender\": null," +
                "  \"country\": [\"Russia\",null]" +
                "}";

        String json3 = "{" +
                "\"gender\":\"undefined\"," +
                "\"country\":\"Russia\"" +
                "}";

        String json4 = "{" +
                "\"gender\":\"\"," +
                "\"country\":\"Russia\"" +
                "}";

        IndexDoc.indexWithStr(this_index, this_type, "1", json1);
        IndexDoc.indexWithStr(this_index, this_type, "2", json2);
        IndexDoc.indexWithStr(this_index, this_type, "3", json3);
        IndexDoc.indexWithStr(this_index, this_type, "4", json4);

        QueryBuilder qb = QueryBuilders.missingQuery("gender")
                .existence(true)//find missing field that doesn’t exist  //设置为false，查不出什么啊
                .nullValue(true);//find missing field with an explicit null value //需要配合mapping的 null_value 使用，否则，没用

        logger.info(qb);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(this_index).setTypes(this_type).setQuery(qb).get();
        logger.info("结果：" + sResponse);

/*        DeleteDoc.deleteDocWithId(this_index, this_type, "1");
        DeleteDoc.deleteDocWithId(this_index, this_type, "2");
        DeleteDoc.deleteDocWithId(this_index, this_type, "3");
        DeleteDoc.deleteDocWithId(this_index, this_type, "4");*/
    }
}