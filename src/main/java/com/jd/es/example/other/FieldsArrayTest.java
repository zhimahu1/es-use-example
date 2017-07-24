package com.jd.es.example.other;

import com.jd.es.example.common.Tool;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Title: FieldsArrayTest
 * Description: FieldsArrayTest
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2017/1/16
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class FieldsArrayTest {

    private static Logger logger = LoggerFactory.getLogger(FieldsArrayTest.class);

    private static String This_Index = "array_field_test";
    private static String This_Type = "my_type_name";

    public static void main(String[] args){
        testFieldsArrayTest();
    }


    /**
    对于 array类型，_source 设为 false，store 设为 true，通过field查询,看看结果如何
    */

    //1.创建mapping
    /*
    URL:array_field_test
    POST
    {
        "settings": {
        "index": {
            "number_of_shards": "4",
                    "number_of_replicas": "1"
        }
    },
        "mappings": {
        "my_type_name": {
            "_source": {
                "enabled": false
            },
            "properties": {
                "message": {
                    "type": "string",
                            "store": true
                },
                "age": {
                    "type": "integer",
                            "store": true
                }
            }
        }
    },
        "aliases": {},
        "warmers": {}
    }
    */

    //2.写入文档
    /*
    URL:array_field_test/my_type_name/1
    POST
    {
        "message": [
        "one",
                "two"
        ],
        "age": [
        1,
                2
        ]
    }

    URL:array_field_test/my_type_name/2
    POST
    {
        "message": [
        "three",
                "four",
                "five"
        ],
        "age": [
        3,
                4,
                5
        ]
    }
    */

    //3.查询
    /*
    URL:array_field_test/my_type_name/_search
    POST
    {
        "fields": [
        "message",
         "age"
        ]
    }
    */

    private static  void testFieldsArrayTest(){

        try{
/*            SearchRequestBuilder sb = Tool.CLIENT.prepareSearch(This_Index).setTypes(This_Type)
                    ;//.setQuery(QueryBuilders.termQuery("country", "China"));

            //添加你要查询的field
            sb.addField("message");
            sb.addField("age");
            logger.info("查询语句：" + sb);

            SearchResponse sResponse = sb.get();
            logger.info("查询结果：" + sResponse);

            SearchHits hits = sResponse.getHits();
            long count = hits.getTotalHits();

            SearchHit[] hitArray = hits.getHits();


            for (SearchHit hit : hitArray) {
                Map<String, SearchHitField> fields = hit.getFields();
                for (String key : fields.keySet()) {
                    SearchHitField f = fields.get(key);
                    System.out.println(f.getName() + " -> " + f.getValue());
                }
                System.out.println("========================");

                for(String key : fields.keySet()) {
                    System.out.println(key + ":");
                    System.out.println("getValue:" + fields.get(key).getValue().toString());
                    System.out.println("getValues:" + fields.get(key).getValues());
                    System.out.println("getValues().get(0):" + fields.get(key).getValues().get(0));
                    System.out.println("getClass:" + fields.get(key).getValues().get(0).getClass());
                }
            }*/

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

}
