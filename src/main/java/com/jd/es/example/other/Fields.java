package com.jd.es.example.other;

import com.jd.es.example.common.IndexDoc;
import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;

import java.util.Map;

/**
 * Title:
 *
 * 储存字段
 * 除了索引字段的值，你也可以选择 储存 字段的原始值以备日后取回。使用 Lucene 做后端的用户用储存字段来选择搜索结果的返回值，
 * 事实上，_source 字段就是一个储存字段。
 * 在 Elasticsearch 中，单独设置储存字段不是一个好做法。完整的文档已经被保存在 _source 字段中。
 * 通常最好的办法会是使用 _source 参数来过滤你需要的字段。
 * <p>
 * 我们一般都是从source中获取文档数据。但是，也可以从field中获取数据
 * 如果mapping中，对于某个type，设置了 "_source": {"enabled": false}，那么查询结果中就没有_source字段，只能查到文档id
 * 如果mapping中，field设置了store为true，例如 "age":{"type":"integer","store":true}，那么就可以在查询时通过指定fields获取到指定的field的数据
 *
 * store默认false
 * By default, field values indexed to make them searchable, but they are not stored.
 * This means that the field can be queried, but the original field value cannot be retrieved.
 *
 * store是不可以改的
 *
 * In certain situations it can make sense to store a field.
 * For instance, if you have a document with a title, a date, and a very large content field,
 * you may want to retrieve just the title and the date without having to extract those fields from a large _source field
 *
 * TODO fielddata_fields 和 fields有什么区别。目前发现fielddata_fields同时还返回_source, fields不返回_source
 *      https://www.elastic.co/guide/en/elasticsearch/reference/2.1/search-request-fielddata-fields.html
 *      https://www.elastic.co/guide/en/elasticsearch/reference/2.1/search-request-fields.html
 *
 * Description: Fields https://www.elastic.co/guide/en/elasticsearch/reference/2.1/mapping-store.html
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/10/26
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class Fields {

    private static Logger logger = Logger.getLogger(Fields.class);

    private static String this_index = "field_test_index";
    private static String this_type = "agg_type";

    /*
    //在测试环境创建索引field_test_index,其中country，height两个filed设置为store，其它字段都不store
    field_test_index
    POST
    {
        "mappings": {
        "agg_type": {
            "properties": {
                "country": {
                    "index": "not_analyzed",
                            "store": true,
                            "type": "string"
                },
                "gender": {
                    "index": "not_analyzed",
                            "type": "string"
                },
                "name": {
                    "index": "not_analyzed",
                            "type": "string"
                },
                "dateOfBirth": {
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSSZ",
                            "type": "date"
                },
                "age": {
                    "type": "integer"
                },
                "height": {
                    "type": "float",
                            "store": true
                }
            },
            "_source": {
                "enabled": false
            }
        }
    }
    }
     */

    /*  HTTP 接口
        URL:  field_test_index/agg_type/_search
        TYPE：GET
        BODY:
            {
                "query": {
                "term": {
                    "country": "China"
                }
            },
                "fields": [
                "country",
                        "name",
                        "age",
                        "height"
                ]
            }
     */
    public static void main(String[] args) {
        try {

            //prepareTestDoc();//插入8个文档

            SearchRequestBuilder sb = Tool.CLIENT.prepareSearch(this_index).setTypes(this_type)
                    .setQuery(QueryBuilders.termQuery("country", "China"));

            //添加你要查询的field
            sb.addField("country");
            sb.addField("name");
            sb.addField("age");
            sb.addField("height");

            logger.info("查询语句：" + sb);

            SearchResponse sResponse = sb.get();
            logger.info("查询结果：" + sResponse);
            SearchHits hits = sResponse.getHits();
            long count = hits.getTotalHits();
            logger.info("查询到的记录：" + count);
            SearchHit[] hitArray = hits.getHits();

            //只有country，height两个filed设置为store，其它字段都不store，所以只能查出这两个字段来
            for (int i = 0; i < count; i++) {
                SearchHit hit = hitArray[i];
                Map<String, SearchHitField> fm = hit.getFields();
                for (String key : fm.keySet()) {
                    SearchHitField f = fm.get(key);
                    System.out.println(f.getName() + " -> " + f.getValue());
                }
                System.out.println("========================");
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }






    //准备测试数据
    public static void prepareTestDoc(){

        String json1 = "{" +
                "\"name\":\"c1\"," +
                "\"gender\":\"male\"," +
                "\"country\":\"China\"," +
                "\"age\":\"1\"," +
                "\"height\":\"10.0\"," +
                "\"dateOfBirth\":\"2010-08-19 09:00:00\"" +
                "}";

        String json2 = "{" +
                "\"name\":\"c2\"," +
                "\"gender\":\"male\"," +
                "\"country\":\"China\"," +
                "\"age\":\"2\"," +
                "\"height\":\"20.0\"," +
                "\"dateOfBirth\":\"2011-08-19 09:00:00\"" +
                "}";

        String json3 = "{" +
                "\"name\":\"c3\"," +
                "\"gender\":\"female\"," +
                "\"country\":\"China\"," +
                "\"age\":\"3\"," +
                "\"height\":\"30.0\"," +
                "\"dateOfBirth\":\"2012-08-19 09:00:00\"" +
                "}";

        String json4 = "{" +
                "\"name\":\"c4\"," +
                "\"gender\":\"female\"," +
                "\"country\":\"China\"," +
                "\"age\":\"4\"," +
                "\"height\":\"40.0\"," +
                "\"dateOfBirth\":\"2012-08-19 09:00:00\"" +
                "}";

        String json5 = "{" +
                "\"name\":\"a5\"," +
                "\"gender\":\"male\"," +
                "\"country\":\"American\"," +
                "\"age\":\"5\"," +
                "\"height\":\"50.0\"," +
                "\"dateOfBirth\":\"2013-08-19 09:00:00\"" +
                "}";

        String json6 = "{" +
                "\"name\":\"a6\"," +
                "\"gender\":\"male\"," +
                "\"country\":\"American\"," +
                "\"age\":\"6\"," +
                "\"height\":\"60.0\"," +
                "\"dateOfBirth\":\"2014-08-19 09:00:00\"" +
                "}";

        String json7 = "{" +
                "\"name\":\"a7\"," +
                "\"gender\":\"female\"," +
                "\"country\":\"American\"," +
                "\"age\":\"7\"," +
                "\"height\":\"70.0\"," +
                "\"dateOfBirth\":\"2015-08-19 09:00:00\"" +
                "}";

        String json8 = "{" +
                "\"name\":\"a8\"," +
                "\"gender\":\"female\"," +
                "\"country\":\"American\"," +
                "\"age\":\"8\"," +
                "\"height\":\"80.0\"," +
                "\"dateOfBirth\":\"2015-08-19 09:00:00\"" +
                "}";

        IndexDoc.indexWithStr(this_index, this_type,"1",json1);
        IndexDoc.indexWithStr(this_index, this_type,"2",json2);
        IndexDoc.indexWithStr(this_index, this_type,"3",json3);
        IndexDoc.indexWithStr(this_index, this_type,"4",json4);
        IndexDoc.indexWithStr(this_index, this_type,"5",json5);
        IndexDoc.indexWithStr(this_index, this_type,"6",json6);
        IndexDoc.indexWithStr(this_index, this_type,"7",json7);
        IndexDoc.indexWithStr(this_index, this_type,"8",json8);
    }

}
