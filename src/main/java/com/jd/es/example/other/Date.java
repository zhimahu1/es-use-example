package com.jd.es.example.other;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Title: ES中的date数据类型  如果需要支持"2015-01-01 12:10:30",必须指定date的格式 "format": "yyy-MM-dd HH:mm:ss"。format可以用户自定义.
 *        你指定什么格式，往ES写入数据就应该以这种格式写入。如果格式不对，会抛异常mapper_parsing_exception, illegal_argument_exception "reason": "Invalid format:
 *
 * Description: Date 参考：https://www.elastic.co/guide/en/elasticsearch/reference/2.1/date.html
 *  ES 的时间可以是 "2015-01-01" or "2015/01/01 12:10:30"，or表示毫秒的一个long数，or表示秒的一个int数
 *  Internally, dates are converted to UTC (if the time-zone is specified) and stored as a long number
 *  representing milliseconds-since-the-epoch.
 *
 *  Date的formats可以自定义
 *  format 的默认值是 "strict_date_optional_time||epoch_millis"
 *
 * Multiple formats can be specified by separating them with || as a separator.
 * Each format will be tried in turn until a matching format is found.
 * The first format will be used to convert the milliseconds-since-the-epoch value back into a string.
 * 举个栗子：在执行date_histogram聚合查询的时候，聚合结果中包含一个"key_as_string"字段，
 * 这个字段就是将es中存储的毫秒值根据用户指定的format的第一种格式进行格式化处理，表示为字符串.最下面有一个聚合的例子。
 *
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/10/26
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class Date {

    private static Logger logger = Logger.getLogger(Date.class);

    private static String this_index = "date_test_index";
    private static String this_index2 = "date_test_index2";
    private static String this_type = "my_type";

    public static void main(String[] agrs){
        //testDate();
        testDate2();
    }

    //如果不存在，先创建一个测试索引
    /*  HTTP 接口
        URL:  date_test_index
        TYPE：PUT
        BODY:
            {
                "mappings": {
                "my_type": {
                    "properties": {
                        "date": {
                            "type": "date"
                        }
                    }
                }
            }
            }
      */
    //不指定format的话，format默认值: "strict_date_optional_time||epoch_millis",


    /* 插入三个文档
    date_test_index/my_type/1
    PUT
    {
        "date": "2015-01-01"
    }

    date_test_index/my_type/2
    PUT
    {
        "date": "2015-01-01T12:10:30Z"
    }

    date_test_index/my_type/3
    PUT
    {
        "date": 1420070400001
    }
    */

    //1420070400001(ms) 表示的是 2015-01-01 08:00:00

    //如果插入以下格式的数据，会报错
    //报错 "reason": "Invalid format: \"2015-01-01 10:10:10\" is malformed at \" 10:10:10\"",
    //因为format的默认格式是"strict_date_optional_time||epoch_millis"，不支持"2015-01-01 10:10:10"
    //如果想插入"2015-01-01 10:10:10"格式的时间，需要自定义format。format可以定义为： "yyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
    /*
    date_test_index/my_type/4
    PUT
    {
        "date": "2015-01-01 10:10:10"
    }
    */

    public static void testDate(){

        SearchRequestBuilder sb = Tool.CLIENT.prepareSearch(this_index).setTypes(this_type)
                .addSort("date", SortOrder.DESC);//SortOrder.ASC;
        logger.info(sb);

        SearchResponse sr = sb.get();
        logger.info(sr);
    }



    //如果不存在，先创建一个测试索引
    /*  HTTP 接口
        URL:  date_test_index2
        TYPE：PUT
        BODY:
            {
                "mappings": {
                "my_type": {
                    "properties": {
                        "date": {
                              "type":   "date",
                              "format": "yyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                         }
                    }
                }
            }
            }
      */


    /* 插入三个文档
    date_test_index2/my_type/1
    PUT
    {
        "date": "2016-01-01 10:10:10"
    }

    date_test_index2/my_type/2
    PUT
    {
        "date": "2000-01-01"
    }

    date_test_index2/my_type/3
    PUT
    {
        "date": 1420070400001
    }
    */

    //1420070400001(ms) 表示的是 2015-01-01 08:00:00

    public static void testDate2(){

        SearchRequestBuilder sb = Tool.CLIENT.prepareSearch(this_index2).setTypes(this_type)
                .addSort("date", SortOrder.ASC);//SortOrder.DESC;
        logger.info(sb);

        SearchResponse sr = sb.get();
        logger.info(sr);
    }



    /* 按照时间聚合查询的例子
    date_test_index2/my_type/_search
    GET
    {
        "aggregations": {
        "agg": {
            "date_histogram": {
                "field": "date",
                "interval": "1y",
                "min_doc_count": 1
            }
        }
    }
    }

    //上面聚合返回结果如下。聚合结果中，每个桶里面有一个key_as_string，
    //不管你存的数据的实际值是什么格式的，key_as_string是根据你指定的format的第一种格式进行格式化后的字符串表示
    "buckets": [
    {
        "key_as_string": "2000-01-01 00:00:00",
            "doc_count": 1,
            "key": 946684800000
    },
    {
        "key_as_string": "2015-01-01 00:00:00",
            "doc_count": 1,
            "key": 1420070400000
    },
    {
        "key_as_string": "2016-01-01 00:00:00",
            "doc_count": 1,
            "key": 1451606400000
    }
    ]
    */

}