package com.jd.es.example.analyzer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Title: PatternAnalyzer
 * Description: PatternAnalyzer 逗号分词器，以逗号分词
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2017/5/18
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class PatternAnalyzer {

    private static Logger logger = LoggerFactory.getLogger(PatternAnalyzer.class);

    /**
    //创建索引，定义pattern分词器，指定以逗号分词
    PUT
    my_comma_analyzer_test
    {
        "settings": {
        "index": {
            "analysis": {
                "analyzer": {
                    "my_comma_analyzer": {
                        "type": "pattern",
                        "pattern": ","
                    }
                }
            }
        }
    }
    }
     */

    /**
    //测试分词效果
    GET
    my_comma_analyzer_test/_analyze?analyzer=my_comma_analyzer
    {
        "text": "小明喜欢红花,小红&小华 不喜欢 小红花,Good morning!。"
    }
    */

    //结果
    /**
    {
        "tokens": [
        {
            "end_offset": 6,
                "start_offset": 0,
                "position": 0,
                "type": "word",
                "token": "小明喜欢红花"
        },
        {
            "end_offset": 20,
                "start_offset": 7,
                "position": 101,
                "type": "word",
                "token": "小红&小华 不喜欢 小红花"
        },
        {
            "end_offset": 35,
                "start_offset": 21,
                "position": 202,
                "type": "word",
                "token": "good morning!。"
        }
        ]
    }
    */


}
