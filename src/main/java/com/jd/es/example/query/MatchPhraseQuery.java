package com.jd.es.example.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Title: MatchPhraseQuery
 * Description: MatchPhraseQuery 短语匹配
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2017/1/16
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class MatchPhraseQuery {

    private static Logger logger = LoggerFactory.getLogger(MatchPhraseQuery.class);


    //http://106.186.120.253/preview/_phrase_search.html

    /*
    找出一个属性中的独立单词是没有问题的，但有时候想要精确匹配一系列单词或者短语 。 比如， 我们想执行这样一个查询，仅匹配同时包含 “rock” 和 “climbing” ，并且 二者以短语 “rock climbing” 的形式紧挨着的雇员记录。

    为此对 match 查询稍作调整，使用一个叫做 match_phrase 的查询：

    GET /megacorp/employee/_search
    {
        "query" : {
        "match_phrase" : {
            "about" : "rock climbing"
        }
    }
    }
    */


}
