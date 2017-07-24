package com.jd.es.example.query;

/**
 * Title: TermAndMatchCompare
 * Description: TermAndMatchCompare
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class TermAndMatchCompare {

    //文档地址
    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/2.1/query-dsl-term-query.html
     * https://www.elastic.co/guide/en/elasticsearch/reference/2.1/query-dsl-match-query.html
     */

    //String fields can be analyzed, or not_analyzed. By default, however, string fields are analyzed.
    //分词器会将字符串进行分词，根据分词结果创建倒排索引
    //For instance, the standard analyzer would turn the string “Quick Brown Fox!” into the terms [quick, brown, fox].
    //This analysis process makes it possible to search for individual words within a big block of full text.
    //The term query looks for the exact term in the field’s inverted index — it doesn’t know anything about the field’s analyzer. This makes it useful for looking up values in not_analyzed string fields, or in numeric or date fields.
    //When querying full text fields, use the match query instead, which understands how the field has been analyzed.


    //TODO ???验证
    //如果一个字符串被定义为analyzed（默认），那么它会被进行分词放入倒排索引。"good man"被分成"good","man",没有"good man"这个倒排索引
    //如果not_analyzed，那么"good man"作为一个整体放入倒排索引

    //match会对要匹配的内容进行分词，然后分别匹配这些分词
    //term将要匹配的内容作为一个整体进行匹配

    //例子

    /** //创建索引
        PUT my_index
        {
            "mappings": {
            "my_type": {
                "properties": {
                    "full_text": {
                        "type":  "string"
                    },
                    "exact_value": {
                        "type":  "string",
                                "index": "not_analyzed"
                    }
                }
            }
        }
        }
     */


    /**
        //插入文档
        PUT my_index/my_type/1
        {
            "full_text":   "Quick Foxes!",
                "exact_value": "Quick Foxes!"
        }
     */

    //The full_text field is analyzed by default.
    //The exact_value field is set to be not_analyzed.
    //The full_text inverted index will contain the terms: [quick, foxes].
    //The exact_value inverted index will contain the exact term: [Quick Foxes!].


/**
     //匹配
    //This query matches because the exact_value field contains the exact term Quick Foxes!.
    GET my_index/my_type/_search
    {
        "query": {
        "term": {
            "exact_value": "Quick Foxes!"
        }
    }
    }
*/


 /**
    //不匹配
    //This query does not match, because the full_text field only contains the terms quick and foxes. It does not contain the exact term Quick Foxes!.
    GET my_index/my_type/_search
    {
        "query": {
        "term": {
            "full_text": "Quick Foxes!"
        }
    }
    }
*/


/**    //匹配
    //A term query for the term foxes matches the full_text field.
    GET my_index/my_type/_search
    {
        "query": {
        "term": {
            "full_text": "foxes"
        }
    }
    }
 */


/**
    //匹配
    //This match query on the full_text field first analyzes the query string, then looks for documents containing quick or foxes or both.
    GET my_index/my_type/_search
    {
        "query": {
        "match": {
            "full_text": "Quick Foxes!"
        }
    }
    }
 */

}
