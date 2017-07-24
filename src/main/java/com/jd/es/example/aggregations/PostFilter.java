package com.jd.es.example.aggregations;

/**
 * Title: PostFilter
 * Description: PostFilter
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/13
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class PostFilter {


    /**TODO  https://www.elastic.co/guide/en/elasticsearch/guide/current/_recap.html
     * A non-scoring query inside a filter clause affects both search results and aggregations.
     A filter bucket affects just aggregations.
     A post_filter affects just search results.
     */


    /**
    GET /cars/transactions/_search
    {
        "size" : 0,
            "query": {
        "match": {
            "make": "ford"
        }
    },
        "post_filter": {
        "term" : {
            "color" : "green"
        }
    },
        "aggs" : {
        "all_colors": {
            "terms" : { "field" : "color" }
        }
    }
    }

     */

}
