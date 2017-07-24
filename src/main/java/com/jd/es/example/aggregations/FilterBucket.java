package com.jd.es.example.aggregations;

/**
 * Title: FilterBucket
 * Description: FilterBucket
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/13
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class FilterBucket {

    /**
    GET /cars/transactions/_search
    {
        "size" : 0,
            "query":{
        "match": {
            "make": "ford"
        }
    },
        "aggs":{
        "recent_sales": {
            "filter": {
                "range": {
                    "sold": {
                        "from": "now-1M"
                    }
                }
            },
            "aggs": {
                "average_price":{
                    "avg": {
                        "field": "price"   //This avg metric will therefore average only docs that are both ford and sold in the last month.


     }
                }
            }
        }
    }
    }
     */


}
