package com.jd.es.example.query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Title: QueryStringQuery
 * Description: QueryStringQuery    https://www.elastic.co/guide/en/elasticsearch/reference/2.1/query-dsl-query-string-query.html
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/11/23
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */public class QueryStringQuery {

    private static Logger logger = LoggerFactory.getLogger(QueryStringQuery.class);

    public static void main(String[] args){

    }



    /*
    //agg_index/agg_type/_search
        {
      "query": {
        "query_string": {
          "default_field": "name",
          "query": "c* OR a8"
        }
        }
        }
     */
}
