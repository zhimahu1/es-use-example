package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

/**
 * Title: TermsQuery
 * Description: TermsQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class TermsQuery {

    private static Logger logger = Logger.getLogger(TermsQuery.class);

    public static void main(String[] args) {
        testTermsQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "name", "c1", "c2");
        testTermsQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "name", "c1", "c2", "a5");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                  "terms": {
                    "name": [
                      "c1",
                      "a5"
                    ]
                  }
              }
            }
     */
    private static void testTermsQuery(String indexName, String typeName, String fieldName,String... fieldValues) {

        QueryBuilder qb = QueryBuilders.termsQuery(fieldName, fieldValues);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }



/**
    {
        "from": 0,
            "size": 100,
            "query": {
        "bool": {
            "should": [
            {
                "bool": {
                "filter": {
                    "term": {
                        "name": "c1"
                    }
                }
            }
            },
            {
                "bool": {
                "filter": {
                    "term": {
                        "name": "c2"
                    }
                }
            }
            },
            {
                "bool": {
                "filter": {
                    "term": {
                        "name": "a6"
                    }
                }
            }
            }
            ],
            "minimum_should_match": "1"
        }
    }
    }
*/





}