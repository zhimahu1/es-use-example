package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.*;

/**
 * Title: FunctionScoreQuery
 * Description: FunctionScoreQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class FunctionScoreQuery {

    private static Logger logger = Logger.getLogger(FunctionScoreQuery.class);

    public static void main(String[] args) {
        testFunctionScoreQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "country", "China");
        testFunctionScoreQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "gender", "male");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void testFunctionScoreQuery(String indexName, String typeName, String fieldName, String fieldValue) {

        QueryBuilder qb = QueryBuilders.functionScoreQuery()
                .add(
                        QueryBuilders.matchQuery(fieldName, fieldValue),
                        randomFunction("ABCDEF")
                )
                .add(exponentialDecayFunction("age", 4L, 1L));
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
