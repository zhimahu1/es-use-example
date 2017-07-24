package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: HasParentQuery
 * Description: HasParentQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/13
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class HasParentQuery {

    private static Logger logger = Logger.getLogger(HasParentQuery.class);

    public static void main(String[] args) {
        testHasParentQuery(Tool.INDEX_NAME, Tool.TYPE_NAME);
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void testHasParentQuery(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.hasParentQuery(
                "blog",
                QueryBuilders.termQuery("tag", "something")
        );
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
