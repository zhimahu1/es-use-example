package com.jd.es.example.other;

import com.jd.es.example.common.IndexDoc;
import com.jd.es.example.common.Tool;
import com.jd.es.example.query.MatchAllQuery;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Title: FilterTest 结论：没有什么不同
 * Description: FilterTest
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/10/27
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class FilterTest {

    private static Logger logger = Logger.getLogger(FilterTest.class);

    private static String this_index = "filter_test_index";
    private static String this_type = "my_type";

    private static int ThreadPoolSize = 20;
    private static ExecutorService threadPool = Executors.newFixedThreadPool(ThreadPoolSize);

    //如果不存在，先创建一个测试索引
    /*  HTTP 接口
        URL:  filter_test_index
        TYPE：PUT
        BODY:
            {
                "mappings": {
                "my_type": {
                    "properties": {
                        "des": {
                            "index": "not_analyzed",
                                    "type": "string"
                        },
                        "name": {
                            "index": "not_analyzed",
                                    "type": "string"
                        },
                        "class": {
                            "index": "not_analyzed",
                                    "type": "string"
                        }
                    }
                }
            }
            }
      */

    public static void main(String[] agrs) {

        //prepareDoc(500000);

        Tool.CLIENT.prepareSearch(this_index).setTypes(this_type).setSize(100).get();

        test2();
        test1();
        test2();
        test1();

        test2();
        test2();
        test2();

        test1();
        test1();
        test1();

        test2();
        test1();

        test2();
        test1();

        test2();
        test1();

        test2();
        test1();

        test2();
        test1();

        test2();
        test1();

        test2();
        test1();

        test2();
        test1();
    }

    private static void test1() {
        try {
            long begin = System.currentTimeMillis();
            QueryBuilder qb = QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("des", "d0"))
                    .should(QueryBuilders.termQuery("class", "c0"))
                    .minimumNumberShouldMatch(1);
            SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(this_index).setTypes(this_type).setQuery(qb).setFrom(9000).setSize(10);
            //logger.info(searchRequestBuilder);
            //SearchResponse sResponse =
            searchRequestBuilder.get();
            //logger.info(sResponse.getHits().getTotalHits());
            logger.info("term:" + (System.currentTimeMillis() - begin));
        }catch (Exception e){
            logger.error(e.getMessage(),e);
        }
    }

    private static void test2() {
        try {
            long begin = System.currentTimeMillis();
            QueryBuilder qb = QueryBuilders.boolQuery()
                    .should(QueryBuilders.boolQuery().filter((QueryBuilders.termQuery("des", "d0"))))
                    .should(QueryBuilders.boolQuery().filter((QueryBuilders.termQuery("class", "c0"))))
                    .minimumNumberShouldMatch(1);

            SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(this_index).setTypes(this_type).setQuery(qb).setFrom(9000).setSize(10);
            //logger.info(searchRequestBuilder);
            //SearchResponse sResponse =
            searchRequestBuilder.get();
            //logger.info(sResponse.getHits().getTotalHits());
            logger.info("bool filter:" + (System.currentTimeMillis() - begin));
        }catch (Exception e){
            logger.error(e.getMessage(),e);
        }
    }


    private static void prepareDoc(long docTotal) {
        long begin = System.currentTimeMillis();

        List<Future<String>> futureList = new ArrayList<>();
        for (int i = 0; i < ThreadPoolSize; i++) {
            futureList.add(threadPool.submit(new IndexDocTask((int)(docTotal/ThreadPoolSize) * i, (int)(docTotal/ThreadPoolSize))));
        }

        for (Future<String> future : futureList) {
            try {
                String result = future.get(30, TimeUnit.MINUTES);//设置超时时间
                logger.info("成功：" + result);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                future.cancel(true);
            }
        }

        logger.info("写" + docTotal + "文档总耗时：" + (System.currentTimeMillis() - begin));
    }


    //写入文档任务
    private static class IndexDocTask implements Callable<String> {

        private int docIdBegin = 0;
        private int length = 0;

        public IndexDocTask(int doIdBegin,int length){
            this.docIdBegin = doIdBegin;
            this.length = length;
        }

        @Override
        public String call() {
            long begin = System.currentTimeMillis();
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = docIdBegin; i < docIdBegin + length; i++) {
                stringBuilder.append("{\"des\":\"d")
                        .append(i%3)
                        .append("\",\"name\":\"n")
                        .append(i)
                        .append("\",\"class\":\"c")
                        .append(i%10)
                        .append("\"}");
                //logger.info(stringBuilder.toString());
                IndexDoc.indexWithStr(this_index, this_type, i + "", stringBuilder.toString());
                stringBuilder.setLength(0);
            }
            logger.info(docIdBegin + "->" + (docIdBegin + length-1) + " 写文档耗时：" + (System.currentTimeMillis() - begin));
            return docIdBegin + "->" + (docIdBegin + length-1);
        }
    }


    /*
    filter_test_index/my_type/_search
    GET
    {
        "query": {
        "bool": {
            "should": [
            {
                "bool": {
                "filter": {
                    "term": {
                        "des": "d0"
                    }
                }
            }
            },
            {
                "bool": {
                "filter": {
                    "term": {
                        "class": "c0"
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

    /*
    filter_test_index/my_type/_search
    GET
    {
        "query": {
        "bool": {
            "should": [
            {
                "term": {
                "des": "d0"
            }
            },
            {
                "term": {
                "class": "c0"
            }
            }
            ],
            "minimum_should_match": "1"
        }
    }
    }
    */

}