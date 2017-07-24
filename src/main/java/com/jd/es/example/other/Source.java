package com.jd.es.example.other;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: Source 参考：http://es.xiaoleilu.com/070_Index_Mgmt/31_Metadata_source.html
 * 默认情况下，Elasticsearch 用 JSON 字符串来表示文档主体保存在 _source 字段中。像其他保存的字段一样，
 * _source 字段也会在写入硬盘前压缩。
 * 这几乎始终是需要的功能，因为：
    1.搜索结果中能得到完整的文档 —— 不需要额外去别的数据源中查询文档
    2.如果缺少 _source 字段，部分 更新 请求不会起作用
    3.当你的映射有变化，而且你需要重新索引数据时，你可以直接在 Elasticsearch 中操作而不需要重新从别的数据源中取回数据。
    4.你可以从 _source 中通过 get 或 search 请求取回部分字段，而不是整个文档。
    5.这样更容易排查错误，因为你可以准确的看到每个文档中包含的内容，而不是只能从一堆 ID 中猜测他们的内容。
 * 即便如此，存储 _source 字段还是要占用硬盘空间的。假如上面的理由对你来说不重要，你可以用下面的映射禁用 _source 字段：
 * PUT /my_index
     {
         "mappings": {
             "my_type": {
                 "_source": {
                     "enabled": false
                 }
             }
         }
     }

 在搜索请求中你可以通过限定 _source 字段来请求指定字段：
     GET /_search
     {
     "query":   { "match_all": {}},
     "_source": [ "title", "created" ]
     }
 这些字段会从 _source 中提取出来，而不是返回整个 _source 字段。

 * Description: Source
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/10/26
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class Source {

    private static Logger logger = Logger.getLogger(Source.class);

    public static void main(String[] agrs){
        testSource();
    }

    /*  HTTP 接口
        URL:  agg_index/agg_type/_search
        TYPE：GET
        BODY:
            {
                "query": {
                "match_all": {}
                },
                "_source": [
                "name",
                 "country"
                ]
            }
    */
    public static void testSource(){
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(Tool.INDEX_NAME).setTypes(Tool.TYPE_NAME).setQuery(qb);
        searchRequestBuilder.setFetchSource("name",null);//_source 中只包含name
        //searchRequestBuilder.setFetchSource(new String[]{"name","country"},null);//设置多个filed
        logger.info(searchRequestBuilder);
        SearchResponse sResponse = searchRequestBuilder.get();
        logger.info(sResponse);
    }

}

