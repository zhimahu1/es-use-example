package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.util.Map;

/**
 * Title: StructuringAggregations
 * Description: StructuringAggregations   https://www.elastic.co/guide/en/elasticsearch/client/java-api/2.1/_structuring_aggregations.html
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class StructuringAggregations {

    public static void main(String[] args){
        structuringAggregations(Tool.INDEX_NAME, Tool.TYPE_NAME);
    }

    /*
    {
  "size": 0,
  "aggs": {
    "by_country": {
      "terms": {
        "field": "country"
      },
      "aggs": {
        "by_year": {
          "date_histogram": {
            "field": "dateOfBirth",
            "interval": "year",
            "format": "yyyy-MM-dd"
          }
        },
        "avg_age": {
          "avg": {
            "field": "age"
          }
        }
      }
    }
  }
}
     */



    /**
     * 聚合查询
     * @param indexName
     * @param typeName
     * @param termName
     * @param termValue
     * @param sortField
     * @param highlightField
     */
    private static void structuringAggregations(String indexName, String typeName) {
        //search result get source

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName)
                .addAggregation(
                        AggregationBuilders.terms("by_country").field("country")
                                .subAggregation(AggregationBuilders.dateHistogram("by_year")
                                                .field("dateOfBirth")
                                                .interval(DateHistogramInterval.YEAR)
                                                .subAggregation(AggregationBuilders.avg("avg_age").field("age"))
                                )
                )
                .execute().actionGet();

        System.out.print(sResponse);

        Map<String,Aggregation> maps = sResponse.getAggregations().getAsMap();

        int tShards = sResponse.getTotalShards();
        long timeCost = sResponse.getTookInMillis();
        int sShards = sResponse.getSuccessfulShards();
//		System.out.println(tShards+","+timeCost+","+sShards);
        SearchHits hits = sResponse.getHits();
        long count = hits.getTotalHits();
        SearchHit[] hitArray = hits.getHits();
        for(int i = 0; i < count; i++) {
            System.out.println("==================================");
            SearchHit hit = hitArray[i];
            Map<String, Object> fields = hit.getSource();
            for(String key : fields.keySet()) {
                System.out.println(key);
                System.out.println(fields.get(key));
            }
        }
    }

}
