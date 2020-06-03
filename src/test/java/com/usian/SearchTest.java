package com.usian;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {ElasticsearchApp.class})
public class SearchTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    private SearchRequest searchRequest;
    private SearchResponse searchResponse;

    @Before
    public void initSearchRequest(){
        // 搜索请求对象
        searchRequest = new SearchRequest("java1906");
        searchRequest.types("course");
    }

    //简单查询
    @Test
    public void getDoc() throws IOException {
        GetRequest getRequest = new GetRequest("java1906","course","1");
        GetResponse getResponse = restHighLevelClient.get(getRequest);
        boolean exists = getResponse.isExists();
        System.out.println(exists);
        String sourceAsString = getResponse.getSourceAsString();
        System.out.println(sourceAsString);
    }

    //
    @Test
    public void IndexSearchTest() throws IOException {

        // 搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        // 设置搜索源
        searchRequest.source(searchSourceBuilder);

        // 执行搜索
        searchResponse =
                restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        displayHits(searchResponse);

    }

    private void displayHits(SearchResponse searchResponse) {
        // 搜索匹配结果
        SearchHits hits = searchResponse.getHits();
        // 搜索总记录数
        long totalHits = hits.getTotalHits();
        System.out.println("共搜索到" + totalHits + "条文档");
        // 匹配的文档
        SearchHit[] hits1 = hits.getHits();
        for (int i = 0; i < hits1.length; i++) {
            SearchHit documentFields = hits1[i];
            // 文档id
            String id = documentFields.getId();
            System.out.println(id);
            // 源文档内容
            String sourceAsString = documentFields.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }

    @Test
    public void testSearchPage() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from(1);
        searchSourceBuilder.size(2);
        searchSourceBuilder.sort("price", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        searchResponse =
                restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        displayHits(searchResponse);
    }

    @Test
    public void testMatchQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name","spring开发").operator(Operator.AND));
        searchRequest.source(searchSourceBuilder);
        searchResponse =
                restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        displayHits(searchResponse);
    }

    @Test
    public void testMultiMatchQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("开发",new String[] {"name","description"}));
        searchRequest.source(searchSourceBuilder);
        searchResponse =
                restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        displayHits(searchResponse);
    }

    @Test
    public void testBooleanQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name","开发"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("description","开发"));
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        searchResponse =
                restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        displayHits(searchResponse);
    }

    @Test
    public void testFilterQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name","开发"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(50).lte(100));
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        searchResponse =
                restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        displayHits(searchResponse);
    }



}
