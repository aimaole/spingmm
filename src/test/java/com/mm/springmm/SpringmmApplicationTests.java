package com.mm.springmm;

import com.alibaba.fastjson.JSON;
import com.mm.springmm.bean.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.ml.UpdateDatafeedRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class SpringmmApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    void testCreateIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("test1mm");
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);

    }

    @Test
    void testExitIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("test1mm");
        boolean exists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);

    }

    @Test
    void testDelIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("test1mm");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());

    }


    @Test
    void testAddDoc() throws IOException {
        User user = new User("test", 12);
        IndexRequest test1mm = new IndexRequest("test1mm");
        test1mm.id("1");
        test1mm.timeout(TimeValue.timeValueSeconds(1));
        test1mm.timeout("1s");

        test1mm.source(JSON.toJSONString(user), XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(test1mm, RequestOptions.DEFAULT);
        System.out.println(index.toString());


    }

    @Test
    void testGetDoc() throws IOException {
        GetRequest test1mm = new GetRequest("test1mm", "1");
        //不获取上下文
//        test1mm.fetchSourceContext(new FetchSourceContext(false));
//        test1mm.storedFields("_none_");
//        boolean exists = restHighLevelClient.exists(test1mm, RequestOptions.DEFAULT);
//        System.out.println(exists);
        GetResponse getResponse = restHighLevelClient.get(test1mm, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());
        System.out.println(getResponse);
    }

    @Test
    void testUpdateDoc() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("test1mm", "1");
        updateRequest.timeout("1s");
        User asdadada = new User("asdadada", 21);
        updateRequest.doc(JSON.toJSONString(asdadada), XContentType.JSON);


        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(update);
    }

    @Test
    void testDelDoc() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("test1mm", "1");
        deleteRequest.timeout("1s");
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }

    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("1s");
        ArrayList<User> users = new ArrayList<>();
        users.add(new User("dad", 45));
        users.add(new User("daahsdd", 47));
        users.add(new User("dadjkadad", 12));
        users.add(new User("dadjkhjkda", 69));
        users.add(new User("dsjkhdad", 23));
        users.add(new User("daxd", 78));
        users.add(new User("daaasajhkhxd", 32));
        users.add(new User("dadbcad", 96));
        users.add(new User("dadbczxc", 455));
        users.add(new User("cvb", 69));
        for (int i = 0; i < users.size(); i++) {
            bulkRequest.add(new IndexRequest("test1mm").id("" + (i + 2)).source(JSON.toJSONString(users.get(i)), XContentType.JSON));
        }
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.status());

    }


    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("test1mm");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "dad");
        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(search.getHits()));

    }
}
