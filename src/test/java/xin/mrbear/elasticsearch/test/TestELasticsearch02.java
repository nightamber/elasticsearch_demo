package xin.mrbear.elasticsearch.test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import xin.mrbear.elasticsearch.util.QueryUtil;

public class TestELasticsearch02 {
    private TransportClient client;

    /**
     * 获得client
     * @throws UnknownHostException
     */
    @Before
    public void getClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        client = new PreBuiltTransportClient(settings)
            .addTransportAddress(new TransportAddress(InetAddress.getByName("bigdata01"), 9300));
    }

    /**
     * 关闭client
     */
    @After
    public void closeClient(){
        client.close();
    }

    /**
     * Elasticsearch以类似于REST Query DSL的方式提供完整的Java查询dsl。 查询构建器的工厂是QueryBuilders。
     */
    @Test
    public void matchAllQuery(){
        //构造查询对象
        MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();
        //搜索结果存入SearchResponse
        SearchResponse response = client.prepareSearch("index1")
            .setQuery(query)
            .setSize(3)
            .get();


        SearchHits hits = response.getHits();

        for (SearchHit hit : hits) {
            System.out.println("source:"+hit.getSourceAsString());
            System.out.println("index:"+hit.getIndex());
            System.out.println("type:"+hit.getType());
            System.out.println("id:"+hit.getId());
            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key+"="+map.get(key));

            }
            System.out.println("--------------------");
        }

    }

    /**
     * matchQuery
     */
    @Test
    public void matchQueryTest() throws UnknownHostException {
        QueryUtil util=new QueryUtil("website",5);
        //构造查询对象
        QueryBuilder qb=QueryBuilders.matchQuery(
            "title",
            "centos");
        util.query(qb).print();
    }

    /**
     * Operator
     */
    @Test
    public void operatorTest() throws UnknownHostException {
        QueryUtil util=new QueryUtil("website",5);
        //构造查询对象
        //QueryBuilder qb=QueryBuilders.matchQuery("title", "centos");
        QueryBuilder qb=QueryBuilders
            .matchQuery("title", "centos升级")
            .operator(Operator.AND);
        util.query(qb).print();
    }

    /**
     * multiMatchQuery
     */
    @Test
    public void multiMatchQueryTest() throws UnknownHostException {
        QueryUtil util=new QueryUtil("website",5);
        QueryBuilder qb=QueryBuilders.multiMatchQuery("centos","title","abstract");
        util.query(qb).print();
    }



}
