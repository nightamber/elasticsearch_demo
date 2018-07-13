package xin.mrbear.elasticsearch.test;





import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import javax.annotation.Resource;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestElasticsearch {
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

//        GetResponse response = client.prepareGet("blog01", "article", "1").execute()
//            .actionGet();
//        System.out.println(response);
//        client.close();
    }

    /**
     * 关闭client
     */
    @After
    public void closeClient(){
        client.close();
    }


    //创建文档 自动创建索引

    /**
     * 拼装json的字符串
     */
    @Test
    public void createIndexNoMapping(){
        String json = "{" +
            "\"id\":\"1\"," +
            "\"title\":\"基于Lucene的搜索服务器\"," +
            "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"" +
            "}";
        /**
         * 创建文档，自动创建索引，自动创建索引
         */
        IndexResponse indexResponse = this.client.prepareIndex("blog", "article", "1")
            .setSource(json,XContentType.JSON).get();
        printResult(indexResponse);


    }

    private void printResult(IndexResponse indexResponse) {
        //获取结果
        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String id = indexResponse.getId();

        long version = indexResponse.getVersion();
        boolean forcedRefresh = indexResponse.forcedRefresh();

        System.out.println(index + " : " + type + ": " + id + ": " + version + ": " + forcedRefresh+": "+indexResponse.status());
    }

    @Test
    public void createIndexNoMapping2() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field("id", "2")
            .field("title", "Java设计模式之单例模式")
            .field("content", "枚举单例模式可以防反射攻击。")
            .endObject();
        System.out.println(xContentBuilder.toString());
        IndexResponse indexResponse = client.prepareIndex("blog3", "article", "4")
            .setSource(xContentBuilder).get();
        printResult(indexResponse);

    }

    /**
     * 查看索引是否存在
     */
    @Test
    public void isExist(){
        //获取IndicesAdminClient对象
        IndicesAdminClient indicesAdminClient = this.client.admin().indices();
        //判断索引是否存在
        IndicesExistsResponse response = indicesAdminClient.prepareExists("blog01").get();
        System.out.println(response.isExists());

    }

    /**
     * 获取文档
     */
    @Test
    public void getDoc(){
        GetResponse response = client.prepareGet("blog", "article", "1").get();
        System.out.println(response.isExists());
        System.out.println(response.getType());
        System.out.println(response.getIndex());
        System.out.println(response.getId());
        System.out.println(response.getVersion());
    }

    /**
     * 文档删除
     */
    @Test
    public void deleteDocTest(){
        DeleteResponse response = this.client.prepareDelete("blog3", "article", "4").get();
        //删除成功返回OK，否则返回NOT_FOUND
        System.out.println(response.status());
        //返回被删除文档的类型
        System.out.println(response.getType());
        //返回被删除文档的ID
        System.out.println(response.getId());
        //返回被删除文档的版本信息
        System.out.println(response.getVersion());
    }

    /**
     * 文档更新
     */
    @Test
    public void updateDocTest() throws IOException, ExecutionException, InterruptedException {
        UpdateRequest request = new UpdateRequest();
        request.index("blog01")
            .type("article")
            .id("1")
            .doc(
                XContentFactory.jsonBuilder().startObject()
                .field("tilte","一拳超人")
                .endObject()
            );
        UpdateResponse response = client.update(request).get();
        //更新成功返回OK，否则返回NOT_FOUND
        System.out.println(response.status());
        //返回被更新文档的类型
        System.out.println(response.getType());
        //返回被更新文档的ID
        System.out.println(response.getId());
        //返回被更新文档的版本信息
        System.out.println(response.getVersion());
    }

    @Test
    public void upsetDocTest() throws IOException, ExecutionException, InterruptedException {
        IndexRequest request1 =new IndexRequest("index1","blog","1")
            .source(
                XContentFactory.jsonBuilder().startObject()
                    .field("id","1")
                    .field("title","装饰模式")
                    .field("content","动态地扩展一个对象的功能")
                    .field("postdate","2018-02-03 14:38:10")
                    .field("url","csdn.net/79239072")
                    .endObject()
            );
        UpdateRequest request2=new UpdateRequest("index1","blog","1")
            .doc(
                XContentFactory.jsonBuilder().startObject()
                    .field("title","装饰模式解读")
                    .endObject()
            ).upsert(request1);
        UpdateResponse response=client.update(request2).get();
        //upsert操作成功返回OK，否则返回NOT_FOUND
        System.out.println(response.status());
        //返回被操作文档的类型
        System.out.println(response.getType());
        //返回被操作文档的ID
        System.out.println(response.getId());
        //返回被操作文档的版本信息
        System.out.println(response.getVersion());
    }




}
