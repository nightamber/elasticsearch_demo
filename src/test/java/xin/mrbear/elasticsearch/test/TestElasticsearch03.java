package xin.mrbear.elasticsearch.test;

import java.net.UnknownHostException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import xin.mrbear.elasticsearch.util.QueryUtil;

public class TestElasticsearch03 {

    @Test
    public void termQuery() throws UnknownHostException {
        QueryUtil util=
            new QueryUtil("website",5);
        //构造查询对象
        QueryBuilder qb= QueryBuilders.termQuery("title","vmware");
        util.query(qb).print();
    }


    @Test
    public void rangeQuery() throws UnknownHostException {
        QueryUtil util=new QueryUtil("website",5);
        //构造查询对象
        QueryBuilder qb=QueryBuilders.rangeQuery("postdate").from("2017-01-01").to("2017-12-31").format("yyyy-MM-dd");
        util.query(qb).print();
    }


    @Test
    public void prefixQuery() throws UnknownHostException {
        QueryUtil util=new QueryUtil("my-index",5);
        //构造查询对象
        QueryBuilder qb=QueryBuilders.prefixQuery("name","程");
        util.query(qb).print();
    }

    @Test
    public void wildcardQuery() throws UnknownHostException {
        QueryUtil util=new QueryUtil("website",5);
        //构造查询对象
        QueryBuilder qb=QueryBuilders.wildcardQuery("title","*yum*");
        util.query(qb).print();
    }

    @Test
    public void regexpQuery() throws UnknownHostException {
        QueryUtil util=new QueryUtil("website",5);
        //构造查询对象
        QueryBuilder qb=QueryBuilders.regexpQuery("title","gc.*");
        util.query(qb).print();
    }

    

}
