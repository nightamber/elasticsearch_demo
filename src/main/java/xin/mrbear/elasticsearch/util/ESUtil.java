package xin.mrbear.elasticsearch.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ESUtil {

    private static TransportClient client;



    public static TransportClient getClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        client = new PreBuiltTransportClient(settings)
            .addTransportAddress(new TransportAddress(InetAddress.getByName("bigdata01"), 9300));

        return client;
    }
}
