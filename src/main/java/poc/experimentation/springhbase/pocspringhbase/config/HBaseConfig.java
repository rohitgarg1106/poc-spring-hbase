package poc.experimentation.springhbase.pocspringhbase.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import poc.experimentation.springhbase.pocspringhbase.constants.HBaseConstants;

import java.io.IOException;

@Configuration
public class HBaseConfig {

    @Value("${hbase.zookeeper.quorum}")
    private String zkHost;

    @Value("${hbase.zookeeper.property.clientPort}")
    private Integer zkPort;

    @Bean(HBaseConstants.HBASE_CONNECTION_BEAN_QUALIFIER)
    public synchronized Connection getHBaseConnection() throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set(HBaseConstants.ZK_HOST, zkHost);
        config.set(HBaseConstants.ZK_PORT, zkPort.toString());
        config.set("hbase.client.pause","1000");
        config.set("hbase.client.retries.number","3");
        config.set("zookeeper.recovery.retry","1");
        config.set("hbase.rpc.timeout","10000");
        config.set("hbase.client.scanner.timeout.period","10000");
        return ConnectionFactory.createConnection(config);
    }


}
