package poc.experimentation.springhbase.pocspringhbase.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import poc.experimentation.springhbase.pocspringhbase.constants.HBaseConstants;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseConnection;

import java.io.IOException;

@Configuration
public class HBaseConfig {

    @Value("${hbase.zookeeper.quorum}")
    private String zkHost;

    @Value("${hbase.zookeeper.property.clientPort}")
    private Integer zkPort;

    @Bean(name = {"fetchConnection","HBaseConnection"}, destroyMethod="destroy")
    public synchronized HBaseConnection fetchConnection() throws IOException {
        return new HBaseConnection(zkHost, zkPort);
    }


}
