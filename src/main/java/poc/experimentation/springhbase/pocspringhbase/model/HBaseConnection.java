package poc.experimentation.springhbase.pocspringhbase.model;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import poc.experimentation.springhbase.pocspringhbase.constants.HBaseConstants;

import java.io.IOException;

public class HBaseConnection {

    private Connection connection;

    public Connection getConnection() {
        return connection;
    }

    public HBaseConnection(String zkHost, Integer zkPort) throws IOException {
        this.connection = getHBaseConnection(zkHost, zkPort);
    }

    public synchronized Connection getHBaseConnection(String zkHost, Integer zkPort) throws IOException {
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

    public void destroy() {
        try{
            this.connection.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
