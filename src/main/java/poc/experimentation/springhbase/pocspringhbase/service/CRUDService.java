package poc.experimentation.springhbase.pocspringhbase.service;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseConnection;

import java.io.IOException;


@Service
public class CRUDService {

    @Autowired
    private HBaseConnection connection;

    public void listTables() throws IOException {
        TableName table = TableName.valueOf("default", "user_audience_map");
        Connection conn = this.connection.getConnection();
        System.out.println(conn.getTable(table));
    }
}
