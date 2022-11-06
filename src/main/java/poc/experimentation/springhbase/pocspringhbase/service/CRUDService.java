package poc.experimentation.springhbase.pocspringhbase.service;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseConnection;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;


@Service
public class CRUDService {

    @Autowired
    private HBaseConnection connection;

    public List<String> listTables() throws IOException {
        TableName table = TableName.valueOf("default", "user_audience_map");
        Connection conn = this.connection.getConnection();
        Admin admin = conn.getAdmin();
        List<TableDescriptor> tableDescriptorList = admin.listTableDescriptors();
        List <String> tableList = tableDescriptorList.stream().map(td ->td.getTableName().getNameAsString()).collect(Collectors.toList());
        return tableList;
    }

    public void createDefaultTable() throws IOException {
        Connection conn = this.connection.getConnection();
        Admin admin =  conn.getAdmin();
        TableName table = TableName.valueOf("default", "table1");
        String family1 = "family1";
        String family2 = "family2";

        HTableDescriptor desc = new HTableDescriptor(table);
        desc.addFamily(new HColumnDescriptor(family1));
        desc.addFamily(new HColumnDescriptor(family2));
        admin.createTable(desc);

    }
}
