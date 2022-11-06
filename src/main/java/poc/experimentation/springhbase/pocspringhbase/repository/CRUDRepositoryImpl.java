package poc.experimentation.springhbase.pocspringhbase.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseConnection;
import poc.experimentation.springhbase.pocspringhbase.request.PutDataRequest;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Repository
public class CRUDRepositoryImpl implements CRUDRepository{

    @Autowired
    private HBaseConnection connection;

    @Autowired
    private ObjectMapper mapper;

    private Table getTable(Connection conn, String namespace, String tableName) throws IOException {
        TableName table = TableName.valueOf(namespace, tableName);
        return conn.getTable(table);

    }

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

    public void addData(PutDataRequest request) throws IOException {
        Connection conn = this.connection.getConnection();
        byte[] rowKeyBytes = Bytes.toBytes(request.getRow());
        byte[] objectBytes = mapper.writeValueAsBytes(request.getData());
        byte[] familyBytes = request.getColumnFamily().getBytes();
        byte[] qualifierBytes = request.getColumnQualifier().getBytes();

        Put putOp = new Put(rowKeyBytes);
        putOp.addColumn(familyBytes, qualifierBytes, objectBytes);

        //perform put on table
        Table table = getTable(conn, request.getNamespace(), request.getTableName());
        table.put(putOp);
    }
}


