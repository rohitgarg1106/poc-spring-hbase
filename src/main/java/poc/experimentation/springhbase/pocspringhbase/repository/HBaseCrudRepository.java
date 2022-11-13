package poc.experimentation.springhbase.pocspringhbase.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import poc.experimentation.springhbase.pocspringhbase.ab.HbaseUtils;
import poc.experimentation.springhbase.pocspringhbase.exception.HBaseTableExistsException;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseConnection;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseData;

import java.io.IOException;
import java.util.List;

@Repository
@Slf4j
public class HBaseCrudRepository {

    @Autowired
    private HBaseConnection connection;

    @Autowired
    private ObjectMapper mapper;

    public void createTable(String namespace, String tableName, List<String> cf) throws IOException, HBaseTableExistsException {
        connection.createTable(namespace, tableName, cf);
    }

    public List<String> listTableNames(String namespace) throws IOException {
        return connection.listTableNames(namespace);
    }

    public void addData(HBaseData data) throws IOException {
        connection.putData(data);
    }

    public byte[] getData(HBaseData hbaseData) throws IOException {
//        return connection.scanTable("ab","audience_user_map", 1).get(0).getValue(Bytes.toBytes("data"), Bytes.toBytes("country"));
        return connection.getData(hbaseData);
    }

    public Result getRow(String namespace, String table, String row) throws IOException {
        return connection.getRow(namespace, table, row);
    }

    public List<Result> scanTable(String namespace, String tableName, int limit, Filter filter) throws IOException {
        return connection.scanTable(namespace, tableName,limit,null,null,filter);
    }
}


