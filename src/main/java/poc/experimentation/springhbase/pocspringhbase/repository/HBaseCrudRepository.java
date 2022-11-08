package poc.experimentation.springhbase.pocspringhbase.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import poc.experimentation.springhbase.pocspringhbase.exception.HBaseTableExistsException;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseConnection;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseData;

import java.io.IOException;
import java.util.List;

@Repository
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
        return connection.getData(hbaseData);
    }
}


