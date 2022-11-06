package poc.experimentation.springhbase.pocspringhbase.repository;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.stereotype.Repository;
import poc.experimentation.springhbase.pocspringhbase.request.PutDataRequest;

import java.io.IOException;
import java.util.List;

@Repository
public interface CRUDRepository {

    public List<String> listTables() throws IOException ;

    public void createDefaultTable()  throws IOException ;

    public void addData(PutDataRequest request)  throws IOException ;


}
