package poc.experimentation.springhbase.pocspringhbase.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import poc.experimentation.springhbase.pocspringhbase.exception.HBaseTableExistsException;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseData;
import poc.experimentation.springhbase.pocspringhbase.repository.HBaseCrudRepository;
import poc.experimentation.springhbase.pocspringhbase.request.CreateMapStoreRequest;
import poc.experimentation.springhbase.pocspringhbase.request.GetDataRequest;
import poc.experimentation.springhbase.pocspringhbase.request.PutDataRequest;

import java.io.IOException;
import java.util.List;


@Service
public class HBaseCrudService {

    @Autowired
    private HBaseCrudRepository repository;

    @Autowired
    private ObjectMapper mapper;

    public List<String> listTables(String namespace) throws IOException {
       return repository.listTableNames(namespace);
    }

    public void createTable(CreateMapStoreRequest request) throws IOException, HBaseTableExistsException {

        repository.createTable(request.getNamespace(), request.getTableName(), request.getCf());
    }

    public void addData(PutDataRequest request) throws IOException {
        HBaseData data = request.getHBaseData();
        repository.addData(data);
    }


    public Object getData(GetDataRequest request) throws IOException, ClassNotFoundException {
        HBaseData hbaseData = request.getHBaseData();
        byte[] byteArray = repository.getData(hbaseData);
        return mapper.readValue(byteArray, Object.class);
    }
}