package poc.experimentation.springhbase.pocspringhbase.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseRow;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
public class BulkPutDto{

    private String namespace;
    private String tableName;
    List<HBaseRow> rows;

    public BulkPutDto(String namespace, String tableName) {
        this.namespace = namespace;
        this.tableName = tableName;
        rows = new ArrayList<>();
    }
}
