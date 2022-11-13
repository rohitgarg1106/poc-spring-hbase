package poc.experimentation.springhbase.pocspringhbase.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class HBaseRow {

    private byte[] rowKeyBytes;

    private List<HBaseColumn> columns;


    @JsonIgnore
    public HBaseRow() {
        this.columns = new ArrayList<>();
    }
}
