package poc.experimentation.springhbase.pocspringhbase.ab;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreateAudienceEntityRequest {

    @JsonProperty("audience_id")
    private Integer audienceId;

    @JsonProperty("entity_type")
    private String entityType;

    @JsonProperty("iso_country_code")
    private String country;

    @JsonProperty("entity_ids")
    private List<String> entityIds;
}
