package poc.experimentation.springhbase.pocspringhbase.ab;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
@AllArgsConstructor
public class AudienceEntityMapData {
    private Integer audienceId;
    private Long userId;
    private String entityId;
}
