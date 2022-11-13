package poc.experimentation.springhbase.pocspringhbase.ab;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EntityAudienceMap {

    private EntityAudienceMapData entityAudienceMapData;
    private CountryData country;

}