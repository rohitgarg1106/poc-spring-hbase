package poc.experimentation.springhbase.pocspringhbase.ab;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.shaded.org.apache.commons.codec.digest.DigestUtils;
import poc.experimentation.springhbase.pocspringhbase.constants.HBaseConstants;

import java.util.Arrays;
import java.util.List;

public class HbaseUtils {

    public static String getAudienceUserMapRowKey(String audienceId, String entityId) {
        return String.format(
                "%s_%s",
                getSha1HexString(audienceId),
                entityId
        );
    }

    public static String getSha1HexString(String value) {
        return DigestUtils.sha1Hex(value);
    }

    public static byte[] getUserAudienceRowKey(Integer audienceId, Long userId) {
        return String.format("%s_%s",getSha1HexString(userId.toString()),audienceId.toString()).getBytes();
    }

    public static byte[] getEntityAudienceRowKey(Integer audienceId, String entityId, String entityType) {
        return String.format("%s_%s",getSha1HexString(entityType+"_"+entityId),audienceId.toString()).getBytes();
    }

    public static byte[] getAudienceEntityRowKey(String audienceId, String entityId) {

        return String.format(
                "%s_%s",
                getSha1HexString(audienceId),
                entityId
        ).getBytes();
    }

    public static byte[] calculateTheClosestNextRowKeyForPrefix(byte[] rowKeyPrefix) {
        int offset;
        for(offset = rowKeyPrefix.length; offset > 0 && rowKeyPrefix[offset - 1] == -1; --offset) {
            ;
        }

        if (offset == 0) {
            return HConstants.EMPTY_END_ROW;
        } else {
            byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
            ++newStopRow[newStopRow.length - 1];
            return newStopRow;
        }
    }

    public static String getUserAudienceRowKeyInString(Integer audienceId, Long userId) {
        return String.format("%s_%s",getSha1HexString(userId.toString()),audienceId.toString());
    }

    public static String getEntityAudienceRowKeyInString(Integer audienceId, String entityId, String entityType) {
        return String.format("%s_%s",getSha1HexString(entityType+"_"+entityId),audienceId.toString());
    }

    public static String getLastRowKey(List<EntityAudienceMapData> entityAudienceMapDataList, String entityType) {
        EntityAudienceMapData map = entityAudienceMapDataList.get(entityAudienceMapDataList.size()-1);
        if(entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)){
            return getUserAudienceRowKeyInString(map.getAudienceId(), map.getUserId());
        }
        else{
            return getEntityAudienceRowKeyInString(map.getAudienceId(), map.getEntityId(), entityType);
        }

    }

    public static String getLastRowKeyOld(List<EntityAudienceMapData> entityAudienceMapDataList, String entityType) {
        EntityAudienceMapData map = entityAudienceMapDataList.get(entityAudienceMapDataList.size()-1);
        if(entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)){
            return getUserAudienceRowKey(map.getAudienceId(), map.getUserId()).toString();
        }
        else{
            return getEntityAudienceRowKey(map.getAudienceId(), map.getEntityId(), entityType).toString();
        }

    }

    public static String getAudienceEntityLastRowKey(List<AudienceEntityMapData> audienceEntityMapDataList, String entityType) {
        AudienceEntityMapData map = audienceEntityMapDataList.get(audienceEntityMapDataList.size()-1);
        if(entityType.equalsIgnoreCase(HBaseConstants.DEFAULT_ENTITY_TYPE)){
            return getAudienceUserMapRowKey(map.getAudienceId().toString(), map.getUserId().toString());
        }
        else{
            return getAudienceUserMapRowKey(map.getAudienceId().toString(), map.getEntityId());
        }

    }
}
