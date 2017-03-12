package org.feiware.iot.data;

import java.io.Serializable;
import java.util.Date;

/**
 * class to represent the IoT data.
 *
 * Created by Ken on 12/23/16.
 */

public class IoTData implements Serializable {

    private Date dataTime;

    private String dataId;

    private double dataValue;

    private String dataType;

    private int dataRank;

    public IoTData(Date dataTime, String dataId, double dataValue, String dataType, int dataRank) {
        super();
        this.dataTime = dataTime;
        this.dataId = dataId;
        this.dataValue = dataValue;
        this.dataType = dataType;
        this.dataRank= dataRank;
    }


    public Date getDataTime() {
        return dataTime;
    }
    public void setDataTime(Date dataTime) {
        this.dataTime = dataTime;
    }

    public String getDataId() {
        return dataId;
    }
    public void setDataId(String dataId) {
        this.dataId= dataId;
    }

    public double getDataValue() {
        return dataValue;
    }
    public void setDataValue(double dataValue) {
        this.dataValue = dataValue;
    }

    public String getDataType() {
        return dataType;
    }
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public int getDataRank() {
        return dataRank;
    }
    public void setDataRank(int dataRank) {
        this.dataRank = dataRank;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((dataTime == null) ? 0 : dataTime.hashCode());
        result = prime * result + ((dataId == null) ? 0 : dataId.hashCode());
        result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());;
        result = prime * result + dataRank;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IoTData other = (IoTData) obj;
        if (dataTime == null) {
            if (other.dataTime != null)
                return false;
        } else if (!dataTime.equals(other.dataTime))
            return false;
        if (dataId == null) {
            if (other.dataId != null)
                return false;
        } else if (!dataId.equals(other.dataId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "IoTData [dataTime=" + dataTime + ", dataId=" + dataId + ", dataValue="
                + dataValue  + ", dataType=" + dataType + ", dataRank=" + dataRank + "]";
    }
}
