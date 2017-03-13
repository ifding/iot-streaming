package org.ifding.iot.data;

import java.io.Serializable;

/**
 * class to represent the IoT data Key.
 *
 * Created by ifding on 12/28/16.
 */

public class IoTDataKey implements Serializable {

    private String dataType;

    private double dataValue;

    public IoTDataKey(String dataType, double dataValue) {
        super();
        this.dataType = dataType;
        this.dataValue = dataValue;
    }

    public String getDataType() {
        return dataType;
    }
    public void setDataId(String dataType) {
        this.dataType= dataType;
    }

    public double getDataValue() {
        return dataValue;
    }
    public void setDataValue(double dataValue) {
        this.dataValue = dataValue;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
        result = prime * result + (int)dataValue;
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
        IoTDataKey other = (IoTDataKey) obj;
        if (dataType == null) {
            if (other.dataType != null)
                return false;
        } else if (!dataType.equals(other.dataType))
            return false;
        if (Double.compare(dataValue,other.dataValue) != 0) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "dataType='" + dataType + '\'' + ", dataValue='" + dataValue + '\'';
    }
}
