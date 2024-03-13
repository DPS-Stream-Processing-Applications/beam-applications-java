package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;

public class DbEntry implements Serializable {
    private String mgsid;
    private String trainData;
    private String rowKeyEnd;

    public String getMgsid() {
        return mgsid;
    }

    public void setMgsid(String mgsid) {
        this.mgsid = mgsid;
    }

    public String getTrainData() {
        return trainData;
    }

    public void setTrainData(String trainData) {
        this.trainData = trainData;
    }

    public String getRowKeyEnd() {
        return rowKeyEnd;
    }

    public void setRowKeyEnd(String rowKeyEnd) {
        this.rowKeyEnd = rowKeyEnd;
    }

    @Override
    public String toString() {
        return "DbEntry{"
                + "mgsid='"
                + mgsid
                + '\''
                + ", trainData='"
                + trainData
                + '\''
                + ", rowKeyEnd='"
                + rowKeyEnd
                + '\''
                + '}';
    }
}
