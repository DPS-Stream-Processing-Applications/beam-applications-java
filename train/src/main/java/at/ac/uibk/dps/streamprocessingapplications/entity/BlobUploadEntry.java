package at.ac.uibk.dps.streamprocessingapplications.entity;

import java.io.Serializable;

public class BlobUploadEntry implements Serializable {
    private String msgid;
    private String fileName;

    public BlobUploadEntry(String msgid, String fileName) {
        this.msgid = msgid;
        this.fileName = fileName;
    }

    public String getMsgid() {
        return msgid;
    }

    public void setMsgid(String msgid) {
        this.msgid = msgid;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
