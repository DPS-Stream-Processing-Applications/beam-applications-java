package org.apache.flink.statefun.playground.java.greeter.types.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public final class FIT_data extends TableServiceEntity {
    private String subjectId,
            acc_chest_x,
            acc_chest_y,
            acc_chest_z,
            ecg_lead_1,
            ecg_lead_2,
            acc_ankle_x,
            acc_ankle_y,
            acc_ankle_z,
            gyro_ankle_x,
            gyro_ankle_y,
            gyro_ankle_z,
            magnetometer_ankle_x,
            magnetometer_ankle_y,
            magnetometer_ankle_z,
            acc_arm_x,
            acc_arm_y,
            acc_arm_z,
            gyro_arm_x,
            gyro_arm_y,
            gyro_arm_z,
            magnetometer_arm_x,
            magnetometer_arm_y,
            magnetometer_arm_z,
            label,
            age,
            gender;
    private long ts;

    public static FIT_data parseString(String s, long counter) {
        FIT_data obj = new FIT_data();
        String fields[] = s.split(",");
        obj.rowKey = fields[0] + "-" + fields[1];
        obj.partitionKey = "fit_range";
        obj.setSubjectId(fields[0]);
        obj.setTs(Long.parseLong(fields[1]));
        obj.setAcc_chest_x(fields[2]);
        obj.setAcc_chest_y(fields[3]);
        obj.setAcc_chest_z(fields[4]);
        obj.setEcg_lead_1(fields[5]);
        obj.setEcg_lead_1(fields[6]);
        obj.setAcc_ankle_x(fields[7]);
        obj.setAcc_ankle_y(fields[8]);
        obj.setAcc_ankle_z(fields[9]);
        obj.setGyro_ankle_x(fields[10]);
        obj.setGyro_ankle_y(fields[11]);
        obj.setGyro_ankle_z(fields[12]);
        obj.setMagnetometer_ankle_x(fields[13]);
        obj.setMagnetometer_ankle_y(fields[14]);
        obj.setMagnetometer_ankle_z(fields[15]);
        obj.setAcc_arm_x(fields[16]);
        obj.setAcc_arm_y(fields[17]);
        obj.setAcc_arm_z(fields[18]);
        obj.setGyro_arm_x(fields[19]);
        obj.setGyro_arm_y(fields[20]);
        obj.setGyro_arm_z(fields[21]);
        obj.setMagnetometer_arm_x(fields[22]);
        obj.setMagnetometer_arm_y(fields[23]);
        obj.setMagnetometer_arm_z(fields[24]);
        obj.setLabel(fields[25]);
        return obj;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(String subjectId) {
        this.subjectId = subjectId;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long timestamp) {
        this.ts = timestamp;
    }

    public String getAcc_chest_x() {
        return acc_chest_x;
    }

    public void setAcc_chest_x(String acc_chest_x) {
        this.acc_chest_x = acc_chest_x;
    }

    public String getAcc_chest_y() {
        return acc_chest_y;
    }

    public void setAcc_chest_y(String acc_chest_y) {
        this.acc_chest_y = acc_chest_y;
    }

    public String getAcc_chest_z() {
        return acc_chest_z;
    }

    public void setAcc_chest_z(String acc_chest_z) {
        this.acc_chest_z = acc_chest_z;
    }

    public String getEcg_lead_1() {
        return ecg_lead_1;
    }

    public void setEcg_lead_1(String ecg_lead_1) {
        this.ecg_lead_1 = ecg_lead_1;
    }

    public String getEcg_lead_2() {
        return ecg_lead_2;
    }

    public void setEcg_lead_2(String ecg_lead_2) {
        this.ecg_lead_2 = ecg_lead_2;
    }

    public String getAcc_ankle_x() {
        return acc_ankle_x;
    }

    public void setAcc_ankle_x(String acc_ankle_x) {
        this.acc_ankle_x = acc_ankle_x;
    }

    public String getAcc_ankle_y() {
        return acc_ankle_y;
    }

    public void setAcc_ankle_y(String acc_ankle_y) {
        this.acc_ankle_y = acc_ankle_y;
    }

    public String getAcc_ankle_z() {
        return acc_ankle_z;
    }

    public void setAcc_ankle_z(String acc_ankle_z) {
        this.acc_ankle_z = acc_ankle_z;
    }

    public String getGyro_ankle_x() {
        return gyro_ankle_x;
    }

    public void setGyro_ankle_x(String gyro_ankle_x) {
        this.gyro_ankle_x = gyro_ankle_x;
    }

    public String getGyro_ankle_y() {
        return gyro_ankle_y;
    }

    public void setGyro_ankle_y(String gyro_ankle_y) {
        this.gyro_ankle_y = gyro_ankle_y;
    }

    public String getGyro_ankle_z() {
        return gyro_ankle_z;
    }

    public void setGyro_ankle_z(String gyro_ankle_z) {
        this.gyro_ankle_z = gyro_ankle_z;
    }

    public String getMagnetometer_ankle_x() {
        return magnetometer_ankle_x;
    }

    public void setMagnetometer_ankle_x(String magnetometer_ankle_x) {
        this.magnetometer_ankle_x = magnetometer_ankle_x;
    }

    public String getMagnetometer_ankle_y() {
        return magnetometer_ankle_y;
    }

    public void setMagnetometer_ankle_y(String magnetometer_ankle_y) {
        this.magnetometer_ankle_y = magnetometer_ankle_y;
    }

    public String getMagnetometer_ankle_z() {
        return magnetometer_ankle_z;
    }

    public void setMagnetometer_ankle_z(String magnetometer_ankle_z) {
        this.magnetometer_ankle_z = magnetometer_ankle_z;
    }

    public String getAcc_arm_x() {
        return acc_arm_x;
    }

    public void setAcc_arm_x(String acc_arm_x) {
        this.acc_arm_x = acc_arm_x;
    }

    public String getAcc_arm_y() {
        return acc_arm_y;
    }

    public void setAcc_arm_y(String acc_arm_y) {
        this.acc_arm_y = acc_arm_y;
    }

    public String getAcc_arm_z() {
        return acc_arm_z;
    }

    public void setAcc_arm_z(String acc_arm_z) {
        this.acc_arm_z = acc_arm_z;
    }

    public String getGyro_arm_x() {
        return gyro_arm_x;
    }

    public void setGyro_arm_x(String gyro_arm_x) {
        this.gyro_arm_x = gyro_arm_x;
    }

    public String getGyro_arm_y() {
        return gyro_arm_y;
    }

    public void setGyro_arm_y(String gyro_arm_y) {
        this.gyro_arm_y = gyro_arm_y;
    }

    public String getGyro_arm_z() {
        return gyro_arm_z;
    }

    public void setGyro_arm_z(String gyro_arm_z) {
        this.gyro_arm_z = gyro_arm_z;
    }

    public String getMagnetometer_arm_x() {
        return magnetometer_arm_x;
    }

    public void setMagnetometer_arm_x(String magnetometer_arm_x) {
        this.magnetometer_arm_x = magnetometer_arm_x;
    }

    public String getMagnetometer_arm_y() {
        return magnetometer_arm_y;
    }

    public void setMagnetometer_arm_y(String magnetometer_arm_y) {
        this.magnetometer_arm_y = magnetometer_arm_y;
    }

    public String getMagnetometer_arm_z() {
        return magnetometer_arm_z;
    }

    public void setMagnetometer_arm_z(String magnetometer_arm_z) {
        this.magnetometer_arm_z = magnetometer_arm_z;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }
}
