package org.tallison.corpus;

public class PackageConfig {

    private String dbConnectionString;

    private String selectString;

    private String srcRegion;
    private String srcBucket;

    private String srcPrefix;

    private String srcProfile;

    private String targRegion;
    private String targBucket;

    private String targPrefix;

    private String targProfile;

    private String zipDir;

    private boolean deleteLocalZips = true;

    public String getDbConnectionString() {
        return dbConnectionString;
    }

    public void setDbConnectionString(String dbConnectionString) {
        this.dbConnectionString = dbConnectionString;
    }

    public String getSrcBucket() {
        return srcBucket;
    }

    public void setSrcBucket(String srcBucket) {
        this.srcBucket = srcBucket;
    }

    public String getTargBucket() {
        return targBucket;
    }

    public void setTargBucket(String targBucket) {
        this.targBucket = targBucket;
    }

    public String getZipDir() {
        return zipDir;
    }

    public void setZipDir(String zipDir) {
        this.zipDir = zipDir;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getSrcPrefix() {
        return srcPrefix;
    }

    public void setSrcPrefix(String srcPrefix) {
        this.srcPrefix = srcPrefix;
    }

    public String getTargRegion() {
        return targRegion;
    }

    public void setTargRegion(String targRegion) {
        this.targRegion = targRegion;
    }

    public String getTargPrefix() {
        return targPrefix;
    }

    public void setTargPrefix(String targPrefix) {
        this.targPrefix = targPrefix;
    }

    public String getSrcProfile() {
        return srcProfile;
    }

    public void setSrcProfile(String srcProfile) {
        this.srcProfile = srcProfile;
    }

    public String getSelectString() {
        return selectString;
    }

    public void setSelectString(String selectString) {
        this.selectString = selectString;
    }

    public String getTargProfile() {
        return targProfile;
    }

    public void setTargProfile(String targProfile) {
        this.targProfile = targProfile;
    }

    public boolean isDeleteLocalZips() {
        return deleteLocalZips;
    }

    public void setDeleteLocalZips(boolean deleteLocalZips) {
        this.deleteLocalZips = deleteLocalZips;
    }
}
