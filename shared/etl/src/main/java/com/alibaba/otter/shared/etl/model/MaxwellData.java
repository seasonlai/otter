package com.alibaba.otter.shared.etl.model;

import java.util.Map;

/**
 * Creator: laizhicheng
 * DateTime: 2020/9/25 11:25
 * Description: No Description
 */
public class MaxwellData {

    private String database;
    private String table;
    private String type;
    private long ts;
    private long xid;
    private boolean commit;
    private Map<String,Object> data;
    private Map<String,Object> old;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public long getXid() {
        return xid;
    }

    public void setXid(long xid) {
        this.xid = xid;
    }

    public boolean isCommit() {
        return commit;
    }

    public void setCommit(boolean commit) {
        this.commit = commit;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Map<String, Object> getOld() {
        return old;
    }

    public void setOld(Map<String, Object> old) {
        this.old = old;
    }
}
