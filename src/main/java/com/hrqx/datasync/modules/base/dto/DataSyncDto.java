package com.hrqx.datasync.modules.base.dto;

import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author sunzeng
 * @version 1.0
 * @date 2019/8/22 16:36
 */
@Data
public class DataSyncDto {

    /**
     * 数据同步配置路径
     */
    private String dataSyncPath;

    /**
     * 数据同步替换规则配置路径
     */
    private String replacePath;


    /**
     * 数据同步过滤规则配置路径
     */
    private String filterPath;

    /**
     * 同步的线程数
     */
    private int syncThreadNum;

    public DataSyncDto(String dataSyncPath, String replacePath, String filterPath, int syncThreadNum) {
        this.dataSyncPath = dataSyncPath;
        this.replacePath = replacePath;
        this.filterPath = filterPath;
        this.syncThreadNum = syncThreadNum;
    }
}
