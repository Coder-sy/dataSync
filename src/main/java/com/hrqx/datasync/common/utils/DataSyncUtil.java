package com.hrqx.datasync.common.utils;

import cn.hutool.core.util.StrUtil;
import com.hrqx.datasync.common.connection.SqlConn;
import com.hrqx.datasync.common.sync.DataSyncRuleEngine;
import com.hrqx.datasync.modules.base.dto.DataSyncDto;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author sunzeng
 */
@Data
@Slf4j
public class DataSyncUtil {


    /**
     * @param dataSyncDto 数据库信息对象
     *                    ResultSet 查询结果
     *                    同步远程数据库
     */
    public static void dataBaseSync(DataSyncDto dataSyncDto, JdbcTemplate jdbcTemplate, SqlConn sqlConn) {

        long l = System.currentTimeMillis();
        try {
         
            Document document = getDocument(dataSyncDto.getDataSyncPath());
            //获取顶级节点
            Element database = document.getRootElement();
            String url = database.getAttributeValue("url");
            String username = database.getAttributeValue("username");
            String password = database.getAttributeValue("password");
            //获取连接
            Connection conn = sqlConn.getCon(url, username, password);

            if (conn != null) {
                boolean containsSearchCondition = false;
                //获取所有表
                List<Element> children = database.getChildren();
                Statement stmt = conn.createStatement();
                for (Element child : children) {
                    String remoteTableName = child.getAttributeValue("remotename");
                    String searchColumns = child.getAttributeValue("searchcolumns");
                    log.info("计划任务-" + remoteTableName + "数据同步");
                    StringBuilder querySql = new StringBuilder();
                    if (StrUtil.isBlank(searchColumns)) {
                        querySql.append("select count(*) from ").append(remoteTableName);
                    }
                    //where后查询条件
                    StringBuilder search = new StringBuilder();
                    if (StrUtil.isNotBlank(searchColumns)) {
                        containsSearchCondition = true;

                        //多个查询条件，隔开 PS:id,name
                        String[] searchCondition = searchColumns.split(",");
                        String searchParameters = child.getAttributeValue("searchparameters");

                        //参数分割  PS:(1,2,3)/('wanger','lisi','zhangsan')
                        String[] parameter = searchParameters.split("/");

                        search.append(searchCondition[0]).append(" in ").append(parameter[0]);

                        if (searchCondition.length > 1) {
                            for (int i = 1; i < searchCondition.length; i++) {
                                search.append(" and ").append(searchCondition[i]).append(" in ").append(parameter[i]);
                            }
                        }

                        //是否需要动态sql查询三方表获取参数
                        String parameterIsSql = child.getAttributeValue("parameterissql");
                        if (StrUtil.isNotBlank(parameterIsSql) && parameterIsSql.equals("true")) {
                            String tripartiteTableName = child.getAttributeValue("tripartitetablename");
                            String targetColumn = child.getAttributeValue("targetcolumn");
                            String tripartiteTableSearchColumn = child.getAttributeValue("tripartitetablesearchcolumn");
                            String tripartiteTableParameter = child.getAttributeValue("tripartitetableparameter");
                            search.append(" and ").append(targetColumn).append(" in ").append(" select ").append(targetColumn).append(" from ").append(tripartiteTableName).append(" where ").append(tripartiteTableSearchColumn).append(" = ").append(tripartiteTableParameter);
                        }
                        querySql.append("select count(*) from ").append(remoteTableName).append(" where ").append(search);
                    }

                    ResultSet resultSet = stmt.executeQuery(querySql.toString());
                    resultSet.next();
                    // 表中数据总条数
                    long totalNum = resultSet.getLong(1);
                    //每个线程所同步数据数量
                    long ownerRecordNum = (long) Math.ceil((totalNum * 1.0 / dataSyncDto.getSyncThreadNum()));
                    for (int i = 0; i < dataSyncDto.getSyncThreadNum(); i++) {
                        //查询是否已包含数据，包含则按照最新时间同步三方表
                        // 增量同步
                        String yourUpdatecolumnFlag = child.getAttributeValue("yourupdatecolumnflag");
                        String incrementalFlag = child.getAttributeValue("incrementalflag");


                        String sql = " select " + yourUpdatecolumnFlag + " from " + child.getAttributeValue("localname") + " WHERE " + incrementalFlag + " IS NOT NULL ORDER BY " + yourUpdatecolumnFlag + " DESC LIMIT 1";
                        List<Date> operationDate = jdbcTemplate.query(sql, new BeanPropertyRowMapper<Date>(Date.class));

                        
                        String operationTimeColumn = child.getAttributeValue("operationtimecolumn");
                        StringBuilder stringBuilder = new StringBuilder();
                        //sqlServer  mysql不支持rowID
                        stringBuilder.append("select * from(select ROW_NUMBER() over(order by Id)as rowNumber,* from ").append(remoteTableName).append(" ) c where c.rowNumber between  ").append(i * ownerRecordNum + 1).append(" and ").append(i * ownerRecordNum + ownerRecordNum);
                        if (containsSearchCondition) {
                            stringBuilder.append(" and ").append(search);
                            if (!operationDate.isEmpty()) {
                                stringBuilder.append(" and ").append(operationTimeColumn).append(" > '").append(operationDate.get(0)).append("'");
                            }
                        }
                        if (!containsSearchCondition) {
                            if (!operationDate.isEmpty()) {
                                stringBuilder.append(" where ").append(operationTimeColumn).append(" > '").append(operationDate.get(0)).append("'");
                            }
                        }
                        QueryDataThread workThread = new QueryDataThread(stringBuilder.toString(), stmt, child, jdbcTemplate, dataSyncDto.getReplacePath(), dataSyncDto.getFilterPath());
                        workThread.run();
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        log.info("同步数据耗时" + (System.currentTimeMillis() - l) + "毫秒");
    }


    /**
     * 正则提取手机号码
     *
     * @param str phone
     * @return 手机号
     */
    public static String getMobilPhone(String str) {
        String phones = "";
        if (null != str) {
            String regEx = "1[35789]\\d{9}";
            // "/^1(?:3\\d|4[4-9]|5[0-35-9]|6[67]|7[013-8]|8\\d|9\\d)\\d{8}$/";
            Pattern p = Pattern.compile(regEx);
            Matcher m = p.matcher(str);
            while (m.find()) {
                if (StrUtil.isBlank(phones)) {
                    phones = m.group();
                } else {
                    phones += "," + m.group();
                }
            }
        }
        return phones;
    }

    /**
     * 获取dom树
     */
    public static Document getDocument(String path) throws JDOMException, IOException {
        SAXBuilder saxBuilder = new SAXBuilder();
        InputStream in;
        in = new FileInputStream(path);
        InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
        return saxBuilder.build(isr);
    }


    private static class QueryDataThread implements Runnable {

        private String querySql;
        private Statement stmt;
        private Element child;
        private JdbcTemplate jdbcTemplate;
        private String replacePath;
        private String filterPath;


        QueryDataThread(String querySql, Statement stmt, Element child, JdbcTemplate jdbcTemplate, String replacePath, String filterPath) {
            this.querySql = querySql;
            this.stmt = stmt;
            this.child = child;
            this.jdbcTemplate = jdbcTemplate;
            this.replacePath = replacePath;
            this.filterPath = filterPath;
        }

        @Override
        public void run() {

            try {
                ResultSet resultSet = stmt.executeQuery(querySql);
                // 对列进行操作  child：表 children：字段列
                DataSyncRuleEngine dataSyncRuleEngine = new DataSyncRuleEngine(resultSet, child, jdbcTemplate,replacePath,filterPath);
                dataSyncRuleEngine.sync();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

    }

}
