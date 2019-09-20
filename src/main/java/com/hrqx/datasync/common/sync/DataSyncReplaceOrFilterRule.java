package com.hrqx.datasync.common.sync;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Maps;
import com.hrqx.datasync.common.utils.DataSyncUtil;
import org.assertj.core.util.Sets;
import org.jdom.Document;
import org.jdom.Element;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author sunzeng
 * @version 1.0
 * @date 2019/8/29 15:14
 */

public class DataSyncReplaceOrFilterRule {

    private String execSql;

    private Element element;

    private ResultSet resultSet;

    private JdbcTemplate jdbcTemplate;


    DataSyncReplaceOrFilterRule(String execSql, Element element, ResultSet resultSet, JdbcTemplate jdbcTemplate) {
        this.execSql = execSql;
        this.element = element;
        this.resultSet = resultSet;
        this.jdbcTemplate = jdbcTemplate;
    }

    public void replace(String replacePath) throws Exception {


        //参数
        String remoteColumn = element.getAttributeValue("remotecolumn");
        String oldParameter = execSql.substring(execSql.lastIndexOf(",") + 1, execSql.length() - 1);
        String parameter;
        //替换位置
        ///TODO 目前未实现
        String replacePosition = element.getAttributeValue("replaceposition");
        //由枚举还是数据库获取
        String replaceFrom = element.getAttributeValue("replacefrom");
        //获取替换规则，读取替换配置文件 replacePath
        //String path = System.getProperty("user.dir") + "/src/main/resources/replaceRule.xml";
        //获取替换规则JDOM
        Document document = DataSyncUtil.getDocument(replacePath);
        Element rootElement = document.getRootElement();
        List<Element> children = rootElement.getChildren();
        List<Element> tables = children.stream().filter((item -> item.getName().equals(replaceFrom))).collect(Collectors.toList());

        if (replaceFrom.equals("fromtable")) {
            //来源于哪张表
            String sourceTableName = element.getAttributeValue("sourcetablename");
            Optional<Element> sourceTable = tables.stream().filter(item -> item.getAttributeValue("sourcetable").equals(sourceTableName)).findFirst();
            //取值于哪个字段
            String sourceColumn = sourceTable.get().getAttributeValue("sourcecolumn");
            //和第三方表对应的字段
            String correspondingColumn = sourceTable.get().getAttributeValue("correspondingcolumn");

            //获取与目标值相同的中间列
            List<Map<String, Object>> map = jdbcTemplate.queryForList(" select " + sourceColumn + " from " + sourceTableName + " where " + correspondingColumn + " = " + remoteColumn);
           /* List<Map<String, String>> map = databaseSyncMapper.getlAllByTable(" select " + sourceColumn + " from " + sourceTableName + " where " + correspondingColumn + " = " + remoteColumn
            );*/
            parameter = (String) map.get(0).get(sourceColumn);
            //参数值替换
            execSql = execSql.replace(oldParameter, parameter);
        }

        if (replaceFrom.equals("fromenum")) {
            String localColumn = element.getAttributeValue("localcolumn");
            Element sourceEnum = tables.stream().filter(item -> item.getAttributeValue("column").equals(localColumn)).findFirst().get();
            HashMap<String, String> enumMap = Maps.newHashMap();
            List<Element> enumChildren = sourceEnum.getChildren();
            enumChildren.forEach(item -> enumMap.put(item.getAttributeValue("key"), item.getText()));

            String str = enumMap.get(oldParameter);
            if (StrUtil.isNotBlank(str)) {
                //参数值替换
                parameter = enumMap.get(oldParameter);
            } else {
                parameter = enumMap.get("other");
            }
            execSql = execSql.replace(oldParameter, parameter);
        }


    }


    public void filter(String filterPath) throws Exception {
        //查看学生教师表进行具体去重处理  替换为工号或教育ID
        String filterRule = element.getAttributeValue("filterrule");
        //过滤,去重
        if (filterRule.equals("duplication")) {
            int oldSize;
            int newSize;
            String parameter;

            //获取目标去重字段  在本地表中进行查询
            String localColumn = element.getAttributeValue("localcolumn");
            StringBuilder sb = new StringBuilder();
            //原始字段
            String oldParameter = execSql.substring(execSql.lastIndexOf(",") + 1, execSql.length() - 1);

            //获取本地表名
            String filterFrom = element.getAttributeValue("filterfrom");
            sb.append("select ").append(localColumn).append(" from ").append(filterFrom);
            List<String> query = jdbcTemplate.query(sb.toString(), new BeanPropertyRowMapper<String>());
            HashSet<String> uniqueColumns = Sets.newHashSet();
            query.forEach(item -> uniqueColumns.add(item));
            //Set<String> uniqueColumns = databaseSyncMapper.getColumn(sb.toString());
            oldSize = uniqueColumns.size();
            uniqueColumns.add(oldParameter.toLowerCase());
            newSize = uniqueColumns.size();

            //filterPath
            //String path = System.getProperty("user.dir") + "/src/main/resources/filterRule.xml";
            //获取替换规则JDOM
            Document document = DataSyncUtil.getDocument(filterPath);
            Element rootElement = document.getRootElement();
            List<Element> children = rootElement.getChildren();
            Optional<Element> tableElement = children.stream().filter((item -> item.getAttributeValue("tablename").equals(filterFrom))).findFirst();
            String replacefrom = null;
            if (tableElement.isPresent()) {
                replacefrom = tableElement.get().getAttributeValue("replacefrom");
            }
            //重复则进行替换
            if (oldSize == newSize) {
                parameter = resultSet.getString(replacefrom);
                execSql = execSql.replace(oldParameter, parameter);
            }
        }


    }
}
