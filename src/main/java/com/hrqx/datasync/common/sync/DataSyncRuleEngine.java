package com.hrqx.datasync.common.sync;


import cn.hutool.core.util.StrUtil;
import com.hrqx.datasync.common.utils.DataSyncUtil;
import org.assertj.core.util.Sets;
import org.jdom.Element;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.util.List;
import java.util.Set;

/**
 * @author sunzeng
 * @version 1.0
 * @date 2019/8/23 17:48
 */
public class DataSyncRuleEngine {


    private Element child;

    private ResultSet resultSet;

    private JdbcTemplate jdbcTemplate;

    private String replacePath;

    private String filterPath;


    public DataSyncRuleEngine(ResultSet resultSet, Element child, JdbcTemplate jdbcTemplate, String replacePath, String filterPath) {
        this.resultSet = resultSet;
        this.child = child;
        this.jdbcTemplate = jdbcTemplate;
        this.replacePath = replacePath;
        this.filterPath = filterPath;
    }


    /**
     * 具体同步
     */
    public void sync() throws Exception {

        //获取列
        List<Element> children = child.getChildren();

        String localTableName = child.getAttributeValue("localname");

    
        StringBuilder saveOrUpdateSqls = new StringBuilder("insert into " + localTableName + "(");

        Set<String> columns = getAllColumns(children);

        StringBuilder localColumns = new StringBuilder();
        columns.forEach(item -> localColumns.append(item).append(","));

        localColumns.deleteCharAt(localColumns.length() - 1);
        saveOrUpdateSqls.append(localColumns.toString());


        saveOrUpdateSqls.append(") values ");


        while (resultSet.next()) {
           
            saveOrUpdateSqls.append(" (");

            //拼接数据批量更新语句
            appendExecSql(children, saveOrUpdateSqls);
          
        }

        String[] split = localColumns.toString().split(",");
        StringBuilder valuesSql = new StringBuilder();
        //拼接userName=VALUES(userName), userID = VALUES(userID)
        for (String str : split) {
            valuesSql.append(str).append("=").append("values(").append(str).append("),");
        }


        valuesSql.deleteCharAt(valuesSql.length() - 1);
        saveOrUpdateSqls.deleteCharAt(saveOrUpdateSqls.length() - 1).append(" on duplicate key update ").append(valuesSql);
        jdbcTemplate.batchUpdate(saveOrUpdateSqls.toString());

        //反射执行其他方法‘同步学生表后续需要补充学生的用户ID’取之于用户ID  需要根据学生创建新增用户获取ID后再赋值到学生的用户ID
        String postMethod = child.getAttributeValue("postmethod");
        //TODO 同步后可继续扩展其他操作，继承此类即可
       /* if (StrUtil.isNotBlank(postMethod)) {
            String fullClassName = child.getAttributeValue("fullclassname");
            Class<?> clazz = Class.forName(fullClassName);
            Method syncUser = clazz.getMethod(postMethod, DatabaseSyncMapper.class, UserService.class, UserRoleService.class, String.class, RoleService.class);
            syncUser.invoke(clazz.newInstance(), databaseSyncMapper, userService, userRoleService, localTableName, roleService);
        }*/


    }

    private void appendExecSql(List<Element> children, StringBuilder updateSqls) throws Exception {
        for (Element column : children) {
            String type = column.getAttributeValue("type");
            appendSql(updateSqls, type, resultSet, column);
            updateSqls.append(",");
        }
        updateSqls.deleteCharAt(updateSqls.length() - 1).append("),");
    }

    /**
     * 获取本地表所有字段
     */
    private Set<String> getAllColumns(List<Element> children) {
        Set<String> columns = Sets.newLinkedHashSet();
        for (Element element : children) {
            String localColumn = element.getAttributeValue("localcolumn");
            columns.add(localColumn);
        }

        return columns;
    }

    /**
     * 判断字符类型并对sql进行拼接
     */
    private void appendSql(StringBuilder querySql, String type, ResultSet resultSet, Element element) throws Exception {
        switch (type) {
            //由内部表获取  学生、教师的user_id等
            case "obtainedbyinternaltable":
                querySql.append("'").append('0').append("'");
                replaceOrFilter(element, querySql);
                break;

            case "int":
                querySql.append("'").append(resultSet.getInt(element.getAttributeValue("remotecolumn"))).append("'");
                replaceOrFilter(element, querySql);
                break;

            case "long":
                querySql.append("'").append(resultSet.getLong(element.getAttributeValue("remotecolumn"))).append("'");
                replaceOrFilter(element, querySql);
                break;

            case "String":
                querySql.append("'").append(resultSet.getString(element.getAttributeValue("remotecolumn"))).append("'");
                replaceOrFilter(element, querySql);
                break;

            case "Date":
                querySql.append("'").append(resultSet.getTimestamp(element.getAttributeValue("remotecolumn"))).append("'");
                replaceOrFilter(element, querySql);
                break;

            case "double":
                querySql.append("'").append(resultSet.getDouble(element.getAttributeValue("remotecolumn"))).append("'");
                replaceOrFilter(element, querySql);
                break;

            case "boolean":
                querySql.append("'").append(resultSet.getBoolean(element.getAttributeValue("remotecolumn"))).append("'");
                replaceOrFilter(element, querySql);
                break;
            //type 为联系电话时，进行正则匹配
            case "LXDH":
                String mobilPhone = DataSyncUtil.getMobilPhone(resultSet.getString(element.getAttributeValue("remotecolumn")));
                querySql.append("'").append(mobilPhone).append("'");
                replaceOrFilter(element, querySql);
                break;

       
            default:
                //默认为密码字段   密码为123456
                querySql.append("'").append("123456").append("'");
                replaceOrFilter(element, querySql);
                break;

        }
    }


    /**
     * @param element  列具体信息
     * @param querySql 拼接sql语句
     */
   
    private void replaceOrFilter(Element element, StringBuilder querySql) throws Exception {

        DataSyncReplaceOrFilterRule dataSyncReplaceOrFilterRule = new DataSyncReplaceOrFilterRule(querySql.toString(), element, resultSet, jdbcTemplate);

        String isReplace = element.getAttributeValue("isreplace");
        String isFilter = element.getAttributeValue("isfilter");

        if (StrUtil.isNotBlank(isReplace) && isReplace.equals("true")) {
            dataSyncReplaceOrFilterRule.replace(replacePath);
        }

        if (StrUtil.isNotBlank(isFilter) && isReplace.equals("true")) {
            dataSyncReplaceOrFilterRule.filter(filterPath);
        }

      
    }
}
