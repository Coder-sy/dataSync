package com.hrqx.datasync.common.connection;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 参数传入
 *
 * @author Mc
 */
@Component
@Slf4j
public class SqlConn {

    public Connection getCon(String url, String user, String passWord) {
        Connection conn = null;
        try {
            //1. 加载数据库驱动，注册到驱动管理器
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            //2.获得数据库的连接
            conn = (Connection) DriverManager.getConnection(url, user, passWord);
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return conn;
    }
}
