package com.yahoo.hive.udf.funnel;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SparkSqlClient {

    static Connection conn = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSqlClient.class);
    private static SparkSqlClient client;
    private BasicDataSource ds;

    private SparkSqlClient() {
        try {
            LOGGER.info("SparkSqlClient init");
            // 连接池
            ds = new BasicDataSource();
            ds.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
            ds.setUsername("hadoop");
            ds.setPassword("");
            ds.setUrl("jdbc:hive2://10.10.101.34:10000");

            // 最小空闲连接数
            ds.setMinIdle(10);
            // 初始连接数
            ds.setInitialSize(1);
            // 最大同时连接数
            ds.setMaxActive(50);
            ds.setRemoveAbandoned(true);
            ds.setRemoveAbandonedTimeout(180);

            // 直接连接
            // Class.forName(SparkSqlConfig.JDBC_DRIVER);
        } catch (Exception exception) {
            exception.printStackTrace();
            LOGGER.error(exception.getMessage(), exception);
        }
    }

    /**
     * spark sql client instance.
     * 
     * @return SparkSqlClient
     * @throws IOException
     *             IOException
     * @throws SQLException
     *             SQLException
     * @throws PropertyVetoException
     *             PropertyVetoException
     */
    public static SparkSqlClient getInstance() throws IOException, SQLException, PropertyVetoException {
        if (client == null) {
            synchronized (SparkSqlClient.class) {
                if (client == null) {
                    LOGGER.info("SparkSqlClient new client");
                    client = new SparkSqlClient();
                }
            }
        }
        return client;
    }

    /**
     * 获取sparksql连接.
     * 
     * @return connection
     * @throws SQLException
     *             SQLException
     */
    public Connection getConnection() throws SQLException {
        LOGGER.info("SparkSqlClient getConnection");

        // 使用连接池
        return this.ds.getConnection();

        // 不使用连接池/直接连接
        // try {
        // conn =
        // DriverManager.getConnection(SparkSqlConfig.URL,SparkSqlConfig.USERNAME,SparkSqlConfig.PASSWORD);
        // } catch (SQLException e) {
        // e.printStackTrace();
        // }
        // return conn;

    }

}
