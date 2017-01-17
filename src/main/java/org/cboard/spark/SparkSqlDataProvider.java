package org.cboard.spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cboard.dataprovider.DataProvider;
import org.cboard.dataprovider.annotation.DatasourceParameter;
import org.cboard.dataprovider.annotation.ProviderName;
import org.cboard.dataprovider.annotation.QueryParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author duanyuntao
 * 
 * @date 2016年12月21日14:28:58
 */

@ProviderName(name = "sparkSql")
public class SparkSqlDataProvider extends DataProvider {
	private static final Logger LOG = LoggerFactory
			.getLogger(SparkSqlDataProvider.class);

	@DatasourceParameter(label = "Driver (eg: org.apache.hive.jdbc.HiveDriver)", type = DatasourceParameter.Type.Input, order = 1)
	private String DRIVER = "driver";

	@DatasourceParameter(label = "JDBC Url (eg: jdbc:hive2://host:port/database)", type = DatasourceParameter.Type.Input, order = 2)
	private String JDBC_URL = "jdbcurl";

	@DatasourceParameter(label = "User Name", type = DatasourceParameter.Type.Input, order = 3)
	private String USERNAME = "username";

	@DatasourceParameter(label = "Password", type = DatasourceParameter.Type.Password, order = 4)
	private String PASSWORD = "password";

	@QueryParameter(label = "Spark SQL Text", type = QueryParameter.Type.TextArea, order = 1)
	private String SQL = "sql";

	@Override
	public String[][] getData(Map<String, String> dataSource,
			Map<String, String> query) throws Exception {
		LOG.debug("Execute SparkSqlDataProvider.getData() Start!");
		
		Connection con = getConnection(dataSource);
		String sql = query.get(SQL);
		Statement ps = null;
		ResultSet rs = null;
		List<String[]> list = null;
		LOG.info("Spark SQL:"+sql);

		try {
			ps = con.createStatement();
			rs = ps.executeQuery(sql);
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();
			list = new LinkedList<>();
			String[] row = new String[columnCount];
			for (int i = 0; i < columnCount; i++) {
				row[i] = metaData.getColumnName(i + 1);
			}
			list.add(row);
			while (rs.next()) {
				row = new String[columnCount];
				for (int j = 0; j < columnCount; j++) {
					row[j] = rs.getString(j + 1);
				}
				list.add(row);
			}
		} catch (Exception e) {
			LOG.error("ERROR:" + e.getMessage());
			throw new Exception("ERROR:" + e.getMessage(), e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					LOG.error("ERROR:" + e.getMessage());
					throw new Exception("ERROR:" + e.getMessage(), e);
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					LOG.error("ERROR:" + e.getMessage());
					throw new Exception("ERROR:" + e.getMessage(), e);
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					LOG.error("ERROR:" + e.getMessage());
					throw new Exception("ERROR:" + e.getMessage(), e);
				}
			}
		}
		LOG.debug("Execute SparkSqlDataProvider.getData() End!");
		return list.toArray(new String[][] {});
	}

	@Override
	public int resultCount(Map<String, String> dataSource,
			Map<String, String> query) throws Exception {
		LOG.debug("Execute SparkSqlDataProvider.resultCount() Start!");
		
		Connection con = getConnection(dataSource);
		StringBuffer cubeSqlBuffer = new StringBuffer();
		String querySql = query.get(SQL).replace(";", "");
		cubeSqlBuffer.append("SELECT count(*) AS cnt FROM ( ").append(querySql)
				.append(" ) AS cube_query__");

		Statement ps = null;
		ResultSet rs = null;
		int count = 0;
		LOG.info("Spark SQL:"+cubeSqlBuffer.toString());

		try {
			ps = con.createStatement();
			rs = ps.executeQuery(cubeSqlBuffer.toString());
			rs.next();
			count = rs.getInt("cnt");
		} catch (Exception e) {
			LOG.error("ERROR:" + e.getMessage());
			throw new Exception("ERROR:" + e.getMessage(), e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					LOG.error("ERROR:" + e.getMessage());
					throw new Exception("ERROR:" + e.getMessage(), e);
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					LOG.error("ERROR:" + e.getMessage());
					throw new Exception("ERROR:" + e.getMessage(), e);
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					LOG.error("ERROR:" + e.getMessage());
					throw new Exception("ERROR:" + e.getMessage(), e);
				}
			}
		}
		LOG.debug("Execute SparkSqlDataProvider.resultCount() End!");
		return count;
	}

	private Connection getConnection(Map<String, String> dataSource)
			throws Exception {
		LOG.debug("Execute SparkSqlDataProvider.getConnection() Start!");
		
		// 获取配置参数
		String driver = dataSource.get(DRIVER);
		String jdbcurl = dataSource.get(JDBC_URL);
		String username = dataSource.get(USERNAME);
		String password = dataSource.get(PASSWORD);
		LOG.debug("Get 'SparkSql' JDBC Connection Paramter: driver='" + driver
				+ "',jdbcUrl='" + jdbcurl + "',userName='" + username
				+ "',passWord='" + password + "'");

		Connection connection = null;
		try {
			Class.forName(driver);
			connection = DriverManager.getConnection(jdbcurl, username,
					password);
		} catch (Exception e) {
			LOG.error("ERROR:" + e.getMessage());
			throw new Exception("ERROR:" + e.getMessage(), e);
		}
		LOG.debug("Execute SparkSqlDataProvider.getConnection() End!");
		return connection;
	}
}
