package org.cboard.presto;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cboard.dataprovider.DataProvider;
import org.cboard.dataprovider.annotation.DatasourceParameter;
import org.cboard.dataprovider.annotation.ProviderName;
import org.cboard.dataprovider.annotation.QueryParameter;

import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoStatement;

/**
 *
 * @author duanyuntao
 *
 * @date 2016年12月1日14:28:58
 */

@ProviderName(name = "presto")
public class PrestoDataProvider extends DataProvider {

    @DatasourceParameter(label = "Driver (eg: com.facebook.presto.jdbc.PrestoDriver)", type = DatasourceParameter.Type.Input, order = 1)
    private final String DRIVER = "driver";

    @DatasourceParameter(label = "JDBC Url (eg: jdbc:presto://host:port/catalog)", type = DatasourceParameter.Type.Input, order = 2)
    private final String JDBC_URL = "jdbcurl";

    @DatasourceParameter(label = "User Name", type = DatasourceParameter.Type.Input, order = 3)
    private final String USERNAME = "username";

    @DatasourceParameter(label = "Password", type = DatasourceParameter.Type.Password, order = 4)
    private final String PASSWORD = "password";

    @QueryParameter(label = "SQL TEXT", type = QueryParameter.Type.TextArea, order = 1)
    private final String SQL = "sql";

    @Override
    public String[][] getData(Map<String, String> dataSource,
            Map<String, String> query) throws Exception {
        final PrestoConnection con = getConnection(dataSource);

        final String sql = query.get(SQL);
        PrestoStatement ps = null;
        ResultSet rs = null;
        List<String[]> list = null;

        try {
            ps = (PrestoStatement) con.createStatement();
            rs = ps.executeQuery(sql);
            final ResultSetMetaData metaData = rs.getMetaData();
            final int columnCount = metaData.getColumnCount();
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
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (final Exception e) {
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (final Exception e) {
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (final Exception e) {
                }
            }
        }

        return list.toArray(new String[][]{});
    }

    @Override
    public int resultCount(Map<String, String> dataSource,
            Map<String, String> query) throws Exception {
        final PrestoConnection con = getConnection(dataSource);
        final StringBuffer cubeSqlBuffer = new StringBuffer();
        final String querySql = query.get(SQL).replace(";", "");
        cubeSqlBuffer.append("SELECT count(*) AS cnt FROM ( ").append(querySql)
                .append(" ) AS cube_query__");

        PrestoStatement ps = null;
        ResultSet rs = null;
        int count = 0;

        try {
            ps = (PrestoStatement) con.createStatement();
            rs = ps.executeQuery(cubeSqlBuffer.toString());
            rs.next();
            count = rs.getInt("cnt");
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (final Exception e) {
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (final Exception e) {
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (final Exception e) {
                }
            }
        }
        return count;
    }

    private PrestoConnection getConnection(Map<String, String> dataSource)
            throws Exception {
        // 获取配置参数
        final String driver = dataSource.get(DRIVER);
        final String jdbcurl = dataSource.get(JDBC_URL);
        final String username = dataSource.get(USERNAME);
        final String password = dataSource.get(PASSWORD);
        //
        try {
            Class.forName(driver);
        } catch (final ClassNotFoundException e) {
            e.printStackTrace();
        }

        PrestoConnection connection = null;
        try {
            connection = (PrestoConnection) DriverManager.getConnection(
                    jdbcurl, username, password);
        } catch (final SQLException e) {
            e.printStackTrace();
        } catch (final Exception e) {
            e.printStackTrace();
        }

        return connection;
    }
}
