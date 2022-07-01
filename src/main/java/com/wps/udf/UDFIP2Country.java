package com.wps.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.io.IOException;
import java.sql.*;

/**
 * 不成功
 */
public class UDFIP2Country extends GenericUDF {
    Connection conn = null;
    ResultSet resultSet = null;
    PreparedStatement stmt = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        String driverName="org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://node03:10000/myhive";
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return objectInspectors[0];
    }

    @Override
    public String evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String ip = (String) deferredObjects[0].get();
        String hiveSql = "select country from wm_ip_china where (cast(split("
                +ip+",\"\\\\.\")[0] as bigint)*256*256*256+cast(split("
                +ip+",\"\\\\.\")[1] as bigint)*256*256+cast(split("
                +ip+",\"\\\\.\")[2] as bigint)*256+cast(split("
                +ip+",\"\\\\.\")[3] as bigint)) between long_ip_start and long_ip_end";
        String country=null;
        try {
            stmt = conn.prepareStatement(hiveSql);
            resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                country=resultSet.getString(1);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return country;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }

    @Override
    public void close() throws IOException {
        try {
            resultSet.close();
            conn.close();
        } catch (Exception throwables) {
            throwables.printStackTrace();
        }
    }
}
