package mysql;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Mysql {
  /**
   * 创建数据库连接
   *
   * @return con 数据库连接
   */
  public static Connection getConnection() {
    Connection con = null;
    try {
      //加载属性文件，读取数据库连接配置信息
      Properties pro = new Properties();
      try {
        InputStream in = Mysql.class.getClassLoader().getResourceAsStream("mysql.properties");
        pro.load(in);
      } catch (IOException e) {
        System.out.println("未找到配置文件！！！");
      }
      String url = pro.getProperty("url");
      String username = pro.getProperty("username");
      String password = pro.getProperty("password");
      Class.forName("com.mysql.jdbc.Driver");
      con = DriverManager.getConnection(url, username, password);// 创建数据连接
    } catch (SQLException e) {
      e.printStackTrace();
      System.out.println("数据库连接失败");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("驱动类找不到");
    }
    return con;
  }

  public static Connection getMasterConnection() {
    Connection con = null;
    try {
      //加载属性文件，读取数据库连接配置信息
      Properties pro = new Properties();
      try {
        InputStream in = Mysql.class.getClassLoader().getResourceAsStream("mysql.properties");
        pro.load(in);
      } catch (IOException e) {
        System.out.println("未找到配置文件！！！");
      }
      String url = pro.getProperty("master-url");
      String username = pro.getProperty("master-username");
      String password = pro.getProperty("master-password");
      Class.forName("com.mysql.jdbc.Driver");
      con = DriverManager.getConnection(url, username, password);// 创建数据连接
    } catch (SQLException e) {
      e.printStackTrace();
      System.out.println("数据库连接失败");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("驱动类找不到");
    }
    return con;
  }


  /**
   * 关闭数据库
   *
   * @param con
   * @param stm
   * @param rs
   */
  public static void close(Connection con, Statement stm, ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    if (stm != null) {
      try {
        stm.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    if (con != null) {
      try {
        con.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}


