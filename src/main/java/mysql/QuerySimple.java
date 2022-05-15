package mysql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

public class QuerySimple {

  public static void main(String[] args) throws IOException {
    List<String> sqlList = Files.lines(Paths.get("./sql.sql"))
        .collect(Collectors.toList());
    String sql = sqlList.get(0);
    try {
      Connection con = null;
      Statement stm = null;
      ResultSet rs = null;
      long start = 0L;
      try {
        con = Mysql.getConnection();
        start = System.currentTimeMillis();
        stm = con.createStatement();
        rs = stm.executeQuery(sql);
        System.out.println("run cost :" + (System.currentTimeMillis() - start));
      } catch (Exception e) {
        System.out.println("sql:" + sql + " run error" + e.getMessage());
      } finally {
        Mysql.close(con, stm, rs);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      Connection con = null;
      Statement stm = null;
      ResultSet rs = null;
      long start1 = 0L;
      try {
        con = Mysql.getMasterConnection();
        start1 = System.currentTimeMillis();
        stm = con.createStatement();
        rs = stm.executeQuery(sql);
        boolean execute = stm.execute(sql);
        System.out.println("master run cost :" + (System.currentTimeMillis() - start1));
      } catch (Exception e) {
        System.out.println("sql:" + sql + " run error" + e.getMessage());
      } finally {
        Mysql.close(con, stm, rs);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
