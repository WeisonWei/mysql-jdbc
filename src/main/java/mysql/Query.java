package mysql;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Query {

  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
    Set<String> sqlSet = Files.lines(Paths.get("./sql.sql"))
        .collect(Collectors.toSet());
    int maxSize = (sqlSet.size() + 1000 - 1) / 1000;
    List<Set<String>> sqlList = Stream.iterate(0, n -> n + 1)
        .limit(maxSize)
        .parallel()
        .map(a -> sqlSet.parallelStream().skip(a * 1000).limit(1000).collect(Collectors.toSet()))
        .collect(Collectors.toList());
    System.out.println(sqlSet.size() + " split to " + sqlList.size() + " group to run");
    ExecutorService fixedThreadPool = Executors.newFixedThreadPool(4);
    List<Future<Set<String>>> futures = new ArrayList<>();
    for (int i = 0; i < sqlList.size(); i++) {
      int finalI = i;
      Future<Set<String>> future = fixedThreadPool.submit(() -> {
        Set<String> sqls = sqlList.get(finalI);
        Set<String> result = new HashSet<>();
        for (String sql : sqls) {
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
              /*System.out.println("result:");
              System.out.println(rs);
              while (rs.next()) {
                System.out.println(rs.next());
              }*/
              result.add(sql + " ,;" + (System.currentTimeMillis() - start));
            } catch (Exception e) {
              System.out.println("sql:" + sql + " run error" + e.getMessage());
            } finally {
              Mysql.close(con, stm, rs);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        System.out.println("The " + finalI + " group is over");
        return result;
      });
      futures.add(future);
    }
    List<String> result = new ArrayList<>();
    for (int i = 0; i < futures.size(); i++) {
      Future<Set<String>> future = futures.get(i);
      Set<String> strings = future.get();
      result.addAll(strings);
    }

    String log = "./sql_run.log";
    File file = new File(log);
    if (file.createNewFile()) {
    } else {
      file.delete();
      file.createNewFile();
    }
    System.out.println("create result file");
    try (FileWriter writer = new FileWriter(file)) {
      for (int i = 0; i < result.size(); i++) {
        writer.write(result.get(i) + "\t");
      }
    }
    System.out.println(result);
  }
}
