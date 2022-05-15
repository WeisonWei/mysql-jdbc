package mysql;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Filter {
  public static void main(String[] args) throws IOException {
    List<String> sqls = Files.lines(Paths.get("./all.csv"))
        .collect(Collectors.toList());
    HashMap<String, String> map = new HashMap<>();
    for (int i = 0; i < sqls.size(); i++) {
      if ("".equals(sqls.get(i))) {
        continue;
      }
      String replace = sqls.get(i).replace("\t", "");
      String[] split = replace.split("#");
      if (map.containsKey(split[0].toLowerCase())) {
        if (split.length == 1) {
          String s = map.get(split[0]) + "#000";
          map.put(split[0], s);
        } else {
          String s = map.get(split[0]) + "#" + split[1];
          map.put(split[0].toLowerCase(), s);
        }
      }
      if (split.length == 1) {
        map.put(split[0].toLowerCase(), "000");
      } else {
        map.put(split[0].toLowerCase(), split[1]);
      }
      System.out.println();
    }
    System.out.println();

    String csv = "./all2.csv";
    File file = new File(csv);
    if (file.createNewFile()) {
    } else {
      file.delete();
      file.createNewFile();
    }
    System.out.println("create result file");
    try (FileWriter writer = new FileWriter(file)) {

      Set<String> keys = map.keySet();
      for (String key : keys) {
        writer.write(key + "#" + map.get(key) + "===");
      }
    }
  }
}
