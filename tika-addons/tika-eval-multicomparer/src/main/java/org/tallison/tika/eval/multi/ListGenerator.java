package org.tallison.tika.eval.multi;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class ListGenerator {

    public static void main(String[] args) throws Exception {
        Set<String> seen = new HashSet<>();
        File tools = new File("/home/tallison/data/extracts");
        for (File tool : tools.listFiles()) {
            for (File c : tool.listFiles()) {
                for (File e : c.listFiles()) {
                    String n = e.getName().replaceAll(".json", "").replaceAll(".txt", "");
                    if (! n.startsWith("._")) {
                        seen.add(n);
                    }
                }
            }
        }
        for (String n : seen) {
            System.out.println(n);
        }
    }
}
