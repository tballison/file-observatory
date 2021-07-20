package org.tallison.digest;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class CompareLists {

    public static void main(String[] args) throws Exception {
        Path pwd = Paths.get("");
        Path oneMillion = pwd.resolve("");
        Path s3 = pwd.resolve("");
        Path corpora = pwd.resolve("");

        Set<String> eval3 = load(oneMillion, Collections.EMPTY_SET);
        Set<String> s3Set = load(s3, eval3);
        Set<String> corporaSet = load(corpora, Collections.EMPTY_SET);

        System.out.println("eval3 size: "+ eval3.size());
        System.out.println("s3 size: "+ s3Set.size());
        System.out.println("corpora size: "+ corporaSet.size());

        int bad = 0;
        for (String k : s3Set) {
            if (! corporaSet.contains(k)) {
                System.out.println("Bad digest in s3 but not corpora: "+k);
                bad++;
            }
        }

        for (String k : corporaSet) {
            if (! s3Set.contains(k)) {
                System.out.println("Bad digest in corpora but not s3: "+k);
                bad++;
            }
        }

        for (String k : corporaSet) {
            if (! eval3.contains(k)) {
                System.out.println("file in corpora not in 1million "+k);
            }
        }
        System.out.println("bad "+bad);
    }

    private static Set<String> load(Path p, Set filterSet) throws Exception {
        Set<String> set = new HashSet<>();
        try (BufferedReader r = Files.newBufferedReader(p)) {
            String line = r.readLine();
            while (line != null) {
                String[] bits = line.split("\\s+");
                String k = bits[0].trim();
                k = k.replaceFirst("", "");
                k = k.replaceFirst("", "");
                k = k.trim();
                //System.out.println(k);
                if (filterSet.size() > 0 && filterSet.contains(k)) {
                    set.add(k);
                } else if (filterSet.size() == 0) {
                    set.add(k);
                }
                line = r.readLine();
            }
        }
        return set;
    }
}
