package org.tallison.digest;

import java.io.BufferedReader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3ListCompare {
    public static void main(String[] args) throws Exception {
        Path pwd = Paths.get("");
        Path oneMillion = pwd.resolve("");
        Path s3 = pwd.resolve("s3-files.txt");
        Set<String> eval = load(oneMillion);
        Set<String> s3list = load(s3);
        System.out.println(eval.size());
        System.out.println(s3list.size());
        int missing = 0;
        for (String k : eval) {
            if (! s3list.contains(k)) {
                System.out.println("file missing in s3: "+ k);
                missing++;
            }
        }

        System.out.println("missing: " + missing);
    }

    private static Set<String> load(Path p) throws Exception {
        Set<String> set = new HashSet<>();
        try (BufferedReader r = Files.newBufferedReader(p)) {
            String line = r.readLine();
            while (line != null) {
                String[] bits = line.split("\\s+");
                String k = bits[0].trim();
                k = k.replaceFirst("", "");
                k = k.replaceFirst("", "");
                k = k.trim();
                set.add(k);
                line = r.readLine();
            }
        }
        return set;
    }
}
