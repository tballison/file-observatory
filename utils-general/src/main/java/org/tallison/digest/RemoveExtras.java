package org.tallison.digest;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class RemoveExtras {

    public static void main(String[] args) throws Exception {
        Path pwd = Paths.get("");
        Path oneMillion = pwd.resolve("");

        Set<String> eval3 = load(oneMillion, Collections.EMPTY_SET);
        Set<String> extras = new HashSet<>();
        AWSCredentialsProvider provider = new ProfileCredentialsProvider("saml-pub");
        AmazonS3 s3Client =
                AmazonS3ClientBuilder.standard().withRegion("us-east-1").withCredentials(provider).build();
        String bucket = "safedocs-eval-three";
        String prefix = "staging2";
        int good = 0;
        int total = 0;
        for (S3ObjectSummary summary : S3Objects.withPrefix(s3Client, bucket, prefix)) {
            String k = summary.getKey();
            k = k.replaceFirst("\\Astaging2\\/", "");
            if (eval3.contains(k)) {
                good++;
            } else {
                System.out.println(good + " " + extras.size() + " : " + k);
                extras.add(k);
            }
            total++;
        }
        List<String> sorted = new ArrayList<>(extras);
        Collections.sort(sorted);
        int deleted = 0;
        System.out.println("about to delete " + sorted.size());
        for (String k : sorted) {
            k = prefix+"/"+k;
            System.out.println("deleting extra: " + deleted++
                    + " " + k);

            s3Client.deleteObject(bucket, k);
        }
        System.out.println(good + " : " + extras.size() + " " + total);
    }

    private static Set<String> load(Path p, Set filterSet) throws Exception {
        Set<String> set = new HashSet<>();
        try (BufferedReader r = Files.newBufferedReader(p)) {
            String line = r.readLine();
            while (line != null) {
                String[] bits = line.split("\\s+");
                String k = bits[0].trim();
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
