package org.tallison.digest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.codec.digest.DigestUtils;

public class S3Compare {
    public static void main(String[] args) throws Exception {
        Path pwd = Paths.get("");
        Path oneMillion = pwd.resolve("");
        try (Writer writer = Files.newBufferedWriter(pwd.resolve("s3-files.txt"))) {
            Set<String> evalThree = load(oneMillion);
            Set<String> s3 = new HashSet<>();
            String bucket = "S3_BUCKET";
            AWSCredentialsProvider provider = new ProfileCredentialsProvider("");
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion("").withCredentials(provider).build();
            String prefix = "";
            int cnt = 0;
            for (S3ObjectSummary summary : S3Objects.withPrefix(s3Client, bucket, prefix)) {
                String k = summary.getKey();
                k = k.replaceFirst("", "");
                s3.add(k);
                writer.write(k);
                writer.write("\n");
                cnt++;
                if (cnt % 1000 == 0) {
                    System.out.println("counted " + cnt);
                }
            }
        }

        //System.out.println(evalThree.size() + " " + s3.size());
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
