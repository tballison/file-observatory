package org.tallison.cc.index;

import org.tallison.cc.index.IndexRecordProcessor;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public abstract class AbstractRecordProcessor implements IndexRecordProcessor {

    protected static AtomicInteger threadCounter = new AtomicInteger(0);

    private final int threadNumber;

    public AbstractRecordProcessor() {
        threadNumber = threadCounter.incrementAndGet();
    }


    public void init(String[] args) throws Exception {

        if (args.length > 0 &&
                (args[0].equals("-h") || args[0].equals("--help"))) {
            usage();
            System.exit(1);
        }

    }

    public abstract void usage();

    protected int getThreadNumber() {
        return threadNumber;
    }

    String getExtension(String u) {
        if (u == null || u.length() == 0) {
            return null;
        }
        int i = u.lastIndexOf('.');
        if (i < 0 || i+6 < u.length()) {
            return null;
        }
        String ext = u.substring(i+1);
        ext = ext.trim();
        Matcher m = Pattern.compile("^\\d+$").matcher(ext);
        if (m.find()) {
            return null;
        }
        ext = ext.toLowerCase(Locale.ENGLISH);
        ext = ext.replaceAll("\\/$", "");
        return ext;
    }

    //returns "" if key is null, otherwise, trims and converts remaining \r\n\t to " "
    protected static String clean(String key) {
        if (key == null) {
            return "";
        }
        return key.trim().replaceAll("[\r\n\t]", " ");
    }

}
