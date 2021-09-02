package org.tallison.cc.index;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatLongAdder {

    public static String MAX_MIND_DB_PATH_PROPERTY_NAME = "MAX_MIND_DB_PATH";
    private static final HostRecord END_QUEUE = new HostRecord(-1, null);
    private static DatabaseReader MAX_MIND;
    static Logger LOGGER = LoggerFactory.getLogger(LatLongAdder.class);

    public static void load() throws IOException {
        String maxMindPath = System.getenv(MAX_MIND_DB_PATH_PROPERTY_NAME);
        MAX_MIND = new DatabaseReader.Builder(new File(maxMindPath)).build();
    }

    public static void main(String[] args) throws Exception {
        load();
        String dbUrl = args[0];
        int numThreads = args.length > 1 ? Integer.parseInt(args[1]) : 5;
        Connection connection = DriverManager.getConnection(dbUrl);
        ArrayBlockingQueue<HostRecord> queue = new ArrayBlockingQueue<>(1000);

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads+1);
        ExecutorCompletionService executorCompletionService =
                new ExecutorCompletionService(executorService);

        executorCompletionService.submit(new HostEnqueuer(connection, queue, numThreads));
        for (int i = 0; i < numThreads; i++) {
            executorCompletionService.submit(new HostQuerier(connection, queue));
        }

        int finished = 0;
        try {
            while (finished < numThreads + 1) {
                Future<Integer> future = executorCompletionService.take();
                future.get();
                finished++;
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    private static class HostRecord {
        private final int id;
        private final String host;

        public HostRecord(int id, String host) {
            this.id = id;
            this.host = host;
        }

        @Override
        public String toString() {
            return "HostRecord{" + "id=" + id + ", host='" + host + '\'' + '}';
        }
    }

    private static class HostEnqueuer implements Callable<Integer> {
        private final Connection connection;
        private final ArrayBlockingQueue<HostRecord> queue;
        private final int numThreads;
        public HostEnqueuer(Connection connection, ArrayBlockingQueue<HostRecord> queue,
                            int numThreads) {
            this.connection = connection;
            this.queue = queue;
            this.numThreads = numThreads;
        }

        @Override
        public Integer call() throws Exception {
            String sql = "select id, host from cc_hosts";
            try (ResultSet rs = connection.createStatement().executeQuery(sql)) {
                while (rs.next()) {
                    //blocking
                    queue.put(new HostRecord(rs.getInt(1), rs.getString(2)));
                }
            }
            for (int i = 0; i < numThreads; i++) {
                queue.put(END_QUEUE);
            }
            return 1;
        }
    }

    private static class HostQuerier implements Callable<Integer> {
        private final Connection connection;
        private final ArrayBlockingQueue<HostRecord> queue;
        private final PreparedStatement update;

        public HostQuerier(Connection connection, ArrayBlockingQueue<HostRecord> queue) throws
                SQLException {
            this.connection = connection;
            this.queue = queue;
            update = connection.prepareStatement("update cc_hosts set " +
                    "(ip_address, country, latitude, " +
                    "longitude) = (?,?,?,?) where id = ?");
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                //blocking
                HostRecord r = queue.take();
                if (r == END_QUEUE) {
                    return 1;
                }
                try {
                    InetAddress ipAddress = InetAddress.getByName(r.host);
                    CityResponse cityResponse = MAX_MIND.city(ipAddress);
                    update.clearParameters();
                    update.setString(1, ipAddress.getHostAddress());
                    update.setString(2, cityResponse.getCountry().getIsoCode());
                    update.setDouble(3, cityResponse.getLocation().getLatitude());
                    update.setDouble(4, cityResponse.getLocation().getLongitude());
                    update.setInt(5, r.id);
                    update.execute();
                } catch (SQLException e) {
                    throw e;
                } catch (UnknownHostException|AddressNotFoundException e) {
                    LOGGER.warn(r.toString(), e);
                } catch (Exception e) {
                    throw e;
                }
            }
        }
    }
}
