import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

public class FSProfiler {
    public static void main(String[] args) throws Exception {
        Path rootDir = Paths.get(args[0]);
        Connection connection = DriverManager.getConnection(args[1]);
        FSProfiler profiler = new FSProfiler();
        profiler.execute(rootDir, connection);
    }

    private void execute(Path rootDir, Connection connection)
            throws SQLException, IOException {
        createTable(connection);
        PreparedStatement insert = prepareInsert(connection);
        AtomicInteger cnt = new AtomicInteger(0);
        Files.walkFileTree(rootDir, new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (cnt.incrementAndGet() % 1000 == 0) {
                    System.out.println("processed "+cnt.get());
                }
                if (file.getFileName().toString().startsWith(".")) {
                    //curse you .DS_STORE!
                    return FileVisitResult.CONTINUE;
                }
                try {
                    long sz = Files.size(file);
                    String digest = "";
                    try (InputStream is = Files.newInputStream(file)) {
                        digest = DigestUtils.sha256Hex(is);
                    }
                    Path rel = rootDir.relativize(file);
                    String collection = rel.getName(0).toString();
                    String path = rel.toString();
                    insert.clearParameters();
                    insert.setString(1, path);
                    insert.setString(2, collection);
                    insert.setLong(3, sz);
                    insert.setString(4, digest);
                    insert.execute();
                } catch (SQLException e) {
                    throw new IOException(e);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                throw exc;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }

        });
    }

    private PreparedStatement prepareInsert(Connection connection)
        throws SQLException {
        String insert = "insert into profiles values (?,?,?,?)";
        return connection.prepareStatement(insert);
    }

    private void createTable(Connection connection) throws SQLException {
        String sql = "drop table if exists profiles";
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
            sql = "create table profiles (" +
                    "path varchar(10000)," +
                    "collection varchar(256),"+
                    "size bigint,"+
                    "shasum256 varchar(64))";
            st.execute(sql);
        }
    }
}
