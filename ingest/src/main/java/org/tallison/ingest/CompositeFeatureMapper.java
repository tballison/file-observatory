package org.tallison.ingest;

import org.apache.tika.config.ServiceLoader;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class CompositeFeatureMapper implements FeatureMapper {
    private static final ServiceLoader DEFAULT_LOADER =
            new ServiceLoader(FeatureMapper.class.getClassLoader());

    List<FeatureMapper> mappers;

    public CompositeFeatureMapper() {
        this(DEFAULT_LOADER.loadServiceProviders(FeatureMapper.class));
    }

    public CompositeFeatureMapper(List<FeatureMapper> mappers) {
        this.mappers = mappers;
    }

    @Override
    public void addFeatures(Map<String, String> row, Fetcher fetcher,
                            StoredDocument storedDocument) throws SQLException {
        for (FeatureMapper mapper : mappers) {
            mapper.addFeatures(row, fetcher, storedDocument);
        }
    }
}
