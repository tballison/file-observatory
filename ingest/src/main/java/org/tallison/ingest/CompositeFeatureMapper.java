package org.tallison.ingest;

import org.apache.tika.config.ServiceLoader;
import org.tallison.quaerite.core.StoredDocument;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class CompositeFeatureMapper implements FeatureMapper {
    private static final ServiceLoader DEFAULT_LOADER =
            new ServiceLoader(FeatureMapper.class.getClassLoader());

    List<FeatureMapper> mappers;

    public CompositeFeatureMapper() {
        mappers = DEFAULT_LOADER.loadServiceProviders(FeatureMapper.class);
    }

    @Override
    public void addFeatures(ResultSet resultSet, Path rootDir,
                            StoredDocument storedDocument) throws SQLException {
        for (FeatureMapper mapper : mappers) {
            mapper.addFeatures(resultSet, rootDir, storedDocument);
        }
    }
}
