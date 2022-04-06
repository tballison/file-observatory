package org.tallison.tika.parser;

import java.util.ArrayList;
import java.util.List;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.FetchEmitTuple;

public class ConcatenatingParser {

    private List<TikaServerClient> parsers = new ArrayList<>();

    public List<Metadata> parse(FetchEmitTuple tuple) {
        List<Metadata> results = new ArrayList<>();

        return results;
    }
}
