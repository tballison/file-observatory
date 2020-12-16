/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.fileutils.tika;

import org.apache.commons.io.IOUtils;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.http.HTTPConduit;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Client for tika-server
 */
public class TikaServerClient {

    enum INPUT_METHOD {
        INPUTSTREAM, //classic use of tika-server the client will put the bytes
        FILE//POTENTIALLY INSECURE:
            // the client will send the file path for tika-server to read the file, rather
            //than putting the bytes.  For this to work, tika-server must be started with
            //-enableUnsecureFeatures -enableFileUrl
            //https://cwiki.apache.org/confluence/display/TIKA/TikaJAXRS#TikaJAXRS-SpecifyingaURLInsteadofPuttingBytes

    }

    private static final Logger LOG = LoggerFactory.getLogger(TikaServerClient.class);

    private final static String END_POINT = "/rmeta/text";
    private final static int MAX_TRIES = 3;
    private final static int TIMEOUT_SECONDS = 360; // seconds

    //if there are retries, what is the maximum amount of millis
    private final static long MAX_TIME_FOR_RETRIES_MILLIS = 30000;

    //how long to sleep each time if you couldn't connect to
    //the tika-server
    private final static long SLEEP_ON_NO_CONNECTION_MILLIS = 1000;

    private final List<WebClient> clients = new ArrayList<>();
    private final INPUT_METHOD inputMethod;
    private enum STATE {
        NO_RESPONSE_IO_EXCEPTION,
        RESPONSE_NON_200_STATUS,
        RESPONSE_BAD_REQUEST,
        RESPONSE_PARSE_EXCEPTION,
        RESPONSE_UNPROCESSABLE_EXCEPTION,
        RESPONSE_SERVER_UNAVAILABLE,
        RESPONSE_BAD_ENTITY,
        RESPONSE_SUCCESS,
        INTERRUPTED_EXCEPTION;
    }

    public TikaServerClient(INPUT_METHOD inputMethod, String ... urls) {
        this.inputMethod = inputMethod;
        for (String url : urls) {
            clients.add(buildClient(url));
        }
    }

    private static WebClient buildClient(String url) {
        WebClient client = WebClient.create(url+END_POINT);

        HTTPConduit conduit = WebClient.getConfig(client).getHttpConduit();
        conduit.getClient().setConnectionRequestTimeout(TIMEOUT_SECONDS*1000);
        conduit.getClient().setConnectionTimeout(TIMEOUT_SECONDS*1000);
        return client;
    }

    public List<Metadata> parse(String fileKey, TikaInputStream tis)
            throws IOException, TikaClientException {

        TikaServerResponse response = tryParse(fileKey, tis);

        int tries = 1;
        long retryStart = System.currentTimeMillis();
        long retryElapsed = 0;
        //TODO: clean up this logic
        while (response.state !=
                STATE.RESPONSE_PARSE_EXCEPTION &&
                response.state != STATE.RESPONSE_SUCCESS
                && tries++ < MAX_TRIES
                && retryElapsed < MAX_TIME_FOR_RETRIES_MILLIS) {
            LOG.warn(String.format(Locale.US,
                    "Retrying '%s' because of %s",
                    fileKey,
                    response.state.toString()));
            response = tryParse(fileKey, tis);
            //if there was no connection to the server,
            //decrement the tries and wait a bit before trying again
            if (response.state == STATE.NO_RESPONSE_IO_EXCEPTION) {
                tries--;
                try {
                    Thread.sleep(SLEEP_ON_NO_CONNECTION_MILLIS);
                } catch (InterruptedException e) {
                    throw new TikaClientException("interrupted", e);
                }
            }
            retryElapsed = System.currentTimeMillis() - retryStart;
        }
        if (response.state == STATE.RESPONSE_PARSE_EXCEPTION ||
            response.state == STATE.RESPONSE_SUCCESS) {
            return response.metadataList;
        }
        throw new TikaClientException(
                String.format(Locale.US,
                        "Couldn't parse file (%s); state=%s",
                        fileKey,
                        response.state.toString()));
    }

    private TikaServerResponse tryParse(String fileKey, TikaInputStream is) {
        int index = ThreadLocalRandom.current().nextInt(clients.size());
        WebClient client = clients.get(index);
        Response response = null;
        long start = System.currentTimeMillis();
        long elapsed = -1;
        try {
            if (inputMethod.equals(INPUT_METHOD.INPUTSTREAM)) {
                response = client.accept("application/json")
                        .put(is);
            } else {
                String p = is.getPath().toUri().toString();
                response = client.accept("application/json")
                        .header("fileUrl", p).put("");
            }
            elapsed = System.currentTimeMillis()-start;
            LOG.info("took "+(System.currentTimeMillis()-start) +" to get response");
        } catch (javax.ws.rs.ProcessingException|IOException e) {
            elapsed = System.currentTimeMillis()-start;
            LOG.warn("couldn't connect to tika-server", e);
            //server could be offline or a bad url
            return new TikaServerResponse(STATE.NO_RESPONSE_IO_EXCEPTION);
        }

        return getResponse(fileKey, response, elapsed);
    }

    private TikaServerResponse getResponse(String fileKey, Response response,
                                           long elapsedMillis) {

        int statusInt = response.getStatus();
        if (statusInt == 200 || statusInt == 422) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader((InputStream)response.getEntity(),
                    StandardCharsets.UTF_8))) {
                return new TikaServerResponse(STATE.RESPONSE_SUCCESS,
                        -1, "", JsonMetadataList.fromJson(reader));
            } catch (IOException | TikaException e) {
                LOG.warn("couldn't read http entity", e);
                return new TikaServerResponse(STATE.RESPONSE_BAD_ENTITY,
                        200, "", null);
            }
        } else if (statusInt == 415) {
            LOG.warn("unsupported media type: "+fileKey);
            //unsupported media type
            return new TikaServerResponse(STATE.RESPONSE_UNPROCESSABLE_EXCEPTION);
        } /*else if (statusInt == 422) {
            LOG.warn("parse exception: "+path);
            return new TikaServerResponse(STATE.RESPONSE_PARSE_EXCEPTION);
        }*/ else if (statusInt == 400) {
            LOG.warn("bad request: " + fileKey);
            return new TikaServerResponse(STATE.RESPONSE_BAD_REQUEST);
        } else if (statusInt == 503) {
            LOG.warn("server active, but not available: "+fileKey);
            //server is in shutdown mode
            return new TikaServerResponse(STATE.RESPONSE_SERVER_UNAVAILABLE);
        }
        String msg = null;
        try {
            msg = IOUtils.toString((InputStream)response.getEntity(), StandardCharsets.UTF_8.toString());
        } catch (IOException e) {
            //swallow
        }
        return new TikaServerResponse(STATE.RESPONSE_NON_200_STATUS, statusInt, msg);
    }

    private static class TikaServerResponse {

        private final STATE state;
        private final int status;
        private final String responseMsg;
        private final List<Metadata> metadataList;

        TikaServerResponse(STATE state) {
            this(state, -1, null, null);
        }

        TikaServerResponse(STATE state, int status, String responseMsg) {
            this(state, status, responseMsg, null);
        }

        TikaServerResponse(STATE state, int status,
                                  String responseMsg, List<Metadata> metadataList) {
            this.state = state;
            this.status = status;
            this.responseMsg = responseMsg;
            this.metadataList = metadataList;
        }
    }
}
