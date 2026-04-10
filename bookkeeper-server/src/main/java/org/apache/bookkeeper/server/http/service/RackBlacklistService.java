/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.server.http.service;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.ZKRegistrationClient;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpEndpointService to handle rack blacklist operations.
 *
 * <p>Supported operations:
 * - GET: Retrieve current rack blacklist
 * - PUT: Set rack blacklist (overwrite)
 * - DELETE: Remove racks from blacklist or clear all
 *
 * <p>Request body format (JSON):
 * {
 *   "racks": ["/rack1", "/rack2", "/rack3"]
 * }
 */
public class RackBlacklistService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(RackBlacklistService.class);

    public static final String ZK_BLACKLIST_PATH_SUFFIX = "/blacklisted-racks";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    protected ServerConfiguration conf;
    protected BookKeeperAdmin bka;

    public RackBlacklistService(ServerConfiguration conf, BookKeeperAdmin bka) {
        checkNotNull(conf);
        this.conf = conf;
        this.bka = bka;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        try {
            ZooKeeper zkClient = getZooKeeperClient();
            if (zkClient == null) {
                response.setCode(HttpServer.StatusCode.SERVICE_UNAVAILABLE);
                response.setBody("ZooKeeper client not available");
                return response;
            }

            String zkPath = getBlacklistZkPath();
            switch (request.getMethod()) {
                case GET:
                    return handleGet(zkClient, zkPath, response);
                case PUT:
                    return handlePut(zkClient, zkPath, request, response);
                case DELETE:
                    return handleDelete(zkClient, zkPath, request, response);
                default:
                    response.setCode(HttpServer.StatusCode.METHOD_NOT_ALLOWED);
                    response.setBody("Method not allowed. Supported: GET, PUT, DELETE");
                    return response;
            }
        } catch (Exception e) {
            LOG.error("Error handling rack blacklist request", e);
            response.setCode(HttpServer.StatusCode.INTERNAL_ERROR);
            response.setBody("Internal error: " + e.getMessage());
            return response;
        }
    }

    private HttpServiceResponse handleGet(ZooKeeper zkClient, String zkPath,
                                          HttpServiceResponse response) {
        try {
            if (zkClient.exists(zkPath, false) == null) {
                Map<String, Object> result = new HashMap<>();
                result.put("racks", new String[0]);
                result.put("message", "Blacklist node does not exist");
                response.setCode(HttpServer.StatusCode.OK);
                response.setBody(MAPPER.writeValueAsString(result));
                return response;
            }

            byte[] data = zkClient.getData(zkPath, false, null);
            String blacklist = data != null && data.length > 0
                    ? new String(data, StandardCharsets.UTF_8)
                    : "";

            String[] racks = blacklist.isEmpty() ? new String[0] : blacklist.split(",");
            Map<String, Object> result = new HashMap<>();
            result.put("racks", racks);
            response.setCode(HttpServer.StatusCode.OK);
            response.setBody(MAPPER.writeValueAsString(result));
            return response;
        } catch (Exception e) {
            LOG.error("Failed to get rack blacklist", e);
            response.setCode(HttpServer.StatusCode.INTERNAL_ERROR);
            response.setBody("Failed to get rack blacklist: " + e.getMessage());
            return response;
        }
    }

    private HttpServiceResponse handlePut(ZooKeeper zkClient, String zkPath,
                                          HttpServiceRequest request,
                                          HttpServiceResponse response) {
        try {
            String requestBody = request.getBody();
            if (requestBody == null || requestBody.isEmpty()) {
                response.setCode(HttpServer.StatusCode.BAD_REQUEST);
                response.setBody("Request body required. Format: {\"racks\": [\"/rack1\", \"/rack2\"]}");
                return response;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> body = MAPPER.readValue(requestBody, Map.class);
            Object racksObj = body.get("racks");

            if (racksObj == null) {
                response.setCode(HttpServer.StatusCode.BAD_REQUEST);
                response.setBody("'racks' field required in request body");
                return response;
            }

            String blacklist = null;
            if (racksObj instanceof String) {
                blacklist = (String) racksObj;
            } else if (racksObj instanceof Iterable) {
                blacklist = String.join(",", (Iterable<? extends CharSequence>) racksObj);
            } else {
                response.setCode(HttpServer.StatusCode.BAD_REQUEST);
                response.setBody("'racks' must be a string or array");
                return response;
            }

            setBlacklist(zkClient, zkPath, blacklist);

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("message", "Rack blacklist updated");
            result.put("racks", blacklist.split(","));

            response.setCode(HttpServer.StatusCode.OK);
            response.setBody(MAPPER.writeValueAsString(result));
            return response;
        } catch (Exception e) {
            LOG.error("Failed to set rack blacklist", e);
            response.setCode(HttpServer.StatusCode.INTERNAL_ERROR);
            response.setBody("Failed to set rack blacklist: " + e.getMessage());
            return response;
        }
    }

    private HttpServiceResponse handleDelete(ZooKeeper zkClient, String zkPath,
                                             HttpServiceRequest request,
                                             HttpServiceResponse response) {
        try {
            LOG.info("Removing all racks from blacklist");
            setBlacklist(zkClient, zkPath, "");
            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("message", "Rack blacklist cleared");
            result.put("racks", new String[0]);
            response.setCode(HttpServer.StatusCode.OK);
            response.setBody(MAPPER.writeValueAsString(result));
            return response;
        } catch (Exception e) {
            LOG.error("Failed to remove racks from blacklist", e);
            response.setCode(HttpServer.StatusCode.INTERNAL_ERROR);
            response.setBody("Failed to remove racks: " + e.getMessage());
            return response;
        }
    }

    private void setBlacklist(ZooKeeper zkClient, String zkPath, String blacklist) throws Exception {
        byte[] data = blacklist.getBytes(StandardCharsets.UTF_8);
        try {
            if (zkClient.exists(zkPath, false) == null) {
                zkClient.create(zkPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zkClient.setData(zkPath, data, -1);
            }
        } catch (KeeperException.NodeExistsException e) {
            // Race condition, try to update
            zkClient.setData(zkPath, data, -1);
        }
    }

    private ZooKeeper getZooKeeperClient() {
        return getZooKeeperClientFromAdmin();
    }

    protected ZooKeeper getZooKeeperClientFromAdmin() {
        if (bka == null || bka.getBookKeeper() == null) {
            return null;
        }
        MetadataClientDriver metadataDriver = bka.getBookKeeper().getMetadataClientDriver();
        return metadataDriver.getRegistrationClient()
                        instanceof ZKRegistrationClient
                        ? ((ZKRegistrationClient) metadataDriver
                                .getRegistrationClient()).getZk()
                        : null;
    }

    private String getBlacklistZkPath() {
        String zkLedgersRootPath = conf.getZkLedgersRootPath();
        if (zkLedgersRootPath == null || zkLedgersRootPath.isEmpty()) {
            zkLedgersRootPath = "/ledgers";
        }
        return zkLedgersRootPath + ZK_BLACKLIST_PATH_SUFFIX;
    }
}
