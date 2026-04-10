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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for RackBlacklistService.
 */
public class TestRackBlacklistService {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ZK_PATH = "/ledgers/blacklisted-racks";

    private ZooKeeper zkClient;
    private BookKeeperAdmin bka;
    private ServerConfiguration conf;
    private RackBlacklistService service;

    @Before
    public void setUp() throws Exception {
        zkClient = mock(ZooKeeper.class);
        bka = mock(BookKeeperAdmin.class);

        conf = new ServerConfiguration();
        conf.setZkLedgersRootPath("/ledgers");

        service = new RackBlacklistService(conf, bka) {
            @Override
            protected ZooKeeper getZooKeeperClientFromAdmin() {
                return zkClient;
            }
        };
    }

    @Test
    public void testGetEmptyBlacklist() throws Exception {
        when(zkClient.exists(eq(ZK_PATH), anyBoolean())).thenReturn(null);

        HttpServiceRequest request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.GET);

        HttpServiceResponse response = service.handle(request);

        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        @SuppressWarnings("unchecked")
        Map<String, Object> result = MAPPER.readValue(response.getBody(), Map.class);
        assertNotNull(result.get("racks"));
        assertTrue(((java.util.List) result.get("racks")).isEmpty());
    }

    @Test
    public void testGetExistingBlacklist() throws Exception {
        String blacklist = "/rack1,/rack2,/rack3";
        when(zkClient.exists(eq(ZK_PATH), anyBoolean())).thenReturn(new Stat());
        when(zkClient.getData(eq(ZK_PATH), anyBoolean(), any())).thenReturn(blacklist.getBytes(StandardCharsets.UTF_8));

        HttpServiceRequest request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.GET);

        HttpServiceResponse response = service.handle(request);

        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        @SuppressWarnings("unchecked")
        Map<String, Object> result = MAPPER.readValue(response.getBody(), Map.class);
        @SuppressWarnings("unchecked")
        java.util.List<String> racks = (java.util.List<String>) result.get("racks");
        assertEquals(3, racks.size());
        assertTrue(racks.contains("/rack1"));
        assertTrue(racks.contains("/rack2"));
        assertTrue(racks.contains("/rack3"));
    }

    @Test
    public void testPutBlacklist() throws Exception {
        when(zkClient.exists(eq(ZK_PATH), anyBoolean())).thenReturn(null);
        when(zkClient.create(eq(ZK_PATH), any(byte[].class), any(), any(CreateMode.class))).thenReturn(ZK_PATH);

        HttpServiceRequest request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.PUT);
        request.setBody("{\"racks\": [\"/rack1\", \"/rack2\"]}");

        HttpServiceResponse response = service.handle(request);

        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        @SuppressWarnings("unchecked")
        Map<String, Object> result = MAPPER.readValue(response.getBody(), Map.class);
        assertEquals(true, result.get("success"));
    }

    @Test
    public void testPutBlacklistOverwrite() throws Exception {
        when(zkClient.exists(eq(ZK_PATH), anyBoolean())).thenReturn(new Stat());
        when(zkClient.setData(eq(ZK_PATH), any(byte[].class), eq(-1))).thenReturn(new Stat());

        HttpServiceRequest request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.PUT);
        request.setBody("{\"racks\": [\"/rack3\", \"/rack4\"]}");

        HttpServiceResponse response = service.handle(request);

        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        @SuppressWarnings("unchecked")
        Map<String, Object> result = MAPPER.readValue(response.getBody(), Map.class);
        assertEquals(true, result.get("success"));
    }

    @Test
    public void testDeleteClearAll() throws Exception {
        when(zkClient.exists(eq(ZK_PATH), anyBoolean())).thenReturn(new Stat());
        doNothing().when(zkClient).delete(eq(ZK_PATH), eq(-1));

        HttpServiceRequest request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.DELETE);
        // No body means clear all

        HttpServiceResponse response = service.handle(request);

        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        @SuppressWarnings("unchecked")
        Map<String, Object> result = MAPPER.readValue(response.getBody(), Map.class);
        assertEquals(true, result.get("success"));
        assertTrue(result.get("message").toString().contains("cleared"));
    }

    @Test
    public void testPutBlacklistWithoutBody() throws Exception {
        HttpServiceRequest request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.PUT);
        // No body

        HttpServiceResponse response = service.handle(request);

        assertEquals(HttpServer.StatusCode.BAD_REQUEST.getValue(), response.getStatusCode());
        assertTrue(response.getBody().contains("Request body required"));
    }

    @Test
    public void testPutBlacklistInvalidFormat() throws Exception {
        HttpServiceRequest request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.PUT);
        request.setBody("{\"invalid\": \"field\"}");

        HttpServiceResponse response = service.handle(request);

        assertEquals(HttpServer.StatusCode.BAD_REQUEST.getValue(), response.getStatusCode());
        assertTrue(response.getBody().contains("'racks' field required"));
    }
}
