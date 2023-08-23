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
import org.apache.bookkeeper.bookie.DiskCacheDownGradeStatus;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.proto.BookieServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * HttpEndpointService that handle get disk cache writing details service.
 *
 * <p>the output would be like:
 *        [ {
 *           "ledgerDir" : /data1/bookkeeper/ledgers,
 *           "coldLedgerDir" : /data1/bookkeeper/coldLedgers,
 *           "diskCacheDowngrading" : true
 *         } ]
 */
public class DiskCacheStatusService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(DiskCacheStatusService.class);

    protected ServerConfiguration conf;
    protected BookieServer bookieServer;

    public DiskCacheStatusService(ServerConfiguration conf, BookieServer bookieServer) {
        checkNotNull(conf);
        checkNotNull(bookieServer);
        this.conf = conf;
        this.bookieServer = bookieServer;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        if (HttpServer.Method.GET == request.getMethod()) {
            List<DiskCacheDownGradeStatus> details = bookieServer.getBookie()
                .getLedgerStorage().getDiskCacheDowngradeStatus();

            String jsonResponse = JsonUtil.toJson(details);
            if (LOG.isDebugEnabled()) {
                LOG.debug("output body:" + jsonResponse);
            }
            response.setBody(jsonResponse);
            response.setCode(HttpServer.StatusCode.OK);
            return response;
        } else {
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Only support GET method to retrieve disk cache writing status.");
            return response;
        }
    }
}
