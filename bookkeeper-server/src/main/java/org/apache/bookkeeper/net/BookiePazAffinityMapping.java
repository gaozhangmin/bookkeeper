/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.net;

import static java.util.Objects.requireNonNull;
import static org.apache.bookkeeper.conf.AbstractConfiguration.IGNORE_LOCAL_NODE_IN_PLACEMENT_POLICY;
import static org.apache.commons.lang3.StringUtils.isBlank;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Data;
import org.apache.bookkeeper.client.DefaultBookieAddressResolver;
import org.apache.bookkeeper.client.ITopologyAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackChangeNotifier;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.util.FutureUtil;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookiePazAffinityMapping extends AbstractDNSToSwitchMapping implements RackChangeNotifier {
    private static final Logger LOG = LoggerFactory.getLogger(BookiePazAffinityMapping.class);

    private static final ExecutorService EXECUTORS = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("paz-affinity-executors-%d").setDaemon(true).build());
    private static final BookieKwsInfo NULL_KWS_INFO = new BookieKwsInfo(null, null);

    private volatile Map<String, BookieKwsInfo> bookieKwsInfoMap = new ConcurrentHashMap<>();
    private RegistrationClient registrationClient;
    private ITopologyAwareEnsemblePlacementPolicy<BookieNode> rackawarePolicy = null;
    private String localHostname;
    private String localAddress;
    private String localRack;
    private boolean ignoreLocalNodeInPlacementPolicy;

    @Override
    public synchronized void setConf(Configuration conf) {
        super.setConf(conf);
        ignoreLocalNodeInPlacementPolicy = conf.getBoolean(IGNORE_LOCAL_NODE_IN_PLACEMENT_POLICY, false);
        if (!ignoreLocalNodeInPlacementPolicy) {
            try {
                localHostname = InetAddress.getLocalHost().getCanonicalHostName();
                localAddress = InetAddress.getLocalHost().getHostAddress();
                localRack = rack(System.getenv("KWS_SERVICE_REGION"), System.getenv("KWS_SERVICE_PAZ"));
            } catch (Exception e) {
                throw new RuntimeException("Fail to get localNode place info", e);
            }
        }

        BookieAddressResolver bookieAddressResolver = getBookieAddressResolver();
        if (!(bookieAddressResolver instanceof DefaultBookieAddressResolver)) {
            throw new RuntimeException("Bookie address resolver is not an instance of DefaultBookieAddressResolver");
        }
        registrationClient = ((DefaultBookieAddressResolver) bookieAddressResolver).getRegistrationClient();
        try {
            Set<BookieId> writableBookies = registrationClient.getWritableBookies().get().getValue();
            updateKwsInfoMap(writableBookies);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("Failed to update rack info.", e);
        }

        registrationClient.watchWritableBookies(versionedBookies -> {
            EXECUTORS.submit(() -> {
                try {
                    Set<BookieId> writableBookies = versionedBookies.getValue();
                    updateKwsInfoMap(writableBookies);
                    if (rackawarePolicy != null) {
                        rackawarePolicy.onBookieRackChange(new ArrayList<>(writableBookies));
                    }
                } catch (Throwable throwable) {
                    LOG.error("Failed to update rack info.", throwable);
                }
            });
        });
    }

    @Override
    public List<String> resolve(List<String> addressList) {
        List<String> racks = new ArrayList<>(addressList.size());
        for (String address : addressList) {
            racks.add(getRack(address));
        }
        return racks;
    }

    @Override
    public boolean useHostName() {
        return false;
    }

    @Override
    public void registerRackChangeListener(ITopologyAwareEnsemblePlacementPolicy<BookieNode> rackawarePolicy) {
        this.rackawarePolicy = rackawarePolicy;
    }

    @Override
    public String toString() {
        return "paz based bookie rack affinity mapping";
    }

    @Override
    public void reloadCachedMappings() {
        // no-op
    }

    private void updateKwsInfoMap(Set<BookieId> bookieIds) throws ExecutionException, InterruptedException {
        Map<String, BookieKwsInfo> newBookieInfoMap = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>(bookieIds.size());
        for (BookieId bookieId : bookieIds) {
            futures.add(registrationClient.getBookieServiceInfo(bookieId)
                    .thenAccept(versionedBis -> {
                        BookieServiceInfo bsi = versionedBis.getValue();
                        BookieServiceInfo.Endpoint endpoint = bsi.getEndpoints().stream()
                                .filter(e -> e.getProtocol().equals("bookie-rpc"))
                                .findAny()
                                .orElse(null);
                        if (endpoint == null) {
                            LOG.error("bookie {} does not publish a bookie-rpc endpoint", bookieId);
                        } else {
                            newBookieInfoMap.put(endpoint.getHost(), createBookieKwsInfo(bsi));
                        }
                    }));

        }
        FutureUtil.waitForAll(futures).get();
        bookieKwsInfoMap = newBookieInfoMap;
    }

    private String getRack(String address) {
        if (!ignoreLocalNodeInPlacementPolicy && (address.equals(localAddress) || address.equals(localHostname))) {
            return localRack;
        }
        BookieKwsInfo kwsInfo = bookieKwsInfoMap.get(address);
        if (kwsInfo == null || kwsInfo == NULL_KWS_INFO) {
            // since different placement policy will have different default rack,
            // don't be smart here and just return null
            return null;
        }
        return rack(kwsInfo.getRegion(), kwsInfo.getPaz());
    }

    private String rack(String region, String paz) {
        return "/" + requireNonNull(region, "region is null") + "#" + requireNonNull(paz, "paz is null");
    }

    private BookieKwsInfo createBookieKwsInfo(BookieServiceInfo bsi) {
        Map<String, String> properties = bsi.getProperties();
        if (properties == null || properties.isEmpty()) {
            LOG.warn("Bookie service properties is empty. BookieServiceInfo: {}", bsi);
            return NULL_KWS_INFO;
        }
        String region = properties.get("KWS_SERVICE_REGION");
        if (isBlank(region)) {
            LOG.warn("Bookie service region is blank. BookieServiceInfo: {}", bsi);
            return NULL_KWS_INFO;
        }
        String paz = properties.get("KWS_SERVICE_PAZ");
        if (isBlank(paz)) {
            LOG.warn("Bookie service paz is blank. BookieServiceInfo: {}", bsi);
            return NULL_KWS_INFO;
        }
        return new BookieKwsInfo(region, paz);
    }

    @Data
    private static class BookieKwsInfo {
        private final String region;
        private final String paz;
    }
}
