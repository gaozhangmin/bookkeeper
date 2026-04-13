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

package org.apache.bookkeeper.meta.zk;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DNS A-record based HostProvider that dynamically resolves host addresses.
 *
 * <p>Periodically refreshes the server list by querying DNS A records and uses
 * HostConnectionManager for connection management and reconfiguration logic.</p>
 *
 * <p><strong>Two-Phase Update Strategy:</strong></p>
 * <ul>
 * <li><strong>Phase 1 (Background):</strong> Timer thread detects DNS changes</li>
 * <li><strong>Phase 2 (Connection-time):</strong> Fresh DNS lookup during connect attempt if changes detected</li>
 * </ul>
 */
@InterfaceAudience.Public
public final class DnsSrvHostProvider implements HostProvider {
    private static final Logger LOG = LoggerFactory.getLogger(DnsSrvHostProvider.class);
    public static final long DNS_SRV_REFRESH_INTERVAL_SECONDS_DEFAULT = 60;

    public interface DnsARecordResolver {
        InetAddress[] lookupARecords(String hostname) throws UnknownHostException;
    }

    private final String hostname;
    private final int port;
    private final DnsARecordResolver aRecordResolver;

    private final HostConnectionManager connectionManager;
    private volatile Set<InetSocketAddress> previousServerSet;
    private Timer dnsRefreshTimer;

    private final AtomicReference<InetSocketAddress> currentConnectedHost = new AtomicReference<>();
    private final AtomicBoolean serverListChanged = new AtomicBoolean(false);

    /**
     * Constructs a DnsSrvHostProvider with the given hostname and port.
     *
     * @param hostname the hostname to resolve via A records
     * @param port     the port to use for all resolved addresses
     */
    public DnsSrvHostProvider(final String hostname, final int port) {
        this(hostname, port, null);
    }

    /**
     * Constructs a DnsSrvHostProvider with the given hostname, port and ZKClientConfig.
     *
     * @param hostname     the hostname to resolve via A records
     * @param port         the port to use for all resolved addresses
     * @param clientConfig ZooKeeper client configuration
     */
    public DnsSrvHostProvider(final String hostname, final int port, final ZKClientConfig clientConfig) {
        this(hostname, System.currentTimeMillis() ^ hostname.hashCode(), port, clientConfig);
    }

    /**
     * Constructs a DnsSrvHostProvider with randomness seed.
     *
     * @param hostname       the hostname to resolve via A records
     * @param randomnessSeed seed for randomization
     * @param port           the port to use for all resolved addresses
     * @param clientConfig   ZooKeeper client configuration
     */
    public DnsSrvHostProvider(final String hostname,
                              final long randomnessSeed,
                              final int port,
                              final ZKClientConfig clientConfig) {
        this(hostname, randomnessSeed, port, new DefaultARecordResolver(), clientConfig);
    }

    /**
     * Full constructor with custom resolver (primarily for testing).
     *
     * @param hostname        the hostname to resolve via A records
     * @param randomnessSeed  seed for randomization
     * @param port            the port to use for all resolved addresses
     * @param aRecordResolver custom A record resolver
     * @param clientConfig    ZooKeeper client configuration
     */
    public DnsSrvHostProvider(final String hostname,
                              final long randomnessSeed,
                              final int port,
                              final DnsARecordResolver aRecordResolver,
                              final ZKClientConfig clientConfig) {
        if (StringUtils.isBlank(hostname)) {
            throw new IllegalArgumentException("Hostname cannot be null or empty");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }

        this.hostname = hostname;
        this.port = port;
        this.aRecordResolver = aRecordResolver;
        try {
            final List<InetSocketAddress> serverAddresses = lookupAddresses();
            if (serverAddresses.isEmpty()) {
                LOG.error("No A records found for hostname: {}", hostname);
                throw new IllegalArgumentException("No A records found for hostname: " + hostname);
            }

            this.connectionManager = new HostConnectionManager(serverAddresses, randomnessSeed, clientConfig);
            this.previousServerSet = new HashSet<>(serverAddresses);

            final long refreshIntervalInSeconds = DNS_SRV_REFRESH_INTERVAL_SECONDS_DEFAULT;
            dnsRefreshTimer = new Timer("DnsRefresh-" + hostname, true);
            dnsRefreshTimer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    refreshServerListInBackground();
                }
            }, refreshIntervalInSeconds * 1000, refreshIntervalInSeconds * 1000);

            LOG.info("DnsSrvHostProvider initialized with {} servers from hostname: {}"
                            + " port: {} with refresh interval: {} seconds",
                    serverAddresses.size(), hostname, port, refreshIntervalInSeconds);
        } catch (final Exception e) {
            LOG.error("Failed to initialize DnsSrvHostProvider for hostname: {}", hostname, e);

            if (dnsRefreshTimer != null) {
                dnsRefreshTimer.cancel();
            }

            if (e instanceof IllegalArgumentException) {
                throw e;
            } else {
                throw new IllegalArgumentException(
                        "Failed to initialize DnsSrvHostProvider for hostname: " + hostname, e);
            }
        }
    }

    @Override
    public int size() {
        return connectionManager.size();
    }

    @Override
    public InetSocketAddress next(long spinDelay) {
        applyServerListUpdate();
        return connectionManager.next(spinDelay);
    }

    @Override
    public void onConnected() {
        currentConnectedHost.set(connectionManager.getServerAtCurrentIndex());
        connectionManager.onConnected();
    }

    @Override
    public boolean updateServerList(Collection<InetSocketAddress> serverAddresses, InetSocketAddress currentHost) {
        return connectionManager.updateServerList(serverAddresses, currentHost);
    }

    private List<InetSocketAddress> lookupAddresses() {
        try {
            final InetAddress[] inetAddresses = aRecordResolver.lookupARecords(hostname);
            if (inetAddresses == null || inetAddresses.length == 0) {
                throw new RuntimeException("A record lookup returned no addresses for " + hostname);
            }
            final List<InetSocketAddress> result = new ArrayList<>();
            for (final InetAddress inetAddress : inetAddresses) {
                result.add(new InetSocketAddress(inetAddress, port));
            }
            LOG.debug("Resolved {} to {} addresses with port {}", hostname, result.size(), port);
            return result;
        } catch (final UnknownHostException e) {
            LOG.error("A record lookup failed for {}", hostname, e);
            throw new RuntimeException("A record lookup failed for " + hostname, e);
        }
    }

    private void refreshServerListInBackground() {
        try {
            final List<InetSocketAddress> newAddresses = lookupAddresses();
            if (newAddresses.isEmpty()) {
                LOG.warn("DNS lookup returned no records for {}, will retry on next refresh", hostname);
                return;
            }

            final Set<InetSocketAddress> newServerSet = new HashSet<>(newAddresses);
            if (!Objects.equals(previousServerSet, newServerSet)) {
                serverListChanged.set(true);
                LOG.info("Server list change detected for {}: {} servers", hostname, newAddresses.size());
            }
        } catch (final Exception e) {
            LOG.warn("Failed to refresh server list for {}: {}", hostname, e.getMessage());
        }
    }

    private synchronized void applyServerListUpdate() {
        if (serverListChanged.get()) {
            try {
                final List<InetSocketAddress> latestServerList = lookupAddresses();
                if (latestServerList.isEmpty()) {
                    LOG.warn("DNS lookup returned no records for {}, will use the existing ones", hostname);
                    return;
                }

                final boolean needReconnect = connectionManager
                        .updateServerList(latestServerList, currentConnectedHost.get());
                previousServerSet = new HashSet<>(latestServerList);
                serverListChanged.set(false);

                LOG.info("Applied server list update for {}. servers size: {}, need reconnection: {}",
                        hostname, latestServerList.size(), needReconnect);
            } catch (final Exception e) {
                LOG.warn("Failed to apply server list update", e);
            }
        }
    }

    private static class DefaultARecordResolver implements DnsARecordResolver {
        @Override
        public InetAddress[] lookupARecords(final String hostname) throws UnknownHostException {
            return InetAddress.getAllByName(hostname);
        }
    }
}
