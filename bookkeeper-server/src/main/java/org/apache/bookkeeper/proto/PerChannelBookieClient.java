/**
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
package org.apache.bookkeeper.proto;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperClientStats;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.CorruptedFrameException;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages all details of connection to a particular bookie. It also
 * has reconnect logic if a connection to a bookie fails.
 *
 */
public class PerChannelBookieClient extends SimpleChannelHandler implements ChannelPipelineFactory {

    static final Logger LOG = LoggerFactory.getLogger(PerChannelBookieClient.class);

    public static final int MAX_FRAME_LENGTH = 2 * 1024 * 1024; // 2M

    final BookieSocketAddress addr;
    final ClientSocketChannelFactory channelFactory;
    final OrderedSafeExecutor executor;
    final HashedWheelTimer requestTimer;
    final int addEntryTimeout;
    final int readEntryTimeout;

    ConcurrentHashMap<CompletionKey, AddCompletion> addCompletions = new ConcurrentHashMap<CompletionKey, AddCompletion>();
    ConcurrentHashMap<CompletionKey, ReadCompletion> readCompletions = new ConcurrentHashMap<CompletionKey, ReadCompletion>();

    private final StatsLogger statsLogger;
    private final OpStatsLogger readEntryOpLogger;
    private final OpStatsLogger readTimeoutOpLogger;
    private final OpStatsLogger addEntryOpLogger;
    private final OpStatsLogger addTimeoutOpLogger;

    /**
     * The following member variables do not need to be concurrent, or volatile
     * because they are always updated under a lock
     */
    private volatile Queue<GenericCallback<PerChannelBookieClient>> pendingOps =
            new ArrayDeque<GenericCallback<PerChannelBookieClient>>();
    volatile Channel channel = null;

    enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED, CLOSED
    }

    volatile ConnectionState state;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
    private final ClientConfiguration conf;

    public PerChannelBookieClient(OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                                  BookieSocketAddress addr) {
        this(new ClientConfiguration(), executor, channelFactory, addr, null, NullStatsLogger.INSTANCE);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedSafeExecutor executor,
                                  ClientSocketChannelFactory channelFactory, BookieSocketAddress addr,
                                  HashedWheelTimer requestTimer, StatsLogger parentStatsLogger) {
        this.conf = conf;
        this.addr = addr;
        this.executor = executor;
        this.channelFactory = channelFactory;
        this.state = ConnectionState.DISCONNECTED;
        this.requestTimer = requestTimer;
        this.addEntryTimeout = conf.getAddEntryTimeout();
        this.readEntryTimeout = conf.getReadEntryTimeout();

        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(addr.getHostname().replace('.', '_').replace('-', '_'))
            .append("_").append(addr.getPort());

        this.statsLogger = parentStatsLogger.scope(BookKeeperClientStats.CHANNEL_SCOPE)
            .scope(nameBuilder.toString());

        readEntryOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_READ_OP);
        addEntryOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_ADD_OP);
        readTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_READ);
        addTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_ADD);
    }

    private void completeOperation(GenericCallback<PerChannelBookieClient> op, int rc) {
        closeLock.readLock().lock();
        try {
            if (ConnectionState.CLOSED == state) {
                op.operationComplete(BKException.Code.ClientClosedException, this);
            } else {
                op.operationComplete(rc, this);
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    private void connect() {
        LOG.info("Connecting to bookie: {}", addr);

        // Set up the ClientBootStrap so we can create a new Channel connection
        // to the bookie.
        ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
        bootstrap.setPipelineFactory(this);
        bootstrap.setOption("tcpNoDelay", conf.getClientTcpNoDelay());
        bootstrap.setOption("keepAlive", true);

        ChannelFuture future = bootstrap.connect(addr.getSocketAddress());
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOG.debug("Channel connected ({}) {}", future.isSuccess(), future.getChannel());
                int rc;
                Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;

                synchronized (PerChannelBookieClient.this) {
                    if (future.isSuccess() && state == ConnectionState.CONNECTING) {
                        LOG.info("Successfully connected to bookie: {}", future.getChannel());
                        rc = BKException.Code.OK;
                        channel = future.getChannel();
                        state = ConnectionState.CONNECTED;
                    } else if (future.isSuccess() && (state == ConnectionState.CLOSED
                                                      || state == ConnectionState.DISCONNECTED)) {
                        LOG.warn("Closed before connection completed, clean up: {}, current state {}",
                                 future.getChannel(), state);
                        closeChannel(future.getChannel());
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        channel = null;
                    } else if (future.isSuccess() && state == ConnectionState.CONNECTED) {
                        LOG.debug("Already connected with another channel({}), so close the new channel({})",
                                  channel, future.getChannel());
                        closeChannel(future.getChannel());
                        return; // pendingOps should have been completed when other channel connected
                    } else {
                        LOG.error("Could not connect to bookie: {}/{}, current state {} : ",
                                  new Object[] { future.getChannel(), addr,
                                                 state, future.getCause() });
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        closeChannel(future.getChannel());
                        channel = null;
                        if (state != ConnectionState.CLOSED) {
                            state = ConnectionState.DISCONNECTED;
                        }
                    }

                    // trick to not do operations under the lock, take the list
                    // of pending ops and assign it to a new variable, while
                    // emptying the pending ops by just assigning it to a new
                    // list
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<GenericCallback<PerChannelBookieClient>>();
                }

                for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
                    completeOperation(pendingOp, rc);
                }
            }
        });
    }

    void connectIfNeededAndDoOp(GenericCallback<PerChannelBookieClient> op) {
        boolean completeOpNow = false;
        int opRc = BKException.Code.OK;
        // common case without lock first
        if (channel != null && state == ConnectionState.CONNECTED) {
            completeOpNow = true;
        } else {

            synchronized (this) {
                // check the channel status again under lock
                if (channel != null && state == ConnectionState.CONNECTED) {
                    completeOpNow = true;
                    opRc = BKException.Code.OK;
                } else if (state == ConnectionState.CLOSED) {
                    completeOpNow = true;
                    opRc = BKException.Code.BookieHandleNotAvailableException;
                } else {
                    // channel is either null (first connection attempt), or the
                    // channel is disconnected. Connection attempt is still in
                    // progress, queue up this op. Op will be executed when
                    // connection attempt either fails or succeeds
                    pendingOps.add(op);

                    if (state == ConnectionState.CONNECTING) {
                        // just return as connection request has already send
                        // and waiting for the response.
                        return;
                    }
                    // switch state to connecting and do connection attempt
                    state = ConnectionState.CONNECTING;
                }
            }
            if (!completeOpNow) {
                // Start connection attempt to the input server host.
                connect();
            }
        }

        if (completeOpNow) {
            completeOperation(op, opRc);
        }

    }

    /**
     * This method should be called only after connection has been checked for
     * {@link #connectIfNeededAndDoOp(GenericCallback)}
     *
     * @param ledgerId
     *          Ledger Id
     * @param masterKey
     *          Master Key
     * @param entryId
     *          Entry Id
     * @param toSend
     *          Buffer to send
     * @param cb
     *          Write callback
     * @param ctx
     *          Write callback context
     * @param options
     *          Add options
     */
    void addEntry(final long ledgerId, byte[] masterKey, final long entryId, ChannelBuffer toSend, WriteCallback cb,
                  Object ctx, final int options) {
        BookieProtocol.AddRequest r = new BookieProtocol.AddRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION,
                ledgerId, entryId, (short)options, masterKey, toSend);
        final int entrySize = toSend.readableBytes();
        final CompletionKey completionKey = new CompletionKey(ledgerId, entryId, BookieProtocol.ADDENTRY);
        addCompletions.put(completionKey,
                new AddCompletion(addEntryOpLogger, cb, ctx, scheduleTimeout(completionKey, addEntryTimeout)));
        final Channel c = channel;
        if (c == null) {
            errorOutAddKey(completionKey);
            return;
        }
        try {
            ChannelFuture future = c.write(r);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request for adding entry: " + entryId + " ledger-id: " + ledgerId
                                                            + " bookie: " + c.getRemoteAddress() + " entry length: " + entrySize);
                        }
                        // totalBytesOutstanding.addAndGet(entrySize);
                    } else {
                        if (!(future.getCause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing addEntry(lid={}, eid={}) to channel {} failed : ",
                                    new Object[] { ledgerId, entryId, c, future.getCause() });
                        }
                        errorOutAddKey(completionKey);
                    }
                }
            });
        } catch (Throwable e) {
            LOG.warn("Add entry operation failed", e);
            errorOutAddKey(completionKey);
        }
    }

    public void readEntryAndFenceLedger(final long ledgerId, byte[] masterKey,
                                        final long entryId,
                                        ReadEntryCallback cb, Object ctx) {
        final CompletionKey key = new CompletionKey(ledgerId, entryId, BookieProtocol.READENTRY);
        readCompletions.put(key,
                new ReadCompletion(readEntryOpLogger, cb, ctx, scheduleTimeout(key, readEntryTimeout)));

        final BookieProtocol.ReadRequest r = new BookieProtocol.ReadRequest(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId,
                BookieProtocol.FLAG_DO_FENCING, masterKey);

        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(key);
            return;
        }

        try {
            ChannelFuture future = c.write(r);
            future.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Successfully wrote request {} to {}",
                                          r, c.getRemoteAddress());
                            }
                        } else {
                            if (!(future.getCause() instanceof ClosedChannelException)) {
                                LOG.warn("Writing readEntryAndFenceLedger(lid={}, eid={}) to channel {} failed : ",
                                        new Object[] { ledgerId, entryId, c, future.getCause() });
                            }
                            errorOutReadKey(key);
                        }
                    }
                });
        } catch(Throwable e) {
            LOG.warn("Read entry operation " + r + " failed", e);
            errorOutReadKey(key);
        }
    }

    public void readEntry(final long ledgerId, final long entryId, ReadEntryCallback cb, Object ctx) {
        final CompletionKey key = new CompletionKey(ledgerId, entryId, BookieProtocol.READENTRY);
        readCompletions.put(key,
                new ReadCompletion(readEntryOpLogger, cb, ctx, scheduleTimeout(key, readEntryTimeout)));

        final BookieProtocol.ReadRequest r = new BookieProtocol.ReadRequest(
                BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId,
                BookieProtocol.FLAG_NONE);

        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(key);
            return;
        }

        try{
            ChannelFuture future = c.write(r);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request {} to {}",
                                      r, c.getRemoteAddress());
                        }
                    } else {
                        if (!(future.getCause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing readEntry(lid={}, eid={}) to channel {} failed : ",
                                    new Object[] { ledgerId, entryId, c, future.getCause() });
                        }
                        errorOutReadKey(key);
                    }
                }
            });
        } catch(Throwable e) {
            LOG.warn("Read entry operation " + r + " failed", e);
            errorOutReadKey(key);
        }
    }

    /**
     * Disconnects the bookie client. It can be reused.
     */
    public void disconnect() {
        closeInternal(false);
    }

    /**
     * Closes the bookie client permanently. It cannot be reused.
     */
    public void close() {
        closeLock.writeLock().lock();
        try {
            if (ConnectionState.CLOSED == state) {
                return;
            }
            state = ConnectionState.CLOSED;
            errorOutOutstandingEntries(BKException.Code.ClientClosedException);
        } finally {
            closeLock.writeLock().unlock();
        }
        closeInternal(true);
    }

    private void closeInternal(boolean permanent) {
        Channel toClose = null;
        synchronized (this) {
            if (permanent) {
                state = ConnectionState.CLOSED;
            } else if (state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
            }
            toClose = channel;
            channel = null;
        }
        if (toClose != null) {
            closeChannel(toClose).awaitUninterruptibly();
        }
    }

    private ChannelFuture closeChannel(Channel c) {
        LOG.debug("Closing channel {}", c);
        ReadTimeoutHandler timeout = c.getPipeline().get(ReadTimeoutHandler.class);
        if (timeout != null) {
            timeout.releaseExternalResources();
        }
        return c.close();
    }

    void errorOutReadKey(final CompletionKey key) {
        errorOutReadKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutReadKey(final CompletionKey key, final int rc) {
        executor.submitOrdered(key.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {

                ReadCompletion readCompletion = readCompletions.remove(key);
                String bAddress = "null";
                Channel c = channel;
                if(c != null) {
                    bAddress = c.getRemoteAddress().toString();
                }

                if (readCompletion != null) {
                    LOG.debug("Could not write request for reading entry: {}"
                              + " ledger-id: {} bookie: {}",
                              new Object[] { key.entryId, key.ledgerId, bAddress });

                    readCompletion.cb.readEntryComplete(rc,
                                                        key.ledgerId, key.entryId, null, readCompletion.ctx);
                }
            }

        });
    }

    void errorOutAddKey(final CompletionKey key) {
        errorOutAddKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutAddKey(final CompletionKey key, final int rc) {
        executor.submitOrdered(key.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {

                AddCompletion addCompletion = addCompletions.remove(key);

                if (addCompletion != null) {
                    String bAddress = "null";
                    Channel c = channel;
                    if(c != null) {
                        bAddress = c.getRemoteAddress().toString();
                    }
                    LOG.debug("Could not write request for adding entry: {} ledger-id: {} bookie: {}",
                              new Object[] { key.entryId, key.ledgerId, bAddress });

                    addCompletion.cb.writeComplete(rc, key.ledgerId,
                                                   key.entryId, addr, addCompletion.ctx);
                    LOG.debug("Invoked callback method: {}", key.entryId);
                }
            }

        });

    }

    /**
     * Errors out pending entries. We call this method from one thread to avoid
     * concurrent executions to QuorumOpMonitor (implements callbacks). It seems
     * simpler to call it from BookieHandle instead of calling directly from
     * here.
     */

    void errorOutOutstandingEntries(int rc) {

        // DO NOT rewrite these using Map.Entry iterations. We want to iterate
        // on keys and see if we are successfully able to remove the key from
        // the map. Because the add and the read methods also do the same thing
        // in case they get a write failure on the socket. The one who
        // successfully removes the key from the map is the one responsible for
        // calling the application callback.

        for (CompletionKey key : addCompletions.keySet()) {
            errorOutAddKey(key, rc);
        }

        for (CompletionKey key : readCompletions.keySet()) {
            errorOutReadKey(key, rc);
        }
    }

    /**
     * In the netty pipeline, we need to split packets based on length, so we
     * use the {@link LengthFieldBasedFrameDecoder}. Other than that all actions
     * are carried out in this class, e.g., making sense of received messages,
     * prepending the length to outgoing packets etc.
     */
    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();

        pipeline.addLast("lengthbasedframedecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
        pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.RequestEncoder());
        pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.ResponseDecoder());


        pipeline.addLast("mainhandler", this);
        return pipeline;
    }

    /**
     * If our channel has disconnected, we just error out the pending entries
     */
    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        Channel c = ctx.getChannel();
        LOG.info("Disconnected from bookie channel {}", c);
        if (c != null) {
            closeChannel(c);
        }

        errorOutOutstandingEntries(BKException.Code.BookieHandleNotAvailableException);

        synchronized (this) {
            if (this.channel == c
                && state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
            }
        }

        // we don't want to reconnect right away. If someone sends a request to
        // this address, we will reconnect.
    }

    /**
     * Called by netty when an exception happens in one of the netty threads
     * (mostly due to what we do in the netty threads)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Throwable t = e.getCause();
        if (t instanceof CorruptedFrameException || t instanceof TooLongFrameException) {
            LOG.error("Corrupted frame received from bookie: {}",
                      e.getChannel().getRemoteAddress());
            return;
        }

        if (t instanceof IOException) {
            // these are thrown when a bookie fails, logging them just pollutes
            // the logs (the failure is logged from the listeners on the write
            // operation), so I'll just ignore it here.
            return;
        }

        synchronized (this) {
            if (state == ConnectionState.CLOSED) {
                LOG.debug("Unexpected exception caught by bookie client channel handler, "
                          + "but the client is closed, so it isn't important", t);
            } else {
                LOG.error("Unexpected exception caught by bookie client channel handler", t);
            }
        }
        // Since we are a library, cant terminate App here, can we?
    }

    /**
     * Called by netty when a message is received on a channel
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if (!(e.getMessage() instanceof BookieProtocol.Response)) {
            ctx.sendUpstream(e);
            return;
        }
        final BookieProtocol.Response r = (BookieProtocol.Response)e.getMessage();

        executor.submitOrdered(r.getLedgerId(), new SafeRunnable() {
            @Override
            public void safeRun() {
                switch (r.getOpCode()) {
                case BookieProtocol.ADDENTRY:
                    BookieProtocol.AddResponse a = (BookieProtocol.AddResponse)r;
                    handleAddResponse(a);
                    break;
                case BookieProtocol.READENTRY:
                    BookieProtocol.ReadResponse rr = (BookieProtocol.ReadResponse)r;
                    handleReadResponse(rr);
                    break;
                default:
                    LOG.error("Unexpected response, type: {}", r);
                }
            }
        });
    }

    void handleAddResponse(BookieProtocol.AddResponse a) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for add request from bookie: {} for ledger: {}", addr, a);
        }

        // convert to BKException code because thats what the uppper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        int rc = BKException.Code.WriteException;
        switch (a.getErrorCode()) {
        case BookieProtocol.EOK:
            rc = BKException.Code.OK;
            break;
        case BookieProtocol.EBADVERSION:
            rc = BKException.Code.ProtocolVersionException;
            break;
        case BookieProtocol.EFENCED:
            rc = BKException.Code.LedgerFencedException;
            break;
        case BookieProtocol.EUA:
            rc = BKException.Code.UnauthorizedAccessException;
            break;
        case BookieProtocol.EREADONLY:
            rc = BKException.Code.WriteOnReadOnlyBookieException;
            break;
        default:
            LOG.warn("Add failed {}", a);
            rc = BKException.Code.WriteException;
            break;
        }

        AddCompletion ac;
        ac = addCompletions.remove(new CompletionKey(a.getLedgerId(),
                                                     a.getEntryId(), BookieProtocol.ADDENTRY));
        if (ac == null) {
            LOG.debug("Unexpected add response from bookie {} for {}", addr, a);
            return;
        }

        ac.cb.writeComplete(rc, a.getLedgerId(), a.getEntryId(), addr, ac.ctx);
    }

    void handleReadResponse(BookieProtocol.ReadResponse rr) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for read request {} entry length: {}", rr,
                    rr.getData() != null ? rr.getData().readableBytes() : -1);
        }

        // convert to BKException code because thats what the uppper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        int rc = BKException.Code.ReadException;
        switch (rr.getErrorCode()) {
        case BookieProtocol.EOK:
            rc = BKException.Code.OK;
            break;
        case BookieProtocol.ENOENTRY:
        case BookieProtocol.ENOLEDGER:
            rc = BKException.Code.NoSuchEntryException;
            break;
        case BookieProtocol.EBADVERSION:
            rc = BKException.Code.ProtocolVersionException;
            break;
        case BookieProtocol.EUA:
            rc = BKException.Code.UnauthorizedAccessException;
            break;
        default:
            LOG.warn("Read error for {}", rr);
            rc = BKException.Code.ReadException;
            break;
        }

        CompletionKey key = new CompletionKey(rr.getLedgerId(), rr.getEntryId(), BookieProtocol.READENTRY);
        ReadCompletion readCompletion = readCompletions.remove(key);

        if (readCompletion == null) {
            /*
             * This is a special case. When recovering a ledger, a client
             * submits a read request with id -1, and receives a response with a
             * different entry id.
             */

            readCompletion = readCompletions.remove(new CompletionKey(rr.getLedgerId(),
                                                                      BookieProtocol.LAST_ADD_CONFIRMED,
                                                                      BookieProtocol.READENTRY));
        }

        if (readCompletion == null) {
            LOG.debug("Unexpected read response received from bookie: {} for {}", addr, rr);
            return;
        }

        readCompletion.cb.readEntryComplete(rc, rr.getLedgerId(), rr.getEntryId(),
                                            rr.getData(), readCompletion.ctx);
    }

    /**
     * Boiler-plate wrapper classes follow
     *
     */

    // visible for testing
    static abstract class CompletionValue {
        final Object ctx;
        protected final Timeout timeout;

        public CompletionValue(Object ctx, Timeout timeout) {
            this.ctx = ctx;
            this.timeout = timeout;
        }

        void cancelTimeout() {
            if (null != timeout) {
                timeout.cancel();
            }
        }
    }

    // visible for testing
    static class ReadCompletion extends CompletionValue {
        final ReadEntryCallback cb;

        public ReadCompletion(ReadEntryCallback cb, Object ctx) {
            this(null, cb, ctx, null);
        }

        public ReadCompletion(final OpStatsLogger readEntryOpLogger,
                              final ReadEntryCallback originalCallback,
                              final Object originalCtx, final Timeout timeout) {
            super(originalCtx, timeout);
            final long requestTimeMillis = MathUtils.now();
            this.cb = null == readEntryOpLogger ? originalCallback : new ReadEntryCallback() {
                @Override
                public void readEntryComplete(int rc, long ledgerId, long entryId, ChannelBuffer buffer, Object ctx) {
                    cancelTimeout();
                    long latencyMillis = MathUtils.now() - requestTimeMillis;
                    if (rc != BKException.Code.OK) {
                        readEntryOpLogger.registerFailedEvent(latencyMillis);
                    } else {
                        readEntryOpLogger.registerSuccessfulEvent(latencyMillis);
                    }
                    originalCallback.readEntryComplete(rc, ledgerId, entryId, buffer, originalCtx);
                }
            };
        }
    }

    // visible for testing
    static class AddCompletion extends CompletionValue {
        final WriteCallback cb;

        public AddCompletion(WriteCallback cb, Object ctx) {
            this(null, cb, ctx, null);
        }

        public AddCompletion(final OpStatsLogger addEntryOpLogger,
                             final WriteCallback originalCallback,
                             final Object originalCtx,
                             final Timeout timeout) {
            super(originalCtx, timeout);
            final long requestTimeMillis = MathUtils.now();
            this.cb = null == addEntryOpLogger ? originalCallback : new WriteCallback() {
                @Override
                public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                    cancelTimeout();
                    long latencyMillis = MathUtils.now() - requestTimeMillis;
                    if (rc != BKException.Code.OK) {
                        addEntryOpLogger.registerFailedEvent(latencyMillis);
                    } else {
                        addEntryOpLogger.registerSuccessfulEvent(latencyMillis);
                    }
                    originalCallback.writeComplete(rc, ledgerId, entryId, addr, originalCtx);
                }
            };
        }
    }

    // visable for testing
    CompletionKey newCompletionKey(long ledgerId, long entryId, byte operationType) {
        return new CompletionKey(ledgerId, entryId, operationType);
    }

    Timeout scheduleTimeout(CompletionKey key, long timeout) {
        if (null != requestTimer) {
            return requestTimer.newTimeout(key, timeout, TimeUnit.SECONDS);
        } else {
            return null;
        }
    }

    class CompletionKey implements TimerTask {
        final long ledgerId;
        final long entryId;
        final long requestAt;
        final byte operationType;

        CompletionKey(long ledgerId, long entryId, byte opType) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.requestAt = MathUtils.nowInNano();
            this.operationType = opType;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CompletionKey)) {
                return false;
            }
            CompletionKey that = (CompletionKey) obj;
            return this.ledgerId == that.ledgerId && this.entryId == that.entryId;
        }

        @Override
        public int hashCode() {
            return ((int) ledgerId << 16) ^ ((int) entryId);
        }

        @Override
        public String toString() {
            return String.format("LedgerEntry(%d, %d)", ledgerId, entryId);
        }

        private long elapsedTime() {
            return MathUtils.elapsedMSec(requestAt);
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }
            if (BookieProtocol.ADDENTRY == operationType) {
                errorOutAddKey(this);
                addTimeoutOpLogger.registerSuccessfulEvent(elapsedTime());
            } else {
                errorOutReadKey(this);
                readTimeoutOpLogger.registerSuccessfulEvent(elapsedTime());
            }
        }
    }

}
