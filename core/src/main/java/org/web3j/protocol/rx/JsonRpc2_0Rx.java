package org.web3j.protocol.rx;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.Scheduler;
import io.reactivex.functions.Cancellable;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;
import org.reactivestreams.Publisher;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.DefaultBlockParameterNumber;
import org.web3j.protocol.core.filters.BlockFilter;
import org.web3j.protocol.core.filters.Callback;
import org.web3j.protocol.core.filters.LogFilter;
import org.web3j.protocol.core.filters.PendingTransactionFilter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthTransaction;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.core.methods.response.Transaction;
import org.web3j.utils.Flowables;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

/**
 * web3j reactive API implementation.
 */
public class JsonRpc2_0Rx {

    private final Web3j web3j;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Scheduler scheduler;

    public JsonRpc2_0Rx(Web3j web3j, ScheduledExecutorService scheduledExecutorService) {
        this.web3j = web3j;
        this.scheduledExecutorService = scheduledExecutorService;
        this.scheduler = Schedulers.from(scheduledExecutorService);
    }

    public Flowable<String> ethBlockHashFlowable(final long pollingInterval) {
        return Flowable.create(new FlowableOnSubscribe<String>() {
            @Override
            public void subscribe(final FlowableEmitter<String> subscriber) throws Exception {
                BlockFilter blockFilter = new BlockFilter(
                        web3j, new Callback<String>() {
                    @Override
                    public void onEvent(final String value) {
                        subscriber.onNext(value);
                    }
                });
                JsonRpc2_0Rx.this.run(blockFilter, subscriber, pollingInterval);
            }
        }, BackpressureStrategy.BUFFER);

    }

    public Flowable<String> ethPendingTransactionHashFlowable(final long pollingInterval) {
        return Flowable.create(new FlowableOnSubscribe<String>() {
            @Override
            public void subscribe(final FlowableEmitter<String> subscriber) throws Exception {
                PendingTransactionFilter pendingTransactionFilter = new PendingTransactionFilter(
                        web3j, new Callback<String>() {
                    @Override
                    public void onEvent(String value) {
                        subscriber.onNext(value);
                    }
                });

                run(pendingTransactionFilter, subscriber, pollingInterval);
            }
        }, BackpressureStrategy.BUFFER);
    }

    public Flowable<Log> ethLogFlowable(
            final org.web3j.protocol.core.methods.request.EthFilter ethFilter, final long pollingInterval) {
        return Flowable.create(new FlowableOnSubscribe<Log>() {
            @Override
            public void subscribe(final FlowableEmitter<Log> emitter) throws Exception {
                final LogFilter logFilter = new LogFilter(web3j, new Callback<Log>() {
                    @Override
                    public void onEvent(Log value) {
                        emitter.onNext(value);
                    }
                }, ethFilter);
                run(logFilter, emitter, pollingInterval);
            }
        }, BackpressureStrategy.BUFFER);
    }

    private <T> void run(
            final org.web3j.protocol.core.filters.Filter<T> filter, final FlowableEmitter<? super T> emitter,
            long pollingInterval) {

        filter.run(scheduledExecutorService, pollingInterval);
        emitter.setCancellable(new Cancellable() {
            @Override
            public void cancel() throws Exception {
                filter.cancel();
            }
        });
    }

    public Flowable<Transaction> transactionFlowable(long pollingInterval) {
        return blockFlowable(true, pollingInterval)
                .flatMapIterable(new Function<EthBlock, Iterable<Transaction>>() {
                    @Override
                    public Iterable<Transaction> apply(EthBlock ethBlock) throws Exception {
                        return JsonRpc2_0Rx.toTransactions(ethBlock);
                    }
                });
    }

    public Flowable<Transaction> pendingTransactionFlowable(long pollingInterval) {
        return ethPendingTransactionHashFlowable(pollingInterval)
                .flatMap(new Function<String, Publisher<EthTransaction>>() {
                    @Override
                    public Publisher<EthTransaction> apply(String transactionHash) throws Exception {
                        return web3j.ethGetTransactionByHash(transactionHash).flowable();
                    }
                })
                .filter(new Predicate<EthTransaction>() {
                    @Override
                    public boolean test(EthTransaction ethTransaction) throws Exception {
                        return ethTransaction.getTransaction() != null;
                    }
                })
                .map(new Function<EthTransaction, Transaction>() {
                    @Override
                    public Transaction apply(EthTransaction ethTransaction) throws Exception {
                        return ethTransaction.getTransaction();
                    }
                });
    }

    public Flowable<EthBlock> blockFlowable(
            final boolean fullTransactionObjects, final long pollingInterval) {
        return ethBlockHashFlowable(pollingInterval)
                .flatMap(new Function<String, Publisher<? extends EthBlock>>() {
                    @Override
                    public Publisher<? extends EthBlock> apply(String blockHash) throws Exception {
                        return web3j.ethGetBlockByHash(blockHash, fullTransactionObjects).flowable();
                    }
                });
    }

    public Flowable<EthBlock> replayBlocksFlowable(
            DefaultBlockParameter startBlock, DefaultBlockParameter endBlock,
            boolean fullTransactionObjects) {
        return replayBlocksFlowable(startBlock, endBlock, fullTransactionObjects, true);
    }

    public Flowable<EthBlock> replayBlocksFlowable(
            DefaultBlockParameter startBlock, DefaultBlockParameter endBlock,
            boolean fullTransactionObjects, boolean ascending) {
        // We use a scheduler to ensure this Flowable runs asynchronously for users to be
        // consistent with the other Flowables
        return replayBlocksFlowableSync(startBlock, endBlock, fullTransactionObjects, ascending)
                .subscribeOn(scheduler);
    }

    private Flowable<EthBlock> replayBlocksFlowableSync(
            DefaultBlockParameter startBlock, DefaultBlockParameter endBlock,
            boolean fullTransactionObjects) {
        return replayBlocksFlowableSync(startBlock, endBlock, fullTransactionObjects, true);
    }

    private Flowable<EthBlock> replayBlocksFlowableSync(
            final DefaultBlockParameter startBlock, final DefaultBlockParameter endBlock,
            final boolean fullTransactionObjects, final boolean ascending) {

        BigInteger startBlockNumber = null;
        BigInteger endBlockNumber = null;
        try {
            startBlockNumber = getBlockNumber(startBlock);
            endBlockNumber = getBlockNumber(endBlock);
        } catch (IOException e) {
            Flowable.error(e);
        }

        if (ascending) {
            return Flowables.range(startBlockNumber, endBlockNumber)
                    .flatMap(new Function<BigInteger, Publisher<? extends EthBlock>>() {
                        @Override
                        public Publisher<? extends EthBlock> apply(BigInteger i) throws Exception {
                            return web3j.ethGetBlockByNumber(
                                    new DefaultBlockParameterNumber(i),
                                    fullTransactionObjects).flowable();
                        }
                    });
        } else {
            return Flowables.range(startBlockNumber, endBlockNumber, false)
                    .flatMap(new Function<BigInteger, Publisher<? extends EthBlock>>() {
                        @Override
                        public Publisher<? extends EthBlock> apply(BigInteger i) throws Exception {
                            return web3j.ethGetBlockByNumber(
                                    new DefaultBlockParameterNumber(i),
                                    fullTransactionObjects).flowable();
                        }
                    });
        }
    }

    public Flowable<Transaction> replayTransactionsFlowable(
            DefaultBlockParameter startBlock, DefaultBlockParameter endBlock) {
        return replayBlocksFlowable(startBlock, endBlock, true)
                .flatMapIterable(new Function<EthBlock, Iterable<Transaction>>() {
                    @Override
                    public Iterable<Transaction> apply(EthBlock ethBlock) throws Exception {
                        return JsonRpc2_0Rx.toTransactions(ethBlock);
                    }
                });
    }

    public Flowable<EthBlock> replayPastBlocksFlowable(
            DefaultBlockParameter startBlock, boolean fullTransactionObjects,
            Flowable<EthBlock> onCompleteFlowable) {
        // We use a scheduler to ensure this Flowable runs asynchronously for users to be
        // consistent with the other Flowables
        return replayPastBlocksFlowableSync(
                startBlock, fullTransactionObjects, onCompleteFlowable)
                .subscribeOn(scheduler);
    }

    public Flowable<EthBlock> replayPastBlocksFlowable(
            DefaultBlockParameter startBlock, boolean fullTransactionObjects) {
        Flowable<EthBlock> fw = Flowable.empty();

        return replayPastBlocksFlowable(
                startBlock, fullTransactionObjects, Flowable.<EthBlock>empty());
    }

    private Flowable<EthBlock> replayPastBlocksFlowableSync(
            final DefaultBlockParameter startBlock, final boolean fullTransactionObjects,
            final Flowable<EthBlock> onCompleteFlowable) {

        BigInteger startBlockNumber;
        BigInteger latestBlockNumber;
        try {
            startBlockNumber = getBlockNumber(startBlock);
            latestBlockNumber = getLatestBlockNumber();
        } catch (IOException e) {
            return Flowable.error(e);
        }

        if (startBlockNumber.compareTo(latestBlockNumber) > -1) {
            return onCompleteFlowable;
        } else {
            final BigInteger latestBlockNumberToUse = latestBlockNumber;
            return Flowable.concat(
                    replayBlocksFlowableSync(
                            new DefaultBlockParameterNumber(startBlockNumber),
                            new DefaultBlockParameterNumber(latestBlockNumber),
                            fullTransactionObjects),

                    Flowable.defer(new Callable<Publisher<EthBlock>>() {
                        @Override
                        public Publisher<EthBlock> call() throws Exception {
                            return JsonRpc2_0Rx.this.replayPastBlocksFlowableSync(
                                    new DefaultBlockParameterNumber(latestBlockNumberToUse.add(BigInteger.ONE)),
                                    fullTransactionObjects,
                                    onCompleteFlowable);
                        }
                    }));
        }
    }

    public Flowable<Transaction> replayPastTransactionsFlowable(
            DefaultBlockParameter startBlock) {
        return replayPastBlocksFlowable(
                startBlock, true, Flowable.<EthBlock>empty())
                .flatMapIterable(new Function<EthBlock, Iterable<Transaction>>() {
                    @Override
                    public Iterable<Transaction> apply(EthBlock ethBlock) throws Exception {
                        return JsonRpc2_0Rx.toTransactions(ethBlock);
                    }
                });
    }

    public Flowable<EthBlock> replayPastAndFutureBlocksFlowable(
            DefaultBlockParameter startBlock, boolean fullTransactionObjects,
            long pollingInterval) {

        return replayPastBlocksFlowable(
                startBlock, fullTransactionObjects,
                blockFlowable(fullTransactionObjects, pollingInterval));
    }

    public Flowable<Transaction> replayPastAndFutureTransactionsFlowable(
            DefaultBlockParameter startBlock, long pollingInterval) {
        return replayPastAndFutureBlocksFlowable(
                startBlock, true, pollingInterval)
                .flatMapIterable(new Function<EthBlock, Iterable<Transaction>>() {
                    @Override
                    public Iterable<Transaction> apply(EthBlock ethBlock) throws Exception {
                        return JsonRpc2_0Rx.toTransactions(ethBlock);
                    }
                });
    }

    private BigInteger getLatestBlockNumber() throws IOException {
        return getBlockNumber(DefaultBlockParameterName.LATEST);
    }

    private BigInteger getBlockNumber(
            DefaultBlockParameter defaultBlockParameter) throws IOException {
        if (defaultBlockParameter instanceof DefaultBlockParameterNumber) {
            return ((DefaultBlockParameterNumber) defaultBlockParameter).getBlockNumber();
        } else {
            EthBlock latestEthBlock = web3j.ethGetBlockByNumber(
                    defaultBlockParameter, false).send();
            return latestEthBlock.getBlock().getNumber();
        }
    }

    private static List<Transaction> toTransactions(EthBlock ethBlock) {
        // If you ever see an exception thrown here, it's probably due to an incomplete chain in
        // Geth/Parity. You should resync to solve.
        List<EthBlock.TransactionResult> transactionResults = ethBlock.getBlock().getTransactions();
        List<Transaction> transactions = new ArrayList<Transaction>(transactionResults.size());

        for (EthBlock.TransactionResult transactionResult : transactionResults) {
            transactions.add((Transaction) transactionResult.get());
        }
        return transactions;
    }
}
