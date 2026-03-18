package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.ColumnRef;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import de.ddm.actors.patterns.LargeMessageProxy;

import java.util.*;

public class DataProvider extends AbstractBehavior<DataProvider.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable {}

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ColumnChunkStore implements Message { // storing column values
        private static final long serialVersionUID = -1061242546032121089L;
        ColumnRef column;
        List<String> values; // chunk values
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Clear implements Message {
        private static final long serialVersionUID = -6562645264235927064L;
        ActorRef<DependencyMiner.Message> replyTo;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GetBloomColumn implements Message {
        private static final long serialVersionUID = -935489330368294602L;
        long requestId;
        ColumnRef column;
        ActorRef<DependencyMiner.Message> replyToMiner;
        ActorRef<LargeMessageProxy.Message> replyToProxy; // worker’s proxy
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BloomColumn implements Message, LargeMessageProxy.LargeMessage {
        private static final long serialVersionUID = 4425279381603506429L;
        long requestId;
        ColumnRef column;
        ActorRef<DependencyMiner.Message> replyToMiner;
        long[] bloomBits;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RemoveColumn implements Message { // removing column values
        private static final long serialVersionUID = 2102096217232283321L;
        ColumnRef column;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class IndCheckExact implements Message {
        long requestId;
        ColumnRef dependent;
        ColumnRef referenced;
        ActorRef<DependencyMiner.Message> replyToMiner;
        ActorRef<DataProvider.Message> dependentOwner;
        ActorRef<LargeMessageProxy.Message> replyToProxy; // worker proxy
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GetColumnValuesInChunks implements Message {
        long requestId;
        ColumnRef column;
        ActorRef<DependencyMiner.Message> replyToMiner;
        int offset;
        int limit;
        ActorRef<DataProvider.Message> replyToProvider;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ColumnValuesInChunks implements Message {
        long requestId;
        ColumnRef column;
        ActorRef<DependencyMiner.Message> replyToMiner;
        List<String> values;
        boolean last;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class IndResultExact implements Message, LargeMessageProxy.LargeMessage {
        long requestId;
        ColumnRef dependent;
        ColumnRef referenced;
        ActorRef<DependencyMiner.Message> replyToMiner;
        boolean isInd;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dataProvider";

    public static Behavior<Message> create() {
        return Behaviors.setup(DataProvider::new);
    }

    private DataProvider(ActorContext<Message> context) {
        super(context);
        this.largeMessageProxy =
                context.spawn(LargeMessageProxy.create(context.getSelf().unsafeUpcast(), false),
                        LargeMessageProxy.DEFAULT_NAME);
    }

    /////////////////
    // Actor State //
    /////////////////

    private static final int BLOOM_LEN = 1 << 20;
    private final Map<ColumnRef, BitSet> bloom = new HashMap<>();
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
    private final Map<PendingID, ExactPending> exactPending = new HashMap<>();
    private static final int CHUNK_VAL = 8192;
    private final Map<ColumnRef, ArrayList<String>> dataValue = new HashMap<>();
    private final Map<ColumnRef, HashSet<String>> seenValue = new HashMap<>();

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ColumnChunkStore.class, this::handle)
                .onMessage(GetBloomColumn.class, this::handle)
                .onMessage(Clear.class, this::handle)
                .onMessage(RemoveColumn.class, this::handle)
                .onMessage(IndCheckExact.class, this::handle)
                .onMessage(GetColumnValuesInChunks.class, this::handle)
                .onMessage(ColumnValuesInChunks.class, this::handle)   // FIXED
                .build();
    }


    private Behavior<Message> handle(ColumnChunkStore msg) {
        BitSet bit = this.bloom.computeIfAbsent(msg.getColumn(), c -> new BitSet(BLOOM_LEN));
        ArrayList<String> list = dataValue.computeIfAbsent(msg.getColumn(), c -> new ArrayList<>());
        HashSet<String> s = seenValue.computeIfAbsent(msg.getColumn(), c -> new HashSet<>());

        for (String val : msg.getValues()) {
            if (val == null) continue;
            addBloom(bit, val);
            if (s.add(val)) {
                list.add(val);
            }
        }
        return this;
    }

    private Behavior<Message> handle(GetBloomColumn msg) {
        BitSet bit = this.bloom.getOrDefault(msg.getColumn(), new BitSet(BLOOM_LEN));

        BloomColumn bc = new BloomColumn(
                msg.getRequestId(),
                msg.getColumn(),
                msg.getReplyToMiner(),
                bit.toLongArray()
        );

        DependencyWorker.WrapColBloom wrap =
                new DependencyWorker.WrapColBloom(bc);

        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(wrap, msg.getReplyToProxy()));
        return this;
    }

    private Behavior<Message> handle(GetColumnValuesInChunks msg) {
        ArrayList<String> allData = dataValue.getOrDefault(msg.getColumn(), new ArrayList<>());

        int f = Math.min(msg.getOffset(), allData.size());
        int t = Math.min(f + msg.getLimit(), allData.size());
        boolean l = (t >= allData.size());

        msg.getReplyToProvider().tell(
                new ColumnValuesInChunks(
                        msg.getRequestId(),
                        msg.getColumn(),
                        msg.getReplyToMiner(),
                        new ArrayList<>(allData.subList(f, t)),
                        l
                )
        );
        return this;
    }

    private Behavior<Message> handle(ColumnValuesInChunks msg) {
        PendingID k = new PendingID(msg.getReplyToMiner(), msg.getRequestId());
        ExactPending pendEx = exactPending.get(k);
        if (pendEx == null) return this;

        for (String val : msg.getValues()) {
            if (!pendEx.referenceSet.contains(val)) {
                pendEx.isFailed = true;
                break;
            }
        }

        if (pendEx.isFailed || msg.isLast()) {
            boolean isIND = !pendEx.isFailed;
            exactPending.remove(k);
            sendResult(msg.getRequestId(), pendEx.dependent, pendEx.reference, pendEx.replyMiner, isIND, pendEx.replyProxy);
            return this;
        }
        pendEx.off += msg.getValues().size();
        pendEx.ownerDependent.tell(
                new GetColumnValuesInChunks(
                        msg.getRequestId(),
                        pendEx.dependent,
                        msg.getReplyToMiner(),
                        pendEx.off,
                        CHUNK_VAL,
                        getContext().getSelf()
                )
        );
        return this;
    }


    private static class ExactPending {
        final ColumnRef dependent;
        final ColumnRef reference;

        final ActorRef<DependencyMiner.Message> replyMiner;
        final ActorRef<DataProvider.Message> ownerDependent;
        final ActorRef<LargeMessageProxy.Message> replyProxy;

        final Set<String> referenceSet;
        int off = 0;
        boolean isFailed = false;

        ExactPending(ColumnRef dependent,
                     ColumnRef reference,
                     ActorRef<DependencyMiner.Message> replyMiner,
                     ActorRef<DataProvider.Message> ownerDependent,
                     ActorRef<LargeMessageProxy.Message> replyProxy,
                     Set<String> referenceSet) {
            this.dependent = dependent;
            this.reference = reference;
            this.replyMiner = replyMiner;
            this.ownerDependent = ownerDependent;
            this.replyProxy = replyProxy;
            this.referenceSet = referenceSet;
        }
    }

    private static final class PendingID {
        final ActorRef<DependencyMiner.Message> replyMiner;
        final long requestID;

        PendingID(ActorRef<DependencyMiner.Message> replyMiner, long requestID) {
            this.replyMiner = replyMiner;
            this.requestID = requestID;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof PendingID)) return false;
            PendingID t = (PendingID) obj;
            return requestID == t.requestID && replyMiner.equals(t.replyMiner);
        }

        @Override
        public int hashCode() {
            int res = replyMiner.hashCode();
            res = 31 * res + Long.hashCode(requestID);
            return res;
        }
    }


    private Behavior<Message> handle(IndCheckExact msg) {
        ArrayList<String> referenceList = dataValue.get(msg.getReferenced());
        if (referenceList == null) {
            referenceList = new ArrayList<>(); // treat as empty
        }

        Set<String> referenceSet = new HashSet<>(referenceList);

        PendingID k = new PendingID(msg.getReplyToMiner(), msg.getRequestId());

        exactPending.put(
                k,
                new ExactPending(
                        msg.getDependent(),
                        msg.getReferenced(),
                        msg.getReplyToMiner(),
                        msg.getDependentOwner(),
                        msg.getReplyToProxy(),
                        referenceSet
                )
        );

        // request first dependent chunk
        msg.getDependentOwner().tell(
                new GetColumnValuesInChunks(
                        msg.getRequestId(),
                        msg.getDependent(),
                        msg.getReplyToMiner(),
                        0,
                        CHUNK_VAL,
                        getContext().getSelf()
                )
        );

        return this;
    }

    private void sendResult(long requestID, ColumnRef d, ColumnRef r,
                                 ActorRef<DependencyMiner.Message> replyMiner,
                                 boolean isIND,
                                 ActorRef<LargeMessageProxy.Message> replyProxy) {
        IndResultExact res = new IndResultExact(requestID, d, r, replyMiner, isIND);
        DependencyWorker.WrapExactINDResult wrapped = new DependencyWorker.WrapExactINDResult(res);
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(wrapped, replyProxy));
    }



    private Behavior<Message> handle(RemoveColumn msg) {
        this.bloom.remove(msg.getColumn());
        this.dataValue.remove(msg.getColumn());
        this.seenValue.remove(msg.getColumn());
        return this;
    }

    private Behavior<Message> handle(Clear msg) {
         msg.getReplyTo().tell(new DependencyMiner.DataProvideFlush(this.getContext().getSelf()));
        return this;
    }

    private void addBloom(BitSet bit, String v) {
        int hash1 = v.hashCode();
        int hash2 = hashMix(hash1);
        int hash3 = hashMix(hash2);

        setBloom(bit, hash1);
        setBloom(bit, hash2);
        setBloom(bit, hash3);
    }

    private void setBloom(BitSet bit, int hash) {
        int i = (hash & 0x7fffffff) % BLOOM_LEN;
        bit.set(i);
    }

    private int hashMix(int y) {
        y ^= (y >>> 16);
        y *= 0x7feb352d;
        y ^= (y >>> 15);
        y *= 0x846ca68b;
        y ^= (y >>> 16);
        return y;
    }

    public static boolean checkBitsetSubset(BitSet d, BitSet r) {
        BitSet tmp = (BitSet) d.clone();
        tmp.andNot(r);
        return tmp.isEmpty();
    }


}
