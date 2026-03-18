package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.ColumnRef;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.BitSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;


public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CheckIndTask implements DependencyWorker.Message {
        private static final long serialVersionUID = -4873254259119648972L;
        ActorRef<DependencyMiner.Message> replyTo;
        ColumnRef dependent;
        ColumnRef referenced;
        long requestId;
        ActorRef<DataProvider.Message> dependentOwner;
        ActorRef<DataProvider.Message> referencedOwner;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WrapColBloom implements Message, LargeMessageProxy.LargeMessage {
        private static final long serialVersionUID = -4713470923980375838L;
        DataProvider.BloomColumn bloom;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class WrapExactINDResult implements Message, LargeMessageProxy.LargeMessage {
        DataProvider.IndResultExact result;
    }


    ////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

    private final ActorRef<DataProvider.Message> dataProvider;

    public static Behavior<Message> create(ActorRef<DataProvider.Message> dataProvider) {
        return Behaviors.setup(ctx -> new DependencyWorker(ctx, dataProvider));
    }


    private DependencyWorker(ActorContext<Message> context, ActorRef<DataProvider.Message> dataProvider) {
        super(context);
        this.dataProvider = dataProvider;

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
    private final Map<PendingID, IndCheckPending> pending = new HashMap<>();
    private final java.util.Set<ActorRef<DependencyMiner.Message>> minersKnown = new java.util.HashSet<>();

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(CheckIndTask.class, this::handle)
                .onMessage(WrapColBloom.class, this::handle)
                .onMessage(WrapExactINDResult.class, this::handle)
				.build();
	}

    private Behavior<Message> handle(ReceptionistListingMessage message) {
        Set<ActorRef<DependencyMiner.Message>> dependencyMiners =
                message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);

        for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners) {
            if (minersKnown.add(dependencyMiner)) {
                dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(), this.dataProvider));
                dependencyMiner.tell(new DependencyMiner.ReadyForTask(this.getContext().getSelf()));
            }
        }
        return this;
    }

    private Behavior<Message> handle(WrapExactINDResult wrap) {
        DataProvider.IndResultExact dr = wrap.getResult();

        PendingID k = new PendingID(dr.getReplyToMiner(), dr.getRequestId());
        IndCheckPending pendCheck = pending.remove(k);
        if (pendCheck == null) return this;

        pendCheck.replyTo.tell(new DependencyMiner.CheckINDResult(pendCheck.dep, pendCheck.ref, dr.isInd()));
        pendCheck.replyTo.tell(new DependencyMiner.ReadyForTask(getContext().getSelf()));
        return this;
    }

    private Behavior<Message> handle(CheckIndTask msg) {
        IndCheckPending pendCheck = new IndCheckPending(msg.getReplyTo(), msg.getDependent(), msg.getReferenced());
        pendCheck.depOwner = msg.getDependentOwner();
        pendCheck.refOwner = msg.getReferencedOwner();
        PendingID k = new PendingID(msg.getReplyTo(), msg.getRequestId());
        pendCheck.k = k;
        this.pending.put(k, pendCheck);

        msg.getDependentOwner().tell(new DataProvider.GetBloomColumn(
                msg.getRequestId(),
                msg.getDependent(),
                msg.getReplyTo(),
                this.largeMessageProxy
        ));

        msg.getReferencedOwner().tell(new DataProvider.GetBloomColumn(
                msg.getRequestId(),
                msg.getReferenced(),
                msg.getReplyTo(),
                this.largeMessageProxy
        ));

        return this;
    }

    private Behavior<Message> handle(WrapColBloom wrap) {
        DataProvider.BloomColumn msg = wrap.getBloom();

        PendingID k = new PendingID(msg.getReplyToMiner(), msg.getRequestId());
        IndCheckPending pendCheck = pending.get(k);
        if (pendCheck == null) return this;

        BitSet bit = BitSet.valueOf(msg.getBloomBits());

        if (pendCheck.dep.equals(msg.getColumn())) {
            pendCheck.depBloom = bit;
        } else if (pendCheck.ref.equals(msg.getColumn())) {
            pendCheck.refBloom = bit;
        } else {
            return this;
        }

        if (!pendCheck.isComplete()) return this;

        boolean isPossible = DataProvider.checkBitsetSubset(pendCheck.depBloom, pendCheck.refBloom);
        if (!isPossible) {
            pendCheck.replyTo.tell(new DependencyMiner.CheckINDResult(pendCheck.dep, pendCheck.ref, false));
            pendCheck.replyTo.tell(new DependencyMiner.ReadyForTask(getContext().getSelf()));
            pending.remove(k);
            return this;
        }

        if (!pendCheck.requestedExact) {
            pendCheck.requestedExact = true;

            pendCheck.refOwner.tell(new DataProvider.IndCheckExact(
                    msg.getRequestId(),
                    pendCheck.dep,
                    pendCheck.ref,
                    pendCheck.replyTo,
                    pendCheck.depOwner,
                    this.largeMessageProxy
            ));
        }
        return this;
    }

    private static class IndCheckPending {
        ActorRef<DataProvider.Message> depOwner;
        ActorRef<DataProvider.Message> refOwner;
        boolean requestedExact = false;
        PendingID k;

        final ActorRef<DependencyMiner.Message> replyTo;
        final ColumnRef dep;
        final ColumnRef ref;

        BitSet depBloom;
        BitSet refBloom;

        IndCheckPending(ActorRef<DependencyMiner.Message> replyTo, ColumnRef dep, ColumnRef ref) {
            this.replyTo = replyTo;
            this.dep = dep;
            this.ref = ref;
        }

        boolean isComplete() {
            return depBloom != null && refBloom != null;
        }
    }

    private static final class PendingID {
        final ActorRef<DependencyMiner.Message> replyMiner;
        final long requestID;

        PendingID(ActorRef<DependencyMiner.Message> replyToMiner, long requestId) {
            this.replyMiner = replyToMiner;
            this.requestID = requestId;
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
}
