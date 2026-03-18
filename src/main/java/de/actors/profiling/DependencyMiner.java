package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import de.ddm.structures.ColumnRef;


public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        ActorRef<DataProvider.Message> dataProvider;
	}

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EndOfFileMsg implements Message {
        private static final long serialVersionUID = 6122888579871894204L;
        int readerId;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CheckINDResult implements Message {
        private static final long serialVersionUID = 1891141898390445548L;

        ColumnRef dependent;
        ColumnRef referenced;
        boolean isInd;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReadyForTask implements Message {
        private static final long serialVersionUID = 1010763204580594884L;
        ActorRef<DependencyWorker.Message> worker;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataProvideFlush implements Message {
        private static final long serialVersionUID = 3380635943516162366L;
        ActorRef<DataProvider.Message> dataProvider;
    }

    ////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.files = InputConfigurationSingleton.get().getInputFiles();
		this.columnHeaders = new String[this.files.length][];

		this.inputReaders = new ArrayList<>(files.length);
		for (int id = 0; id < this.files.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.files[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long beginTime;
    private boolean startedBatches = false;

    private int readersFinished = 0;

    private int receivedHeaders = 0;
    private boolean assignedOwners = false;

    private final File[] files;
	private final String[][] columnHeaders;

    private final List<ColumnRef> allCols = new ArrayList<>();
    private final List<ActorRef<DataProvider.Message>> dataProviders = new ArrayList<>();
    private final Map<ColumnRef, ActorRef<DataProvider.Message>> ownerColumn = new HashMap<>();

    private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;
    private long totalTasks = 0;
    private long finishedTasks = 0;

    private final Set<InclusionDependency> discoveredINDs = new HashSet<>();
    private final Map<Integer, List<ColumnRef>> colsByFile = new HashMap<>();

    private final Map<Integer, Integer> colCountByFile = new HashMap<>();
    private final List<Integer> fileIDs = new ArrayList<>();

    private boolean didDiscoveryStart = false;
    private long nextRequestID = 0;

    private boolean areRequestsFlushed = false;
    private final int providersConfigured = SystemConfigurationSingleton.get().getNumWorkers();
    private final java.util.Set<ActorRef<DataProvider.Message>> flushSet = new java.util.HashSet<>();

    private int dependentIndex = 0;
    private int referencedIndex = 0;
    private int numOfCols = 0;

    private static final int CHUNK_LEN = 1024;
    private final Map<ColumnRef, ArrayList<String>> columnBuffers = new HashMap<>();

    ////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
                .onMessage(CheckINDResult.class, this::handle)
                .onMessage(EndOfFileMsg.class, this::handle)
                .onMessage(ReadyForTask.class, this::handle)
                .onMessage(DataProvideFlush.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

    private Behavior<Message> handle(StartMessage message) {
        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
        this.beginTime = System.currentTimeMillis();
        return this;
    }

    private Behavior<Message> handle(HeaderMessage message) {
        int fileID = message.getId();
        String[] header = message.getHeader();
        this.columnHeaders[fileID] = header;

        for (int colInd = 0; colInd < header.length; colInd++) {
            String columnName = header[colInd];
            ColumnRef columnRef = new ColumnRef(fileID, colInd, columnName);
            this.allCols.add(columnRef);
            this.colsByFile.computeIfAbsent(fileID, k -> new ArrayList<>()).add(columnRef);
            colCountByFile.merge(fileID, 1, Integer::sum);
        }
        receivedHeaders++;
        if (!fileIDs.contains(fileID)) fileIDs.add(fileID);
        maybeOwnerAssign();
        return this;
    }

    private Behavior<Message> handle(RegistrationMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        ActorRef<DataProvider.Message> dataProvider = message.getDataProvider();

        if (!this.dependencyWorkers.contains(dependencyWorker)) {
            this.dependencyWorkers.add(dependencyWorker);
            this.getContext().watch(dependencyWorker);
        }

        if (dataProvider != null) {
            if (!assignedOwners) {
                if (!this.dataProviders.contains(dataProvider)) {
                    this.dataProviders.add(dataProvider);
                }
            } else {
                this.getContext().getLog().info("DataProviders joined after assignedOwners are ignored for stability of partition: {}", dataProvider);
            }
        }
        maybeOwnerAssign();
        maybeBeginFlushing();
        return this;
    }

    private Behavior<Message> handle(BatchMessage message) {
        List<String[]> row = message.getBatch();
        int fileID = message.getId();

        if (row == null || row.isEmpty()) {
            return this;
        }

        List<ColumnRef> cols = this.colsByFile.get(fileID);
        if (cols == null) {
            this.inputReaders.get(fileID)
                    .tell(new InputReader.ReadBatchMessage(getContext().getSelf(), 10000));
            return this;
        }

        if (!assignedOwners) {
            return this;
        }

        for (String[] r : row) {
            int l = Math.min(r.length, cols.size());
            for (int colInd = 0; colInd < l; colInd++) {
                ColumnRef colRef = cols.get(colInd);

                ActorRef<DataProvider.Message> own = this.ownerColumn.get(colRef);
                if (own == null) continue;

                ArrayList<String> buffer =
                        columnBuffers.computeIfAbsent(colRef, k -> new ArrayList<>(CHUNK_LEN));
                buffer.add(r[colInd]);

                if (buffer.size() >= CHUNK_LEN) {
                    own.tell(new DataProvider.ColumnChunkStore(colRef, buffer));
                    columnBuffers.put(colRef, new ArrayList<>(CHUNK_LEN));
                }
            }
        }
        this.inputReaders.get(fileID)
                .tell(new InputReader.ReadBatchMessage(getContext().getSelf(), 10000));
        return this;
    }

    private Behavior<Message> handle(EndOfFileMsg message) {
        this.readersFinished++;
        maybeBeginFlushing();
        return this;
    }

    private Behavior<Message> handle(CheckINDResult msg) {

        if (msg.isInd()) {
            ColumnRef a = msg.getDependent();
            ColumnRef b = msg.getReferenced();

            File depFile = this.files[a.getFileId()];
            File refFile = this.files[b.getFileId()];

            InclusionDependency IND = new InclusionDependency(
                    depFile,
                    new String[]{ a.getColumnName() },
                    refFile,
                    new String[]{ b.getColumnName() }
            );
            this.discoveredINDs.add(IND);
        }

        this.finishedTasks++;
        if (this.finishedTasks == this.totalTasks) {
            this.resultCollector.tell(
                    new ResultCollector.ResultMessage(new ArrayList<>(this.discoveredINDs))
            );
            this.end();
        }
        return this;
    }

    private Behavior<Message> handle(ReadyForTask msg) {
        if (!didDiscoveryStart) return this;
        nextTaskAssign(msg.getWorker());
        return this;
    }

    private Behavior<Message> handle(DataProvideFlush msg) {
        if (!flushSet.add(msg.getDataProvider())) {
            return this;
        }

        if (flushSet.size() == dataProviders.size()) {
            this.getContext().getLog().info("Starting discovery of INDs after the data providers are flushed...");
            IndDiscovery();
        }
        return this;
    }

    private void nextTaskAssign(ActorRef<DependencyWorker.Message> worker) {

        if (!didDiscoveryStart) return;

        while (dependentIndex < numOfCols) {

            while (referencedIndex < numOfCols) {
                int x = dependentIndex;
                int y = referencedIndex;
                referencedIndex++;

                if (x == y) continue;

                ColumnRef a = this.allCols.get(x);
                ColumnRef b = this.allCols.get(y);

                ActorRef<DataProvider.Message> dependentOwner = this.ownerColumn.get(a);
                ActorRef<DataProvider.Message> referenceOwner = this.ownerColumn.get(b);

                if (dependentOwner == null || referenceOwner == null) {
                    this.getContext().getLog().error(
                            "Although assignedOwners=true, owner missing. dependentOwner={}, referenceOwner={}, dependent={}, referenced={}",
                            dependentOwner, referenceOwner,a ,b
                    );
                    this.didDiscoveryStart = false;
                    return;
                }

                DependencyWorker.CheckIndTask indTask = new DependencyWorker.CheckIndTask(
                        this.getContext().getSelf(),
                        a,
                        b,
                        nextRequestID++,
                        dependentOwner,
                        referenceOwner
                );

                worker.tell(indTask);
                return;
            }
            dependentIndex++;
            referencedIndex = 0;
        }
    }

    private void IndDiscovery() {

        maybeOwnerAssign();
        if (!assignedOwners) {
            this.getContext().getLog().warn("Cannot begin IND discovery as owners are not yet assigned.");
            return;
        }
        this.finishedTasks = 0;
        this.nextRequestID = 0;
        this.discoveredINDs.clear();
        this.didDiscoveryStart = false;

        if (this.dependencyWorkers.isEmpty()) {
            this.getContext().getLog().warn("Dependency workers are not registered, always start with workers (-w 0)");
            this.resultCollector.tell(
                    new ResultCollector.ResultMessage(new ArrayList<>(this.discoveredINDs))
            );
            this.end();
            return;
        }

        this.numOfCols = this.allCols.size();
        this.dependentIndex = 0;
        this.referencedIndex = 0;

        this.totalTasks = (long) this.numOfCols * (this.numOfCols - 1);

        this.didDiscoveryStart = true;

        if (this.totalTasks == 0) {
            this.resultCollector.tell(
                    new ResultCollector.ResultMessage(new ArrayList<>(this.discoveredINDs))
            );
            this.end();
            return;
        }
        for (ActorRef<DependencyWorker.Message> worker : this.dependencyWorkers) {
            nextTaskAssign(worker);
        }
    }

    private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoverTime = System.currentTimeMillis() - this.beginTime;
		this.getContext().getLog().info("Completed mining within {} ms", discoverTime);
	}

    private void assignColOwner() {
        if (this.dataProviders.isEmpty()) {
            this.getContext().getLog().warn("DataProviders are not yet registered, so column owners cannot be assigned.");
            return;
        }

        for (ColumnRef col : this.allCols) {
            int indx = Math.floorMod(col.hashCode(), this.dataProviders.size());
            this.ownerColumn.put(col, this.dataProviders.get(indx));
        }
    }

    private void maybeOwnerAssign() {
        if (assignedOwners) return;
        if (receivedHeaders == this.files.length && !this.dataProviders.isEmpty()) {

            assignColOwner();
            assignedOwners = true;

            if (!startedBatches) {
                startedBatches = true;
                for (ActorRef<InputReader.Message> reader : this.inputReaders) {
                    reader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
                }
            }
            return;
        }

        if (receivedHeaders == this.files.length && this.dataProviders.isEmpty()) {
            this.getContext().getLog().warn(
                    "DataProvider are not registered yet (Headers are available), waiting for data provider to load."
            );
        }
    }

    private void maybeBeginFlushing() {
        if (areRequestsFlushed) return;

        if (this.readersFinished == this.files.length && this.dataProviders.isEmpty()) {
            areRequestsFlushed = true;
            this.getContext().getLog().warn(
                    "Finishing IND discovery with no data provider available, EOF reached"
            );
            this.resultCollector.tell(
                    new ResultCollector.ResultMessage(new ArrayList<>(this.discoveredINDs))
            );
            this.end();
            return;
        }

        if (this.readersFinished == this.files.length && !this.dataProviders.isEmpty()) {

            areRequestsFlushed = true;
            flushSet.clear();

            this.getContext().getLog().info(
                    "Flushing {} providers, all readers have ended",
                    this.dataProviders.size()
            );

            handleRemainingMiner();

            for (ActorRef<DataProvider.Message> dataProvider : this.dataProviders) {
                dataProvider.tell(new DataProvider.Clear(this.getContext().getSelf()));
            }
        }
    }

    private void handleRemainingMiner() {
        if (!assignedOwners) return;

        for (Map.Entry<ColumnRef, ArrayList<String>> m : columnBuffers.entrySet()) {
            ColumnRef c = m.getKey();
            ArrayList<String> buffer = m.getValue();
            if (buffer == null || buffer.isEmpty()) continue;

            ActorRef<DataProvider.Message> own = this.ownerColumn.get(c);
            if (own != null) own.tell(new DataProvider.ColumnChunkStore(c, buffer));
        }
        columnBuffers.clear();
    }

    private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}
}