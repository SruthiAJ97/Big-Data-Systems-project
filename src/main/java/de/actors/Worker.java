package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.patterns.Reaper;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.actors.profiling.DataProvider;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

public class Worker extends AbstractBehavior<Worker.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable {}

    @NoArgsConstructor
    public static class ShutdownMessage implements Message {
        private static final long serialVersionUID = 7516129288777469221L;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";

    public static Behavior<Message> create() {
        return Behaviors.setup(Worker::new);
    }

    ///////////////////
    // Actor State   //
    ///////////////////

    private final List<ActorRef<DependencyWorker.Message>> workers;

    private Worker(ActorContext<Message> context) {
        super(context);
        Reaper.watchWithDefaultReaper(this.getContext().getSelf());

        final int numOfWorkers = SystemConfigurationSingleton.get().getNumWorkers();

        this.workers = new ArrayList<>(Math.max(numOfWorkers, 0));

        if (numOfWorkers <= 0) {
            context.getLog().warn("Workers not configured, so system will not process data");
            return;
        }
        ActorRef<DataProvider.Message> dataProvider =
                context.spawn(DataProvider.create(), DataProvider.DEFAULT_NAME);

        for (int i = 0; i < numOfWorkers; i++) {
            this.workers.add(context.spawn(
                    DependencyWorker.create(dataProvider),
                    DependencyWorker.DEFAULT_NAME + "_" + i,
                    DispatcherSelector.fromConfig("akka.worker-pool-dispatcher")));
        }
    }

    //////////////////////
    // Actor Behavior   //
    //////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ShutdownMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ShutdownMessage message) {
        return Behaviors.stopped();
    }
}
