package com.ana3.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.ana3.util.WordCounter;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class Worker extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private Cancellable workReceiveTimeOutCancelHandle;

    public static class Hello implements Serializable {
        private ActorRef masterActorRef;

        public Hello(ActorRef masterActorRef) {
            this.masterActorRef = masterActorRef;
        }

        ActorRef getMasterActorRef() {
            return masterActorRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Hello hello = (Hello) o;
            return Objects.equals(masterActorRef, hello.masterActorRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(masterActorRef);
        }

        @Override
        public String toString() {
            return "Hello{" +
                    "masterActorRef=" + masterActorRef +
                    '}';
        }
    }

    private static class WorkReceiveTimeout implements Serializable {
        private ActorRef masterActorRef;

        public WorkReceiveTimeout(ActorRef masterActorRef) {
            this.masterActorRef = masterActorRef;
        }

        ActorRef getMasterActorRef() {
            return masterActorRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WorkReceiveTimeout that = (WorkReceiveTimeout) o;
            return Objects.equals(masterActorRef, that.masterActorRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(masterActorRef);
        }

        @Override
        public String toString() {
            return "WorkReceiveTimeout{" +
                    "masterActorRef=" + masterActorRef +
                    '}';
        }
    }

    public static class Work implements Serializable {
        private List<String> workItems;
        private ActorRef masterActorRef;

        public Work(List<String> workItems, ActorRef masterActorRef) {
            this.workItems = workItems;
            this.masterActorRef = masterActorRef;
        }

        List<String> getWorkItems() {
            return workItems;
        }

        ActorRef getMasterActorRef() {
            return masterActorRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Work work = (Work) o;
            return Objects.equals(workItems, work.workItems) &&
                    Objects.equals(masterActorRef, work.masterActorRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workItems, masterActorRef);
        }

        @Override
        public String toString() {
            return "Work{" +
                    "workItems=" + workItems +
                    ", masterActorRef=" + masterActorRef +
                    '}';
        }
    }

    public static Props props() {

        return Props.create(Worker.class, Worker::new);
    }

    public Worker() {
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Hello.class, this::processHello)
                .match(WorkReceiveTimeout.class, this::processWorkReceiveTimeout)
                .match(Work.class, this::processWork)
                .matchAny(o -> {
                    log.error("########## --- ########## Worker.receive:: unknown packet {}", o);
                })

                .build();
    }

    private void processWork(Work w) {
        if (workReceiveTimeOutCancelHandle != null) workReceiveTimeOutCancelHandle.cancel();
        Map<String, Long> wordsAndCounts = WordCounter.count(w.workItems);
        Master.WorkDone workDone = new Master.WorkDone(wordsAndCounts, getSelf());
        w.getMasterActorRef().tell(workDone, getSelf());
        requestWork(w.getMasterActorRef());
        UUID uuid = UUID.randomUUID();
        log.debug(uuid+"---Worker.processWork:: words received are {}", w.getWorkItems());
        log.debug(uuid+"---Worker.processWork:: w.getMasterActorRef() = {}, wordsandcount  = {}", w.getMasterActorRef(), wordsAndCounts);
    }

    private void processWorkReceiveTimeout(WorkReceiveTimeout t) {
        ActorRef masterActorRef = t.getMasterActorRef();
        requestWork(masterActorRef);
        log.debug("---Worker.processWorkReceiveTimeout:: {}", t);
    }

    private void processHello(Hello h) {
        ActorRef masterActorRef = h.getMasterActorRef();
        requestWork(masterActorRef);
        log.debug("---Worker.processHello:: {}", h);
    }

    private void requestWork(ActorRef masterActorRef) {
        Master.ReadyForWork readyForWork = new Master.ReadyForWork(getSelf());
        masterActorRef.tell(readyForWork, getSelf());
        workReceiveTimeOutCancelHandle = getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10L),
                getSelf(), new WorkReceiveTimeout(masterActorRef), getContext().getSystem().getDispatcher(), getSelf());
    }

}
