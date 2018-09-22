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

        public ActorRef getMasterActorRef() {
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

    public static class ReadyForWork implements Serializable {
        private ActorRef workerActorRef;

        public ReadyForWork(ActorRef workerActorRef) {
            this.workerActorRef = workerActorRef;
        }

        public ActorRef getWorkerActorRef() {
            return workerActorRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReadyForWork that = (ReadyForWork) o;
            return Objects.equals(workerActorRef, that.workerActorRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workerActorRef);
        }

        @Override
        public String toString() {
            return "ReadyForWork{" +
                    "workerActorRef=" + workerActorRef +
                    '}';
        }
    }

    private static class WorkReceiveTimeout implements Serializable {
        private ActorRef masterActorRef;

        public WorkReceiveTimeout(ActorRef masterActorRef) {
            this.masterActorRef = masterActorRef;
        }

        public ActorRef getMasterActorRef() {
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

        public List<String> getWorkItems() {
            return workItems;
        }

        public ActorRef getMasterActorRef() {
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

    public static class WorkDone implements Serializable {
        private Map<String, Long> workItems;
        private ActorRef workerActorRef;

        public WorkDone(Map<String, Long> workItems, ActorRef workerActorRef) {
            this.workItems = workItems;
            this.workerActorRef = workerActorRef;
        }

        public Map<String, Long> getWorkItems() {
            return workItems;
        }

        public ActorRef getWorkerActorRef() {
            return workerActorRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            WorkDone workDone = (WorkDone) o;

            if (getWorkItems() != null ? !getWorkItems().equals(workDone.getWorkItems()) : workDone.getWorkItems() != null)
                return false;
            return getWorkerActorRef() != null ? getWorkerActorRef().equals(workDone.getWorkerActorRef()) : workDone.getWorkerActorRef() == null;
        }

        @Override
        public int hashCode() {
            int result = getWorkItems() != null ? getWorkItems().hashCode() : 0;
            result = 31 * result + (getWorkerActorRef() != null ? getWorkerActorRef().hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "WorkDone{" +
                    "workItems=" + workItems +
                    ", workerActorRef=" + workerActorRef +
                    '}';
        }
    }

    public static Props props() {

        return Props.create(Worker.class, () -> new Worker());
    }

    public Worker() {
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Hello.class, h -> {
                    processHello(h);
                })
                .match(WorkReceiveTimeout.class, t -> {
                    processWorkReceiveTimeout(t);
                })
                .match(Work.class, w -> {
                    processWork(w);
                })
                .matchAny(o -> {
                    log.error("########## --- ########## Worker.receive:: unknown packet " + o);
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
        log.info(uuid+" words received are "+w.getWorkItems());
        log.info(uuid+"---Worker.processWork:: w.getMasterActorRef() || w || wordsandcount   "+w.getMasterActorRef()+" || "+ wordsAndCounts);
    }

    private void processWorkReceiveTimeout(WorkReceiveTimeout t) {
        ActorRef masterActorRef = t.getMasterActorRef();
        requestWork(masterActorRef);
        log.info("---Worker.processWorkReceiveTimeout:: " + t);
    }

    private void processHello(Hello h) {
        ActorRef masterActorRef = h.getMasterActorRef();
        requestWork(masterActorRef);
        log.info("---Worker.processHello:: h" + h);
    }

    private void requestWork(ActorRef masterActorRef) {
        Master.ReadyForWork readyForWork = new Master.ReadyForWork(getSelf());
        masterActorRef.tell(readyForWork, getSelf());
        workReceiveTimeOutCancelHandle = getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10L),
                getSelf(), new WorkReceiveTimeout(masterActorRef), getContext().getSystem().getDispatcher(), getSelf());
    }

}
