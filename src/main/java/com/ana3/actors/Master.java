package com.ana3.actors;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.ana3.util.MapTools;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Master extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private ActorRef fileReaderActorRef;
    private ActorRef routerActorRef;
    private Cancellable workBatchTimeOutCancelHandle;
    private Cancellable helloTimeOutCancelHandle;
    private List<String> workItemList = new ArrayList<>();
    private Map<String, Long> wordCount = new HashMap<>();
    private Map<ActorRef, Cancellable> actorWorkTimeOutCancelHandle = new HashMap<>();
    private Map<ActorRef, List<String>> actorsProcessingWorkItems = new HashMap<>();
    private int lineCountForWorkItem = 10;

    /**
     * Sent to master to request the current state. Used for testing
     */
    public static class RequestForCurrentState {
        private ActorRef requestActor;

        public RequestForCurrentState(ActorRef requestActor) {
            this.requestActor = requestActor;
        }

        public ActorRef getRequestActor() {
            return requestActor;
        }
    }

    /**
     * Used to send state to the requester. Used for testing.
     */
    public static class ResponseOfCurrentStat {
        private ActorRef fileReaderActorRef;
        private ActorRef routerActorRef;
        private Cancellable workBatchTimeOutCancelHandle;
        private Cancellable helloTimeOutCancelHandle;
        private List<String> workItemList = new ArrayList<>();
        private Map<String, Long> wordCount = new HashMap<>();
        private Map<ActorRef, Cancellable> actorWorkTimeOutCancelHandle = new HashMap<>();
        private Map<ActorRef, List<String>> actorsProcessingWorkItems = new HashMap<>();
        private int lineCountForWorkItem = 10;
    }

    /**
     * Message sent to self to inform that work batch from file reader hasn't been received
     */
    public static class WorkBatchTimeOut {
        private ActorRef fileReaderActorRef;

        public WorkBatchTimeOut(ActorRef fileReaderActorRef) {
            this.fileReaderActorRef = fileReaderActorRef;
        }
    }

    /**
     * Contains work from the file reader
     */
    public static class WorkBatch {
        private List<String> workItems;
        private ActorRef fileReaderActorRef;

        public WorkBatch(List<String> workItems, ActorRef fileReaderActorRef) {
            this.workItems = workItems;
            this.fileReaderActorRef = fileReaderActorRef;
        }

        public List<String> getWorkItems() {
            return workItems;
        }

        public ActorRef getFileReaderActorRef() {
            return fileReaderActorRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            WorkBatch workBatch = (WorkBatch) o;

            if (getWorkItems() != null ? !getWorkItems().equals(workBatch.getWorkItems()) : workBatch.getWorkItems() != null)
                return false;
            return getFileReaderActorRef() != null ? getFileReaderActorRef().equals(workBatch.getFileReaderActorRef()) : workBatch.getFileReaderActorRef() == null;
        }

        @Override
        public int hashCode() {
            int result = getWorkItems() != null ? getWorkItems().hashCode() : 0;
            result = 31 * result + (getFileReaderActorRef() != null ? getFileReaderActorRef().hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "WorkBatch{" +
                    "workItems=" + workItems +
                    ", fileReaderActorRef=" + fileReaderActorRef +
                    '}';
        }
    }

    /**
     * Sent by self to inform that wokers haven't responded with a ReadyForWork message
     */
    public static class HelloTimeOut {
        public HelloTimeOut() {
        }
    }

    /**
     * Sent by worker to notify that it's ready for work
     */
    public static class ReadyForWork {
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

            return getWorkerActorRef() != null ? getWorkerActorRef().equals(that.getWorkerActorRef()) : that.getWorkerActorRef() == null;
        }

        @Override
        public int hashCode() {
            return getWorkerActorRef() != null ? getWorkerActorRef().hashCode() : 0;
        }

        @Override
        public String toString() {
            return "ReadyForWork{" +
                    "workerActorRef=" + workerActorRef +
                    '}';
        }
    }

    /**
     * Sent by self to inform a worker has not sent work results
     */
    public static class WorkTimeout {
        private ActorRef workActorRef;

        public WorkTimeout(ActorRef workActorRef) {
            this.workActorRef = workActorRef;
        }

        public ActorRef getWorkActorRef() {
            return workActorRef;
        }
    }

    /**
     * Send by worker containing work done
     */
    public static class WorkDone {
        private Map<String, Long> results;
        private ActorRef workerActorRef;

        public WorkDone(Map<String, Long> results, ActorRef workerActorRef) {
            this.results = results;
            this.workerActorRef = workerActorRef;
        }

        public Map<String, Long> getResults() {
            return results;
        }

        public ActorRef getWorkerActorRef() {
            return workerActorRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            WorkDone workDone = (WorkDone) o;

            if (getResults() != null ? !getResults().equals(workDone.getResults()) : workDone.getResults() != null)
                return false;
            return getWorkerActorRef() != null ? getWorkerActorRef().equals(workDone.getWorkerActorRef()) : workDone.getWorkerActorRef() == null;
        }

        @Override
        public int hashCode() {
            int result = getResults() != null ? getResults().hashCode() : 0;
            result = 31 * result + (getWorkerActorRef() != null ? getWorkerActorRef().hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "WorkDone{" +
                    "results=" + results +
                    ", workerActorRef=" + workerActorRef +
                    '}';
        }
    }

    public static Props props(int lineCountForWork, Function<ActorContext, ActorRef> fileReaderActorFactory, Function<ActorContext, ActorRef> routerActorFactory) {
        return Props.create(Master.class, () -> new Master(lineCountForWork, fileReaderActorFactory, routerActorFactory));
    }

    public Master(int lineCountForWorkItem, Function<ActorContext, ActorRef> fileReaderActorFactory, Function<ActorContext, ActorRef> routerActorFactory) {
        this.fileReaderActorRef = fileReaderActorFactory.apply(getContext());
        this.routerActorRef = routerActorFactory.apply(getContext());
        this.lineCountForWorkItem = lineCountForWorkItem;
    }

    @Override
    public void preStart() {
        fileReaderActorRef.tell(new FileReader.ReadyForBatch(getSelf()), getSelf());

        workBatchTimeOutCancelHandle = getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10L),
                getSelf(), new Master.WorkBatchTimeOut(fileReaderActorRef), getContext().getSystem().getDispatcher(), null);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(WorkBatchTimeOut.class, this::processMessageWorkBatchTimeout)
                .match(WorkBatch.class, this::processMessageWorkBatch)
                .match(HelloTimeOut.class, this::processMessageHelloTimeOut)
                .match(ReadyForWork.class, this::processMessageReadyForWork)
                .match(Terminated.class, this::processMessageWorkerTerminated)
                .match(WorkTimeout.class, this::processMessageWorkTimeout)
                .match(WorkDone.class, this::processMessageWorkDone)
                .build();
    }

    private void processMessageWorkBatchTimeout(WorkBatchTimeOut to) {
        workBatchTimeOutCancelHandle = getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10L),
                getSelf(), new Master.WorkBatchTimeOut(fileReaderActorRef), getContext().getSystem().getDispatcher(), null);

        fileReaderActorRef.tell(new FileReader.ReadyForBatch(getSelf()), getSelf());
        log.debug("exiting processMessageWorkBatchTimeout "+toString());
    }

    private void processMessageWorkBatch(WorkBatch wb) {
        this.workItemList.addAll(wb.workItems);

        routerActorRef.tell(new Worker.Hello(getSelf()), getSelf());

        helloTimeOutCancelHandle = getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10L),
                getSelf(), new Master.HelloTimeOut(), getContext().getSystem().getDispatcher(), null);

        log.debug("exiting processMessageWorkBatch "+toString());
    }

    private void processMessageHelloTimeOut(HelloTimeOut helloTimeOut) {
        routerActorRef.tell(new Worker.Hello(getSelf()), getSelf());

        helloTimeOutCancelHandle = getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10L),
                getSelf(), new Master.HelloTimeOut(), getContext().getSystem().getDispatcher(), null);

        log.debug("exiting processMessageHelloTimeOut "+toString());
    }

    private void processMessageReadyForWork(ReadyForWork rfw) {
        // ignore request if actor is already doing work
        if (actorsProcessingWorkItems.containsKey(rfw.getWorkerActorRef())) {
            return;
        }

        List<String> workItemsForWorker = workItemList.stream().limit(lineCountForWorkItem).collect(Collectors.toList());
        workItemList = workItemList.subList(workItemsForWorker.size(), workItemList.size());

        rfw.getWorkerActorRef().tell(new Worker.Work(workItemsForWorker, getSelf()), getSelf());

        Cancellable workTimeOutCancelHandle = getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10L),
                getSelf(), new WorkTimeout(rfw.getWorkerActorRef()), getContext().getSystem().getDispatcher(), null);

        actorWorkTimeOutCancelHandle.put(rfw.getWorkerActorRef(), workTimeOutCancelHandle);

        actorsProcessingWorkItems.put(rfw.getWorkerActorRef(), workItemsForWorker);

        log.debug("exiting processMessageReadyForWork "+toString());
    }

    private void processMessageWorkerTerminated(Terminated terminated) {
        workItemList.addAll(actorsProcessingWorkItems.get(terminated.getActor()));

        actorWorkTimeOutCancelHandle.remove(terminated.getActor());

        log.debug("exiting processMessageWorkerTerminated "+toString());
    }

    private void processMessageWorkTimeout(WorkTimeout wto) {
        workItemList.addAll(actorsProcessingWorkItems.remove(wto.getWorkActorRef()));

        log.debug("exiting processMessageWorkTimeout "+toString());
    }

    private void processMessageWorkDone(WorkDone workDone) {
        log.debug(toString());
        // drop work done if actor isn't doing work.
        if (!actorsProcessingWorkItems.containsKey(workDone.getWorkerActorRef())) {
            log.debug("processMessageWorkDone:: workDone dropped ");
            return;
        }

        wordCount = MapTools.concat(wordCount, workDone.results);

        actorWorkTimeOutCancelHandle.remove(workDone.getWorkerActorRef()).cancel();

        actorsProcessingWorkItems.remove(workDone.getWorkerActorRef());

        if ((workItemList.size() == 0) && (actorsProcessingWorkItems.size() == 0)) {
            fileReaderActorRef.tell(new FileReader.WorkBatchResults(wordCount, getSelf()), getSelf());

            fileReaderActorRef.tell(new FileReader.ReadyForBatch(getSelf()), getSelf());
        }

        log.debug("exiting processMessageWorkDone "+toString());
    }


    @Override
    public String toString() {
        return "Master{" + "\""+
                //", fileReaderActorRef=" + fileReaderActorRef +  "\""+
                //", routerActorRef=" + routerActorRef +  "\""+
                //", workBatchTimeOutCancelHandle=" + workBatchTimeOutCancelHandle +  "\""+
                //", helloTimeOutCancelHandle=" + helloTimeOutCancelHandle +  "\""+
                ", workItemList=" + workItemList +  "\""+
                ", wordCount=" + wordCount +  "\""+
                //", actorWorkTimeOutCancelHandle=" + actorWorkTimeOutCancelHandle +  "\""+
                ", actorsProcessingWorkItems=" + actorsProcessingWorkItems +  "\""+
                ", lineCountForWorkItem=" + lineCountForWorkItem + "\""+
                '}';

    }
}
