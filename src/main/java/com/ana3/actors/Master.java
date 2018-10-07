package com.ana3.actors;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.ana3.util.MapTools;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

public class Master extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private ActorRef fileReaderActorRef;
    private ActorRef routerActorRef;
    private Cancellable workBatchTimeOutCancelHandle;
    private Cancellable helloTimeOutCancelHandle;
    private LinkedBlockingQueue<String> workItemList = new LinkedBlockingQueue<>();
    private Map<String, Long> wordCount = new HashMap<>();
    private Map<ActorRef, Cancellable> actorWorkTimeOutCancelHandle = new HashMap<>();
    private Map<ActorRef, List<String>> actorsProcessingWorkItems = new HashMap<>();
    private int lineCountForWorkItem;
    private boolean waitingForWorkBatch;
    private boolean sentInitialHello;

    /**
     * Message sent to self to inform that work batch from file reader hasn't been received
     */
    public static class WorkBatchTimeOut implements Serializable {
        private ActorRef fileReaderActorRef;

        public WorkBatchTimeOut(ActorRef fileReaderActorRef) {
            this.fileReaderActorRef = fileReaderActorRef;
        }

        @Override
        public String toString() {
            return "WorkBatchTimeOut{" +
                    "fileReaderActorRef=" + fileReaderActorRef +
                    '}';
        }
    }

    /**
     * Contains work from the file reader
     */
    public static class WorkBatch implements Serializable {
        private List<String> workItems;
        private ActorRef fileReaderActorRef;

        public WorkBatch(List<String> workItems, ActorRef fileReaderActorRef) {
            this.workItems = workItems;
            this.fileReaderActorRef = fileReaderActorRef;
        }

        List<String> getWorkItems() {
            return workItems;
        }

        ActorRef getFileReaderActorRef() {
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
    public static class HelloTimeOut implements Serializable {
        public HelloTimeOut() {
        }

    }

    /**
     * Sent by worker to notify that it's ready for work
     */
    public static class ReadyForWork implements Serializable {
        private ActorRef workerActorRef;

        public ReadyForWork(ActorRef workerActorRef) {
            this.workerActorRef = workerActorRef;
        }

        ActorRef getWorkerActorRef() {
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
    public static class WorkTimeout implements Serializable {
        private ActorRef workActorRef;

        public WorkTimeout(ActorRef workActorRef) {
            this.workActorRef = workActorRef;
        }

        ActorRef getWorkActorRef() {
            return workActorRef;
        }

        @Override
        public String toString() {
            return "WorkTimeout{" +
                    "workActorRef=" + workActorRef +
                    '}';
        }
    }

    /**
     * Send by worker containing work done
     */
    public static class WorkDone implements Serializable {
        private Map<String, Long> results;
        private ActorRef workerActorRef;

        WorkDone(Map<String, Long> results, ActorRef workerActorRef) {
            this.results = results;
            this.workerActorRef = workerActorRef;
        }

        Map<String, Long> getResults() {
            return results;
        }

        ActorRef getWorkerActorRef() {
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

        //-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        // need the following to tell FileReader actor to send a batch of work after 15 seconds. It'll give the master time to finish starting up. really really important. Lost many days over this.
        //-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        this.fileReaderActorRef.tell(new FileReader.ReadyForBatch(getSelf()), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(WorkBatchTimeOut.class, this::processMessageWorkBatchTimeout)
                .match(WorkBatch.class, this::processMessageWorkBatch)
                .match(HelloTimeOut.class, this::processMessageHelloTimeOut)
                .match(WorkDone.class, this::processMessageWorkDone)
                .match(ReadyForWork.class, this::processMessageReadyForWork)
                .match(Terminated.class, this::processMessageWorkerTerminated)
                .match(WorkTimeout.class, this::processMessageWorkTimeout)
                .matchAny(o -> log.error("########## --- ########## Master.receive:: unknown packet {}", o))
                .build();
    }

    private void processMessageWorkBatchTimeout(WorkBatchTimeOut to) {
        workBatchTimeOutCancelHandle = getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10L),
                getSelf(), new Master.WorkBatchTimeOut(fileReaderActorRef), getContext().getSystem().getDispatcher(), getSelf());

        fileReaderActorRef.tell(new FileReader.ReadyForBatch(getSelf()), getSelf());
        waitingForWorkBatch = false;
        log.info("---Master.processMessageWorkBatchTimeout:: ");
    }

    private void processMessageWorkBatch(WorkBatch wb) {
        List<String> itemstoAddtoWorkItemList = new ArrayList<>(wb.workItems);
        log.info("---Master.processMessageWorkBatch:: itemlist before new workbatch = {} ", workItemList.size());
        this.workItemList.addAll(itemstoAddtoWorkItemList);

        if (!sentInitialHello) {
            sentInitialHello = true;
            routerActorRef.tell(new Worker.Hello(getSelf()), getSelf());
            helloTimeOutCancelHandle = getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10L),
                    getSelf(), new Master.HelloTimeOut(), getContext().getSystem().getDispatcher(), getSelf());
        }
        waitingForWorkBatch = false;
        log.info("---Master.processMessageWorkBatch:: {} received ", wb.getWorkItems().size());
    }

    private void processMessageHelloTimeOut(HelloTimeOut helloTimeOut) {
        routerActorRef.tell(new Worker.Hello(getSelf()), getSelf());

        helloTimeOutCancelHandle = getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10L),
                getSelf(), new Master.HelloTimeOut(), getContext().getSystem().getDispatcher(), getSelf());

        log.debug("---Master.processMessageHelloTimeOut:: {}", helloTimeOut);
        log.info("---Master.processMessageHelloTimeOut:: {}", getSelf());
    }

    private void processMessageReadyForWork(ReadyForWork rfw) {

        helloTimeOutCancelHandle.cancel();

        Cancellable actorWorkTimeOuthandle = actorWorkTimeOutCancelHandle.remove(rfw.getWorkerActorRef());
        if (actorWorkTimeOuthandle != null) {
            actorWorkTimeOuthandle.cancel();
        }

        List<String> workItemsForWorker = sendWorkToWorker(rfw.getWorkerActorRef());

        log.debug("---Master.processMessageReadyForWork:: workItems returned to worker {}",rfw.getWorkerActorRef());
    }

    private List<String> sendWorkToWorker(ActorRef workerActorRef) {
        List<String> workItemsForWorker = new ArrayList<>();

        Cancellable workTimeOutCancelHandle = getContext().getSystem().scheduler().scheduleOnce(Duration.ofSeconds(10L),
                getSelf(), new WorkTimeout(workerActorRef), getContext().getSystem().getDispatcher(), getSelf());

        actorWorkTimeOutCancelHandle.put(workerActorRef, workTimeOutCancelHandle);

        workItemList.drainTo(workItemsForWorker, lineCountForWorkItem);
        log.debug("---Master.sendWorkToWorker:: Assigned new workItems to {}", workerActorRef);

        workerActorRef.tell(new Worker.Work(workItemsForWorker, getSelf()), getSelf());

        actorsProcessingWorkItems.put(workerActorRef, workItemsForWorker);
        return workItemsForWorker;
    }

    private void processMessageWorkerTerminated(Terminated terminated) {
        List<String> linesToPutBackIntoWorkItemList = actorsProcessingWorkItems.remove(terminated.getActor());
        workItemList.addAll(linesToPutBackIntoWorkItemList);

        actorWorkTimeOutCancelHandle.remove(terminated.getActor());

        log.info("---Master.processMessageWorkerTerminated:: {}", toString());
    }

    private void processMessageWorkTimeout(WorkTimeout wto) {
        List<String> itemsToAddToWorkItemList = actorsProcessingWorkItems.remove(wto.getWorkActorRef());
        if (itemsToAddToWorkItemList != null) workItemList.addAll(itemsToAddToWorkItemList);

        log.info("---Master.processMessageWorkTimeout:: {}", wto.getWorkActorRef());
    }

    private void processMessageWorkDone(WorkDone workDone) {
        log.debug(toString());
        // drop work done if actor isn't doing work.
        UUID uuid = UUID.randomUUID();
        log.debug(uuid + "---Master.processMessageWorkDone:: wordcount pre update = {}", wordCount.size());
        if (!actorsProcessingWorkItems.containsKey(workDone.getWorkerActorRef())) {
            log.info(uuid + "---Master.processMessageWorkDone:: workDone dropped, actor not in actorsProcessingWorkItems");
            log.debug(uuid + "---Master.processMessageWorkDone:: self = {} {}", uuid, getSelf());
            return;
        }

        Map<String, Long> wordCountDup = new HashMap<>(wordCount);

        wordCount = MapTools.concat2(wordCountDup, workDone.results);

        actorWorkTimeOutCancelHandle.remove(workDone.getWorkerActorRef()).cancel();

        actorsProcessingWorkItems.remove(workDone.getWorkerActorRef());

        if ((workItemList.size() == 0) && !waitingForWorkBatch) {
            final Map<String, Long> finalBatchWordCount = new HashMap<>(wordCount);
            fileReaderActorRef.tell(new FileReader.WorkBatchResults(finalBatchWordCount, getSelf()), getSelf());
            waitingForWorkBatch = true;
            //TODO remove the following line
            //fileReaderActorRef.tell(new FileReader.ReadyForBatch(getSelf()), getSelf());
            wordCount.clear();
            log.debug(uuid + "---Master.processMessageWorkDone:: no more work. word count cleared");
            log.debug(uuid + "---Master.processMessageWorkDone:: finalBatchWordCount unique word count = {}", finalBatchWordCount.size());
        } else {
            sendWorkToWorker(workDone.getWorkerActorRef());
        }

        log.debug(uuid + "---Master.processMessageWorkDone:: workdone  words = {}", workDone.getResults().size());
        log.debug(uuid + "---Master.processMessageWorkDone:: wordcount size postupdate = {}", wordCount.size());
        //log.info(uuid+ "---Master.processMessageWorkDone:: workItemList = {}", workItemList.size());
    }


    @Override
    public String toString() {
        return "Master{" + "\"" +
                //", fileReaderActorRef=" + fileReaderActorRef +  "\""+
                //", routerActorRef=" + routerActorRef +  "\""+
                //", workBatchTimeOutCancelHandle=" + workBatchTimeOutCancelHandle +  "\""+
                //", helloTimeOutCancelHandle=" + helloTimeOutCancelHandle +  "\""+
                ", workItemList=" + workItemList + "\"" +
                ", wordCount=" + wordCount + "\"" +
                //", actorWorkTimeOutCancelHandle=" + actorWorkTimeOutCancelHandle +  "\""+
                ", actorsProcessingWorkItems=" + actorsProcessingWorkItems + "\"" +
                ", lineCountForWorkItem=" + lineCountForWorkItem + "\"" +
                '}';

    }
}
