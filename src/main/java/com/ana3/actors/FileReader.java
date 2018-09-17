package com.ana3.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.ana3.file.Reader;
import com.ana3.util.MapTools;

import java.util.*;

import static java.util.stream.Collectors.toMap;

public class FileReader extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private ActorRef masterActorRef;
    private Reader reader;
    private Map<String, Long> currentResult = new HashMap<>();
    private long workBatchSize = 1000;

    /**
     * Message sent to FileReader by master to say it is ready for a batch of work
     */
    public static class ReadyForBatch {
        private ActorRef masterActorRef;

        public ReadyForBatch(ActorRef masterActorRef) {
            this.masterActorRef = masterActorRef;
        }

        public ActorRef getMasterActorRef() {
            return masterActorRef;
        }
    }

    /**
     * Message sent to master holding work in the form of a list of text lines
     */
    public static class WorkBatch {
        private List<String> workItemList = new ArrayList<>();
        private ActorRef fileReaderActorRef;

        public WorkBatch(List<String> workItemList, ActorRef fileReaderActorRef) {
            this.workItemList = workItemList;
            this.fileReaderActorRef = fileReaderActorRef;
        }

        public List<String> getWorkItemList() {
            return workItemList;
        }

        public ActorRef getFileReaderActorRef() {
            return fileReaderActorRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WorkBatch workBatch = (WorkBatch) o;
            return Objects.equals(workItemList, workBatch.workItemList) &&
                    Objects.equals(fileReaderActorRef, workBatch.fileReaderActorRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workItemList, fileReaderActorRef);
        }

        @Override
        public String toString() {
            return "WorkBatch{" +
                    "workItemList=" + workItemList +
                    ", fileReaderActorRef=" + fileReaderActorRef +
                    '}';
        }
    }

    /**
     * Message sent by master holding the results of processing the text lines
     */
    public static class WorkBatchResults {
        private Map<String, Long> results;
        private ActorRef masterActorRef;

        public WorkBatchResults(Map<String, Long> results, ActorRef masterActorRef) {
            this.results = results;
            this.masterActorRef = masterActorRef;
        }

        public Map<String, Long> getResults() {
            return results;
        }

        public ActorRef getMasterActorRef() {
            return masterActorRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WorkBatchResults that = (WorkBatchResults) o;
            return Objects.equals(results, that.results) &&
                    Objects.equals(masterActorRef, that.masterActorRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(results, masterActorRef);
        }

        @Override
        public String toString() {
            return "WorkBatchResults{" +
                    "results=" + results +
                    ", masterActorRef=" + masterActorRef +
                    '}';
        }
    }

    /**
     * Request message used during testing to request the current current word count
     */
    public static class RequestCurrentResults {
        private ActorRef masterActorRef;

        public RequestCurrentResults(ActorRef masterActorRef) {
            this.masterActorRef = masterActorRef;
        }

        public ActorRef getMasterActorRef() {
            return masterActorRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RequestCurrentResults that = (RequestCurrentResults) o;
            return Objects.equals(masterActorRef, that.masterActorRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(masterActorRef);
        }

        @Override
        public String toString() {
            return "RequestCurrentResults{" +
                    "masterActorRef=" + masterActorRef +
                    '}';
        }
    }

    /**
     * Response message used during testing to hold the current word count
     */
    public static class ResponseCurrentResults {
        private Map<String, Long> results;
        private ActorRef fileReaderActorRef;

        public ResponseCurrentResults(Map<String, Long> results, ActorRef fileReaderActorRef) {
            this.results = results;
            this.fileReaderActorRef = fileReaderActorRef;
        }

        public Map<String, Long> getResults() {
            return results;
        }

        public ActorRef getFileReaderActorRef() {
            return fileReaderActorRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResponseCurrentResults that = (ResponseCurrentResults) o;
            return Objects.equals(results, that.results) &&
                    Objects.equals(fileReaderActorRef, that.fileReaderActorRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(results, fileReaderActorRef);
        }

        @Override
        public String toString() {
            return "ResponseCurrentResults{" +
                    "results=" + results +
                    ", fileReaderActorRef=" + fileReaderActorRef +
                    '}';
        }
    }

    public static Props props(ActorRef masterActorRef, Reader reader, long workBatchSize){

        return Props.create(FileReader.class, () -> new FileReader(masterActorRef, reader, workBatchSize));
    }

    public FileReader(ActorRef masterActorRef, Reader reader, long workBatchSize) {
        this.reader = reader;
        this.workBatchSize = workBatchSize;
        this.reader.init();
    }

    public void showResults(){
        if (currentResult.size()==0) {
            log.info("There no resuilts to show");
            return;
        }

        Map<String, Long> sortedByCount = currentResult
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(
                        toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                LinkedHashMap::new));
        log.info("words in descending sorted order." + sortedByCount);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadyForBatch.class, rb -> {
                    processMessageReadyForBatch(rb);
                })
                .match(RequestCurrentResults.class, r -> {
                    processMessageRequestCurrentResults(r);
                })
                .match(WorkBatchResults.class, wr ->{
                    processMessageWorkBatchResults(wr);
                })
                .matchAny(o -> log.info("received unknown message"))
                .build();
    }

    private void processMessageWorkBatchResults(WorkBatchResults wr) {
        currentResult = MapTools.concat(currentResult, wr.results);
    }

    private void processMessageRequestCurrentResults(RequestCurrentResults r) {
        ResponseCurrentResults response = new ResponseCurrentResults(currentResult, getSelf());
        r.masterActorRef.tell(response, getSelf());
    }

    private void processMessageReadyForBatch(ReadyForBatch rb) {
        List<String> workBatchLines = new ArrayList<>();
        long lineCount = 0;

        while(lineCount<workBatchSize) {
            String currentList = reader.getLine();
            if (currentList==null) break;
            lineCount++;
            workBatchLines.add(currentList);
        }

        if (workBatchLines.size()==0) {
            showResults();
            rb.getMasterActorRef().tell(PoisonPill.getInstance(), getSelf());
            getContext().stop(getSelf());
        }
        WorkBatch workBatch = new WorkBatch(workBatchLines, getSelf());
        rb.getMasterActorRef().tell(workBatch, getSelf());
    }
}
