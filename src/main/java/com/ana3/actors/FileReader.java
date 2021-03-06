package com.ana3.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.ana3.file.Reader;
import com.ana3.util.MapTools;

import java.io.Serializable;
import java.util.*;

public class FileReader extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private Reader reader;
    private Map<String, Long> currentResult = new HashMap<>();
    private long workBatchSize;
    private long totalLinesRead = 0;
    private int receivedBatchresults;
    private int packetsSentToMaster;
    private List<String> workBatchLines = new ArrayList<>();
    private boolean workBatchCurrentlyBeingProcessed = false;

    /**
     * Message sent to FileReader by master to say it is ready for a batch of work
     */
    public static class ReadyForBatch implements Serializable {
        private ActorRef masterActorRef;

        public ReadyForBatch(ActorRef masterActorRef) {
            this.masterActorRef = masterActorRef;
        }

        ActorRef getMasterActorRef() {
            return masterActorRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ReadyForBatch that = (ReadyForBatch) o;

            return getMasterActorRef() != null ? getMasterActorRef().equals(that.getMasterActorRef()) : that.getMasterActorRef() == null;
        }

        @Override
        public int hashCode() {
            return getMasterActorRef() != null ? getMasterActorRef().hashCode() : 0;
        }

        @Override
        public String toString() {
            return "ReadyForBatch{" +
                    "masterActorRef=" + masterActorRef +
                    '}';
        }
    }

    /**
     * Message sent by master holding the results of processing the text lines
     */
    public static class WorkBatchResults implements Serializable {
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
    public static class RequestCurrentResults implements Serializable {
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
    public static class ResponseCurrentResults implements Serializable {
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

    public static Props props(Reader reader, long workBatchSize) {

        return Props.create(FileReader.class, () -> new FileReader(reader, workBatchSize));
    }

    public FileReader(Reader reader, long workBatchSize) {
        this.reader = reader;
        this.workBatchSize = workBatchSize;
        this.reader.init();
    }

    private void showResults() {
        if (currentResult.size() == 0) {
            log.debug("------Filereader.showResults. There are no results to show");
            return;
        }

        Map<String, Long> sortedWordCount = MapTools.reverseCountSort(currentResult);

        Map.Entry<String, Long> highestWordCount = MapTools.wordWithHighCount(sortedWordCount);
        Map.Entry<String, Long> lowestWordCount = MapTools.wordWithLowCount(sortedWordCount);

        log.info("################################################################################################################################################################");
        log.info("################################################################################################################################################################");
        //log.info("################################################# Filereader.showResults currentResults = {}",sortedWordCount);
        log.info("#######################################################");
        log.info("######################################### Filereader.showResults high count word = {}, count = {}, lowest count word = {}, count {}, ", highestWordCount.getKey(), highestWordCount.getValue(), lowestWordCount.getKey(), lowestWordCount.getValue());
        log.info("################################################################################################################################################################");
        log.info("################################################################################################################################################################");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(WorkBatchResults.class, this::processMessageWorkBatchResults)
                .match(ReadyForBatch.class, this::processMessageReadyForBatch)
                .match(RequestCurrentResults.class, this::processMessageRequestCurrentResults)
                .matchAny(o -> log.error("########## --- ########## FileReader.receive:: unknown packet {} ", o))
                .matchAny(o -> log.debug("------Filereader.receive unknown message"))
                .build();
    }

    private void processMessageWorkBatchResults(WorkBatchResults wr) {
        UUID uuid = UUID.randomUUID();
        log.debug(uuid + "{} ---Filereader.processMessageWorkBatchResults:: work batch results unique words count is {} ", wr.results.keySet().size());

        workBatchCurrentlyBeingProcessed = false;

        Map<String, Long> currentResultDup = new HashMap<>(currentResult);
        currentResult = MapTools.concat2(currentResultDup, wr.results);
        receivedBatchresults++;
        log.debug(uuid + "---Filereader.processMessageWorkBatchResults:: current cumulative unique word count is {} ",currentResult.keySet().size());
        log.info(uuid + "---Filereader.processMessageWorkBatchResults:: cumulative receivedBatchResultsPackets {}", receivedBatchresults);

        sendBatchToMaster(wr.getMasterActorRef());
        log.info("FileReader.processMessageWorkBatchResults:: from {}", wr.getMasterActorRef());
    }

    private void processMessageRequestCurrentResults(RequestCurrentResults r) {
        ResponseCurrentResults response = new ResponseCurrentResults(currentResult, getSelf());
        r.masterActorRef.tell(response, getSelf());
    }

    private void processMessageReadyForBatch(ReadyForBatch rb) {
        if (workBatchCurrentlyBeingProcessed){
            Master.WorkBatch workBatch = new Master.WorkBatch(workBatchLines, getSelf());
            rb.getMasterActorRef().tell(workBatch, getSelf());
            return;
        }

        sendBatchToMaster(rb.masterActorRef);
        log.info("FileReader.processMessageReadyForBatch:: from {}", rb.getMasterActorRef());
    }

    private void sendBatchToMaster(ActorRef masterActorRef){
        long lineCount = 0;
        workBatchLines.clear();

        while (lineCount < workBatchSize) {
            String currentList = reader.getLine();

            if (currentList == null) break;
            lineCount++;
            workBatchLines.add(currentList);
            totalLinesRead++;
        }

        if (workBatchLines.size() == 0) {
            showResults();
            masterActorRef.tell(PoisonPill.getInstance(), getSelf());
            getContext().stop(getSelf());
            return;
        }

        workBatchCurrentlyBeingProcessed = true;

        packetsSentToMaster++;

        log.debug("---Filereader.sendBatchToMaster:: new work batch size is {} lines " + workBatchLines.size());

        Master.WorkBatch workBatch = new Master.WorkBatch(workBatchLines, getSelf());
        masterActorRef.tell(workBatch, getSelf());
        log.debug("---Filereader.sendBatchToMaster:: readyForBatch ");
        log.info("---FileReader.sendBatchToMaster:: total lines read {}", totalLinesRead);
        log.info("---FileReader.sendBatchToMaster:: cumulative packets sent to master {}", packetsSentToMaster);
    }
}
