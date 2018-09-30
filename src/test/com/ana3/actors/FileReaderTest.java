package com.ana3.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.ana3.file.Reader;
import com.ana3.file.ReaderDummyImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class FileReaderTest extends JUnitSuite {

    private static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void testIt() {
        new TestKit(system) {{
            final TestKit probe = new TestKit(system);

            Reader reader = new ReaderDummyImpl();
            final ActorRef fileReaderActor = system.actorOf(FileReader.props(reader, 2), "FileReaderActor");

            List<String> workItemList = new ArrayList<>();
            Master.WorkBatch workBatch;

            FileReader.ReadyForBatch readyForBatch = new FileReader.ReadyForBatch(probe.getRef());

            fileReaderActor.tell(readyForBatch, probe.getRef());
            workItemList.add("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            workItemList.add("<letter>");
            workBatch = new Master.WorkBatch(workItemList, fileReaderActor);
            probe.expectMsg(Duration.ofSeconds(2), workBatch);


            fileReaderActor.tell(readyForBatch, probe.getRef());
            workItemList.clear();
            workItemList.add("    <title maxlength=\"10\"> Quote Letter </title>");
            workItemList.add("    <salutation limit=\"40\">Dear Daniel,</salutation>");
            workBatch = new Master.WorkBatch(workItemList, fileReaderActor);
            probe.expectMsg(Duration.ofSeconds(2), workBatch);

            Map<String, Long> wordsAndCounts = new HashMap<>();
            wordsAndCounts.put("apple", 1L);
            wordsAndCounts.put("orange", 2L);
            wordsAndCounts.put("blackberry", 3L);
            FileReader.WorkBatchResults results = new FileReader.WorkBatchResults(wordsAndCounts, probe.getRef());

            fileReaderActor.tell(results, probe.getRef());

            fileReaderActor.tell(results, probe.getRef());

            FileReader.RequestCurrentResults request = new FileReader.RequestCurrentResults(probe.getRef());
            fileReaderActor.tell(request, probe.getRef());

            Map<String, Long> expectedResults = new HashMap<>();
            expectedResults.put("apple", 2L);
            expectedResults.put("orange", 4L);
            expectedResults.put("blackberry", 6L);

            FileReader.ResponseCurrentResults responseCurrentResults = new FileReader.ResponseCurrentResults(expectedResults, fileReaderActor);
            probe.expectMsgEquals(Duration.ofSeconds(2), responseCurrentResults);


            fileReaderActor.tell(readyForBatch, probe.getRef());
            workItemList.clear();
            workItemList.add("    <text>Thank you f or sending us the information on <emphasis>SDL Trados Studio 2009</emphasis>.");
            workItemList.add("        We like your products and think they certainly represent the most powerful translation solution on the market.");
            workBatch = new Master.WorkBatch(workItemList, fileReaderActor);
            probe.expectMsg(Duration.ofSeconds(2), workBatch);

            fileReaderActor.tell(readyForBatch, probe.getRef());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignore) {
            }
            assertTrue(fileReaderActor.isTerminated());
        }};
    }
}
