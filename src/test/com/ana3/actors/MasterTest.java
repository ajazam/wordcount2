package com.ana3.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.ana3.util.WordCounter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class MasterTest extends JUnitSuite {

    static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
    }

    @Test
    public void testReadyForBatch() {
        new TestKit(system) {{
            final TestKit routerProbeActorRef = new TestKit(system);
            final TestKit fileReaderProbeActorRef = new TestKit(system);

            final ActorRef masterActerRef = system.actorOf(Master.props(2,
                    (AbstractActor.ActorContext context) -> {
                        return fileReaderProbeActorRef.getRef();
                    },
                    (AbstractActor.ActorContext context) -> {
                        return routerProbeActorRef.getRef();
                    }));
            FileReader.ReadyForBatch readyForBatch = new FileReader.ReadyForBatch(masterActerRef);
            fileReaderProbeActorRef.expectMsgEquals(Duration.ofSeconds(1), readyForBatch);

        }};
    }

    @Test
    public void testReadyForBatchTimeout() {
        new TestKit(system) {{
            final TestKit routerProbeActorRef = new TestKit(system);
            final TestKit fileReaderProbeActorRef = new TestKit(system);

            final ActorRef masterActerRef = system.actorOf(Master.props(2,
                    (AbstractActor.ActorContext context) -> {
                        return fileReaderProbeActorRef.getRef();
                    },
                    (AbstractActor.ActorContext context) -> {
                        return routerProbeActorRef.getRef();
                    }));


            FileReader.ReadyForBatch readyForBatch = new FileReader.ReadyForBatch(masterActerRef);
            List<Object> messages = fileReaderProbeActorRef.receiveN(2, Duration.ofSeconds(11));

            assertEquals(readyForBatch, messages.get(0));
            assertEquals(readyForBatch, messages.get(1));
        }};
    }
    @Test
    public void testWorkBatchTimeout(){
        new TestKit(system) {{
            final TestKit routerProbeActorRef = new TestKit(system);
            final TestKit fileReaderProbeActorRef = new TestKit(system);

            final ActorRef masterActerRef = system.actorOf(Master.props(2,
                    (AbstractActor.ActorContext context) -> {
                        return fileReaderProbeActorRef.getRef();
                    },
                    (AbstractActor.ActorContext context) -> {
                        return routerProbeActorRef.getRef();
                    }));


            FileReader.ReadyForBatch readyForBatch = new FileReader.ReadyForBatch(masterActerRef);
            fileReaderProbeActorRef.expectMsg(readyForBatch);


            fileReaderProbeActorRef.expectMsgEquals(Duration.ofSeconds(11), readyForBatch);

            fileReaderProbeActorRef.expectMsgEquals(Duration.ofSeconds(11), readyForBatch);

       }};
    }

    @Test
    public void testHelloTimeOut() {
        new TestKit(system) {{
            final TestKit routerProbeActorRef = new TestKit(system);
            final TestKit fileReaderProbeActorRef = new TestKit(system);

            final ActorRef masterActorRef = system.actorOf(Master.props(2,
                    (AbstractActor.ActorContext context) -> {
                        return fileReaderProbeActorRef.getRef();
                    },
                    (AbstractActor.ActorContext context) -> {
                        return routerProbeActorRef.getRef();
                    }));
            FileReader.ReadyForBatch readyForBatch = new FileReader.ReadyForBatch(masterActorRef);
            fileReaderProbeActorRef.expectMsgEquals(Duration.ofSeconds(1), readyForBatch);

            List<String> workBatchLines = new ArrayList<>();
            workBatchLines.add("But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born and I will give you a");
            workBatchLines.add("complete account of the system, and expound the actual teachings of the great explorer of the truth, the master-builder of human");
            workBatchLines.add("happiness. No one rejects, dislikes, or avoids pleasure itself, because it is pleasure, but because those who do not know how to");
            workBatchLines.add("pursue pleasure rationally encounter consequences that are extremely painful. Nor again is there anyone who loves or pursues or");
            Master.WorkBatch workBatch = new Master.WorkBatch(workBatchLines, fileReaderProbeActorRef.getRef());

            masterActorRef.tell(workBatch, routerProbeActorRef.getRef());
            Worker.Hello hello = new Worker.Hello(masterActorRef);

            List<Object> messages = routerProbeActorRef.receiveN(2, Duration.ofSeconds(11));
            assertEquals(hello, messages.get(0));
            assertEquals(hello, messages.get(1));
        }};
    }

    @Test
    public void testWorkReceptionByWorker() {
        new TestKit(system) {{
            final TestKit routerProbeActorRef = new TestKit(system);
            final TestKit fileReaderProbeActorRef = new TestKit(system);

            final ActorRef masterActorRef = system.actorOf(Master.props(2,
                    (AbstractActor.ActorContext context) -> {
                        return fileReaderProbeActorRef.getRef();
                    },
                    (AbstractActor.ActorContext context) -> {
                        return routerProbeActorRef.getRef();
                    }));
            FileReader.ReadyForBatch readyForBatch = new FileReader.ReadyForBatch(masterActorRef);
            fileReaderProbeActorRef.expectMsgEquals(Duration.ofSeconds(1), readyForBatch);

            List<String> workBatchLines = new ArrayList<>();
            workBatchLines.add("one two three");
            workBatchLines.add("four five six");
            workBatchLines.add("seven eight nine");
            workBatchLines.add("ten eleven twelve");
            Master.WorkBatch workBatch = new Master.WorkBatch(workBatchLines, fileReaderProbeActorRef.getRef());

            masterActorRef.tell(workBatch, routerProbeActorRef.getRef());
            Worker.Hello hello = new Worker.Hello(masterActorRef);
            routerProbeActorRef.expectMsgEquals(hello);

            Master.ReadyForWork readyForWork = new Master.ReadyForWork(routerProbeActorRef.getRef());
            masterActorRef.tell(readyForWork, routerProbeActorRef.getRef());

            List<String> workItems = new ArrayList<>();
            workItems.add("one two three");
            workItems.add("four five six");
            Worker.Work workReceived = new Worker.Work(workItems, masterActorRef);
            routerProbeActorRef.expectMsgEquals(workReceived);
        }};
    }

    @Test
    public void testWorkReceptionByWorkerWithTwoRoundsOfwork() {
        new TestKit(system) {{
            final TestKit routerProbeActorRef = new TestKit(system);
            final TestKit fileReaderProbeActorRef = new TestKit(system);

            final ActorRef masterActorRef = system.actorOf(Master.props(2,
                    (AbstractActor.ActorContext context) -> {
                        return fileReaderProbeActorRef.getRef();
                    },
                    (AbstractActor.ActorContext context) -> {
                        return routerProbeActorRef.getRef();
                    }));
            FileReader.ReadyForBatch readyForBatch = new FileReader.ReadyForBatch(masterActorRef);

            fileReaderProbeActorRef.expectMsgEquals(Duration.ofSeconds(1), readyForBatch);

            List<String> workBatchLines = new ArrayList<>();
            workBatchLines.add("one two three");
            workBatchLines.add("four five six");
            workBatchLines.add("seven eight nine");
            workBatchLines.add("ten eleven twelve");
            workBatchLines.add("thirteen fourteen fifteen");
            workBatchLines.add("sixteen seventeen eighteen");
            Master.WorkBatch workBatch = new Master.WorkBatch(workBatchLines, fileReaderProbeActorRef.getRef());

            masterActorRef.tell(workBatch, fileReaderProbeActorRef.getRef());
            Worker.Hello hello = new Worker.Hello(masterActorRef);
            routerProbeActorRef.expectMsgEquals(hello);

            // first request for work
            Master.ReadyForWork readyForWork = new Master.ReadyForWork(routerProbeActorRef.getRef());
            masterActorRef.tell(readyForWork, routerProbeActorRef.getRef());

            List<String> workItems = new ArrayList<>();
            workItems.add("one two three");
            workItems.add("four five six");
            Worker.Work workReceived = new Worker.Work(workItems, masterActorRef);
            routerProbeActorRef.expectMsgEquals(workReceived);

            Master.WorkDone workDone = new Master.WorkDone(WordCounter.count(workItems), routerProbeActorRef.getRef());
            masterActorRef.tell(workDone, routerProbeActorRef.getRef());

            // second request for work
            masterActorRef.tell(readyForWork, routerProbeActorRef.getRef());
            workItems.clear();
            workItems.add("seven eight nine");
            workItems.add("ten eleven twelve");
            workReceived = new Worker.Work(workItems, masterActorRef);
            routerProbeActorRef.expectMsgEquals(workReceived);
        }};
    };

    @Test
    public void testBatchResults() {
        new TestKit(system) {{
            final TestKit routerProbeActorRef = new TestKit(system);
            final TestKit fileReaderProbeActorRef = new TestKit(system);

            final ActorRef masterActorRef = system.actorOf(Master.props(2,
                    (AbstractActor.ActorContext context) -> {
                        return fileReaderProbeActorRef.getRef();
                    },
                    (AbstractActor.ActorContext context) -> {
                        return routerProbeActorRef.getRef();
                    }));
            FileReader.ReadyForBatch readyForBatch = new FileReader.ReadyForBatch(masterActorRef);
            fileReaderProbeActorRef.expectMsgEquals(Duration.ofSeconds(1), readyForBatch);

            List<String> workBatchLines = new ArrayList<>();
            workBatchLines.add("one two three");
            workBatchLines.add("four five six");
            workBatchLines.add("seven eight nine");
            workBatchLines.add("ten eleven twelve");
            Master.WorkBatch workBatch = new Master.WorkBatch(workBatchLines, fileReaderProbeActorRef.getRef());

            masterActorRef.tell(workBatch, routerProbeActorRef.getRef());
            Worker.Hello hello = new Worker.Hello(masterActorRef);
            routerProbeActorRef.expectMsgEquals(hello);

            // first request for work
            Master.ReadyForWork readyForWork = new Master.ReadyForWork(routerProbeActorRef.getRef());
            masterActorRef.tell(readyForWork, routerProbeActorRef.getRef());

            List<String> workItems = new ArrayList<>();
            workItems.add("one two three");
            workItems.add("four five six");
            Worker.Work workReceived = new Worker.Work(workItems, masterActorRef);
            routerProbeActorRef.expectMsgEquals(workReceived);

            Master.WorkDone workDone = new Master.WorkDone(WordCounter.count(workItems), routerProbeActorRef.getRef());
            masterActorRef.tell(workDone, routerProbeActorRef.getRef());

            // second request for work
            masterActorRef.tell(readyForWork, routerProbeActorRef.getRef());
            workItems.clear();
            workItems.add("seven eight nine");
            workItems.add("ten eleven twelve");
            workReceived = new Worker.Work(workItems, masterActorRef);
            routerProbeActorRef.expectMsgEquals(workReceived);

            workDone = new Master.WorkDone(WordCounter.count(workItems), routerProbeActorRef.getRef());
            masterActorRef.tell(workDone, routerProbeActorRef.getRef());

            List<Object> results = fileReaderProbeActorRef.receiveN(1);
            System.out.printf("results are "+results);
        }};
    }


    @Test
    public void testSecondBatchReception() {
        new TestKit(system) {{
            final TestKit routerProbeActorRef = new TestKit(system);
            final TestKit fileReaderProbeActorRef = new TestKit(system);

            final ActorRef masterActorRef = system.actorOf(Master.props(2,
                    (AbstractActor.ActorContext context) -> {
                        return fileReaderProbeActorRef.getRef();
                    },
                    (AbstractActor.ActorContext context) -> {
                        return routerProbeActorRef.getRef();
                    }));
            FileReader.ReadyForBatch readyForBatch = new FileReader.ReadyForBatch(masterActorRef);
            fileReaderProbeActorRef.expectMsgEquals(Duration.ofSeconds(1), readyForBatch);

            List<String> workBatchLines = new ArrayList<>();
            workBatchLines.add("one two three");
            workBatchLines.add("four five six");
            workBatchLines.add("seven eight nine");
            workBatchLines.add("ten eleven twelve");
            Master.WorkBatch workBatch = new Master.WorkBatch(workBatchLines, fileReaderProbeActorRef.getRef());

            masterActorRef.tell(workBatch, routerProbeActorRef.getRef());
            Worker.Hello hello = new Worker.Hello(masterActorRef);
            routerProbeActorRef.expectMsgEquals(hello);

            // first request for work
            Master.ReadyForWork readyForWork = new Master.ReadyForWork(routerProbeActorRef.getRef());
            masterActorRef.tell(readyForWork, routerProbeActorRef.getRef());

            List<String> workItems = new ArrayList<>();
            workItems.add("one two three");
            workItems.add("four five six");
            Worker.Work workReceived = new Worker.Work(workItems, masterActorRef);
            routerProbeActorRef.expectMsgEquals(workReceived);

            Master.WorkDone workDone = new Master.WorkDone(WordCounter.count(workItems), routerProbeActorRef.getRef());
            masterActorRef.tell(workDone, routerProbeActorRef.getRef());

            // second request for work
            masterActorRef.tell(readyForWork, routerProbeActorRef.getRef());
            workItems.clear();
            workItems.add("seven eight nine");
            workItems.add("ten eleven twelve");
            workReceived = new Worker.Work(workItems, masterActorRef);
            routerProbeActorRef.expectMsgEquals(workReceived);

            workDone = new Master.WorkDone(WordCounter.count(workItems), routerProbeActorRef.getRef());
            masterActorRef.tell(workDone, routerProbeActorRef.getRef());

            List<Object> results = fileReaderProbeActorRef.receiveN(2);
            System.out.printf("results are "+results);
        }};
    }

}
