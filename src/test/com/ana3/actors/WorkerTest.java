package com.ana3.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.testkit.javadsl.TestKit;
import org.junit.*;
import org.scalatest.junit.JUnitSuite;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class WorkerTest  extends JUnitSuite {

    static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown(){
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void helloMessageAndWorkReceiveTimeOutTest(){
        new TestKit(system) {{
            final TestKit probe = new TestKit(system);
            final ActorRef workerActor = system.actorOf(Worker.props(), "workerActor");

            Worker.Hello hello = new Worker.Hello(probe.getRef());
            workerActor.tell(hello, probe.getRef());

            Master.ReadyForWork readyForWork = new Master.ReadyForWork(workerActor);
            probe.expectMsg(Duration.ofSeconds(1), readyForWork);
            probe.expectMsg(Duration.ofSeconds(11), readyForWork);
            workerActor.tell(PoisonPill.getInstance(), getRef());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }

            assertEquals(true, workerActor.isTerminated());
        }};
    }

    @Test
    public void testWork(){
        new TestKit(system) {{
            final TestKit probe = new TestKit(system);
            final ActorRef workerActor = system.actorOf(Worker.props(), "workerActor2");

            List<String> lines = new ArrayList<>();
            lines.add("the the the");
            lines.add("the the the");

            Worker.Work work = new Worker.Work(lines, probe.getRef());
            workerActor.tell(work, getRef());

            Map<String, Long> results = new HashMap<>();
            results.put("the", 6L);
            Master.WorkDone workDone = new Master.WorkDone(results, workerActor);

            probe.expectMsgEquals(Duration.ofSeconds(1), workDone);

            workerActor.tell(PoisonPill.getInstance(), getRef());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }

            assertEquals(true, workerActor.isTerminated());
        }};
    }
}
