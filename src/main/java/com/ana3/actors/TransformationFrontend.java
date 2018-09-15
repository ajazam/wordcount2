package com.ana3.actors;

import static com.ana3.TransformationMessages.BACKEND_REGISTRATION;

import java.util.ArrayList;
import java.util.List;

import com.ana3.TransformationMessages.JobFailed;
import com.ana3.TransformationMessages.TransformationJob;
import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.AbstractActor;

public class TransformationFrontend extends AbstractActor {

  List<ActorRef> backends = new ArrayList<>();
  int jobCounter = 0;

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(TransformationJob.class, job -> backends.isEmpty(), job -> {
        sender().tell(new JobFailed("Service unavailable, try again later", job),
          sender());
      })
      .match(TransformationJob.class, job -> {
        jobCounter++;
        backends.get(jobCounter % backends.size())
          .forward(job, getContext());
      })
      .matchEquals(BACKEND_REGISTRATION, message -> {
        getContext().watch(sender());
        backends.add(sender());
      })
      .match(Terminated.class, terminated -> {
        backends.remove(terminated.getActor());
      })
      .build();
  }
}
