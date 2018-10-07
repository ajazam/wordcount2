package com.ana3;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.routing.ClusterRouterPool;
import akka.cluster.routing.ClusterRouterPoolSettings;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.BroadcastPool;
import com.ana3.actors.FileReader;
import com.ana3.actors.Master;
import com.ana3.actors.Worker;
import com.ana3.file.Reader;
import com.ana3.file.ReaderImpl;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class WordCountMain {

    Config getUpdatedAkkaConfig(String hostip) {
        return ConfigFactory.parseString("akka.remote.artery.canonical.hostname=\"" + hostip + "\"\n" +
                "akka.remote.netty.tcp.hostname=\"" + hostip + "\"\n").withFallback(ConfigFactory.load());
    }

    private static void logAkkaConfiguratation(LoggingAdapter log, String parameterPath, Config akkaConfiguration) {
        log.info(parameterPath + " = " + akkaConfiguration.getString(parameterPath));
    }

    public static void main(String[] args) {
        String hostip = ConfigFactory.load().getString("hostip");
        boolean isMaster = ConfigFactory.load().getStringList("akka.cluster.roles").get(0).contains("master");

        WordCountMain main = new WordCountMain();

        Config akkaConfig = main.getUpdatedAkkaConfig(hostip);

        String currentdirectory = System.getProperty("user.dir");

        File fileCheck = new File(currentdirectory + "/" + akkaConfig.getString("file.name"));

        ActorSystem system = ActorSystem.create("WordCountSystem", akkaConfig);

        final LoggingAdapter log = Logging.getLogger(system, "main");

        logAkkaConfiguratation(log, "akka.remote.artery.canonical.hostname", akkaConfig);
        logAkkaConfiguratation(log, "akka.remote.netty.tcp.hostname", akkaConfig);

        log.info("dump file is " + akkaConfig.getString("file.name"));
        log.info("current directory is " + currentdirectory);

        Cluster.get(system).registerOnMemberUp(() -> startActors(isMaster, fileCheck, system, log));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            CompletionStage<Terminated> cs = system.getWhenTerminated();

            try {
                log.info("************************ finished counting words ************************. Press ^C on the master and workers to quit the application. ****************");
                cs.toCompletableFuture().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

        }));

    }

    private static void startActors(boolean isMaster, File fileCheck, ActorSystem system, LoggingAdapter log) {
        if (isMaster) {
            log.info("it's a master. Starting up master and file reader actor");


            if (!fileCheck.exists()) {
                System.out.println("Text file is not present");
                System.exit(0);
            }

            Reader reader = new ReaderImpl(fileCheck.getAbsolutePath());

            final ActorRef fileReaderActorRef = system.actorOf(FileReader.props(reader, 10000), "fileReader");

            final SupervisorStrategy routerStrategy = new OneForOneStrategy(60, Duration.ofMinutes(1), Collections.singletonList(Exception.class));
            routerStrategy.loggingEnabled();
            system.actorOf(Master.props(50,
                    (AbstractActor.ActorContext context) -> fileReaderActorRef,
                    (AbstractActor.ActorContext context) -> context.actorOf(new ClusterRouterPool(new BroadcastPool(1).withSupervisorStrategy(routerStrategy), new ClusterRouterPoolSettings(100, 8, false, "worker")).props(Props.create(Worker.class)), "router")

            ), "master");
        } else {
            log.info("it's a worker");
        }
    }
}