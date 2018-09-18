package com.ana3;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.ana3.actors.FileReader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class WordCountMain {

    Config getUpdatedAkkaConfig(String hostip, boolean isMaster){
        Config config = ConfigFactory.parseString("akka.remote.artery.canonical.hostname=\"" + hostip + "\"\n" +
                "akka.remote.netty.tcp.hostname=\"" + hostip + "\"\n").withFallback(ConfigFactory.load());
        return config;
    }

    public static void logAkkaConfiguratation(LoggingAdapter log, String parameterPath, Config akkaConfiguration){
        log.info(parameterPath+" = "+akkaConfiguration.getString(parameterPath));
    }

    public static void main(String[] args) {
        String hostip = ConfigFactory.load().getString("hostip");
        boolean isMaster = ConfigFactory.load().getStringList("akka.cluster.roles").get(0).contains("master");

        WordCountMain main = new WordCountMain();

        Config akkaConfig = main.getUpdatedAkkaConfig(hostip, isMaster);

        String currentdirectory = System.getProperty("user.dir");

        File fileCheck = new File(currentdirectory+"/"+ akkaConfig.getString("file.name"));

        if (!fileCheck.exists()) {
            System.out.println("Text file is not present");
            System.exit(0);
        }

        ActorSystem system = ActorSystem.create("WordCountSystem", akkaConfig);

        final LoggingAdapter log = Logging.getLogger(system, "main");

        logAkkaConfiguratation(log, "akka.remote.artery.canonical.hostname", akkaConfig);
        logAkkaConfiguratation(log, "akka.remote.netty.tcp.hostname", akkaConfig);

        log.info("dump file is "+akkaConfig.getString("file.name"));

        log.info("current directory is "+currentdirectory);

        if (isMaster) {
            //TODO start master actor here
            log.info("it's a master. Starting up file reader actor");
            final ActorRef fileReaderActorRef = system.actorOf(
                    Props.create(FileReader.class), "filereader");
        } else {
            log.info("it's a worker");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            CompletionStage<Terminated> cs = system.getWhenTerminated();

            try {
                cs.toCompletableFuture().get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }));

    }

}
