package com.ana3;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class WordCountMain {

    Config getUpdatedAkkaConfig(String hostip, boolean isMaster){
        Config config = ConfigFactory.load();

        config = ConfigFactory.parseString("akka.remote.artery.canonical.hostname=\"" + hostip + "\"\n" +
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

        ActorSystem system = ActorSystem.create("WordCountSystem", akkaConfig);

        final LoggingAdapter log = Logging.getLogger(system, "main");

        String currentdirectory = System.getProperty("user.dir");

        logAkkaConfiguratation(log, "akka.remote.artery.canonical.hostname", akkaConfig);
        logAkkaConfiguratation(log, "akka.remote.netty.tcp.hostname", akkaConfig);

        log.info("dump file is "+akkaConfig.getString("file.name"));

        log.info("current directory is "+currentdirectory);

        if (isMaster) {
            log.info("it's a master");
        } else {
            log.info("ot's a worker");
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
