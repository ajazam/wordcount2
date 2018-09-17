package com.ana3;

import static org.junit.Assert.*;

import com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;

public class WordCountMainTest {

    WordCountMain main;
    String ip;
    Config config;

    @Before
    public void setup(){
        main = new WordCountMain();
        ip = "172.16.0.17";
        config = main.getUpdatedAkkaConfig(ip, true);
    }

    @Test
    public void getNewConfigNettyHostnameTest(){
        assertEquals("172.16.0.17", config.getString("akka.remote.netty.tcp.hostname"));
    }

    @Test
    public void getNewConfigArteryHostnameTest(){
        assertEquals("172.16.0.17", config.getString("akka.remote.artery.canonical.hostname"));
    }

    @Test
    public void getNewConfigSeedNodesTest(){
        assertEquals("akka://WordCountSystem@172.16.0.17:2551", config.getStringList("akka.cluster.seed-nodes").get(0));
    }
}
