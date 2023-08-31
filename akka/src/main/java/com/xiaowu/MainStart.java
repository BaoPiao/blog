package com.xiaowu;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class MainStart {
    public static void main(String[] args) throws InterruptedException {
        Config load = ConfigFactory.load();

        ActorSystem akkademy1 = ActorSystem.create("akkademy1", load.getConfig("app1").withFallback(load));

        ActorSystem akkademy2 = ActorSystem.create("akkademy2", load.getConfig("app2").withFallback(load));

        ActorRef pongActor = akkademy2.actorOf(Props.create(PongActor.class), "pong");

        ActorRef actorRef = akkademy1.actorOf(Props.create(PingActor.class), "ping");

        actorRef.tell("pong", pongActor);
    }
}
