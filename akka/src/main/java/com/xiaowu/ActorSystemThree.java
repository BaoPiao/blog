package com.xiaowu;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ActorSystemThree {
    public static void main(String[] args) throws InterruptedException {
        Config load = ConfigFactory.load();
        ActorSystem akkademy3 = ActorSystem.create("akkademy3", load.getConfig("app3").withFallback(load));
        ActorRef pong3 = akkademy3.actorOf(Props.create(PongActor.class), "pong3");
        ActorSelection actorSelection = akkademy3.actorSelection("akka.tcp://akkademy1@127.0.0.1:2551/user/ping");
        actorSelection.tell("pong", pong3);

    }
}
