package com.xiaowu;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class MainStart {
    public static void main(String[] args) throws InterruptedException {
        Config load = ConfigFactory.load();

        //创建system
        ActorSystem actorSystem1 = ActorSystem.create("actorSystem1", load.getConfig("app1").withFallback(load));
        ActorSystem actorSystem2 = ActorSystem.create("actorSystem2", load.getConfig("app2").withFallback(load));

        //通过system创建actor
        ActorRef system2PongActorRef = actorSystem2.actorOf(Props.create(PongActor.class), "pong");
        ActorRef system1PingActorRef = actorSystem1.actorOf(Props.create(PingActor.class), "ping");

        //给system1ping发送 ‘pong消息’，并标明发送者是system2Pong
        system1PingActorRef.tell("pong", system2PongActorRef);
    }
}
