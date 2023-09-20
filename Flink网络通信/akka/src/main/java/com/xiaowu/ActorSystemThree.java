package com.xiaowu;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ActorSystemThree {
    //得先启动MainStart，然后再启动本类
    public static void main(String[] args) throws InterruptedException {
        Config load = ConfigFactory.load();
        //load.getConfig("app3") 使用配置app3来启动这个system，避免端口冲突
        ActorSystem actorSystem3 = ActorSystem.create("actorSystem3", load.getConfig("app3").withFallback(load));
        ActorRef actorSystem3PongActorRef = actorSystem3.actorOf(Props.create(PongActor.class), "pong3");
        //可以通过actorSelection()方法，获得需要的actor的引用类，可以向其发送消息
        ActorSelection system1PingActorSelection = actorSystem3.actorSelection("akka.tcp://actorSystem1@127.0.0.1:2551/user/ping");
       //远程发送消息给System1Ping
        system1PingActorSelection.tell("pong", actorSystem3PongActorRef);

    }
}
