package com.xiaowu;

import akka.actor.AbstractActor;
import akka.actor.Status;

public class PongActor extends AbstractActor {


    @Override
    public Receive createReceive() {

        return receiveBuilder()
                .matchEquals("ping", s ->
                {
                    System.out.println(self().path() + " " + "receive " + sender().path() + " msg is " + s);
                    Thread.sleep(1000);
                    //sender()方法为获得发送者，然后调用它的tell方法发送消息给发送者
                    sender().tell("pong", self());
                })
                .matchAny(x ->
                {
                    System.out.println(getSender().toString() + ":" + x);
                    sender().tell(
                            new Status.Failure(new Exception("unknown message")), self()
                    );
                })
                .build();

    }
}
