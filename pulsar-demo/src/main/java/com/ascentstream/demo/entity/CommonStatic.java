package com.ascentstream.demo.entity;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CommonStatic {

    public final static Map<String, String> map4VerifyingDelayedMsg = new ConcurrentHashMap<>();

    public static void recordSentMessage(String messageValue){
        map4VerifyingDelayedMsg.put(messageValue, "messageValue");
    }

    public static void recordReceivedMessage(String messageValue){
        map4VerifyingDelayedMsg.remove(messageValue);
    }
}
