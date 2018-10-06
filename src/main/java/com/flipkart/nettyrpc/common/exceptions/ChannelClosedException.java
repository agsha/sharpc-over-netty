package com.flipkart.nettyrpc.common.exceptions;

public class ChannelClosedException extends Exception {
    public ChannelClosedException(String s) {
        super(s);
    }
}
