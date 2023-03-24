package com.ccsu.utils;

public class Panic {
    /**
     * Terminate the program when an unforeseen error is sent
     * @param err
     */
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
