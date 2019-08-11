package org.rcc.tools.yarn.worker;

public class HelloWorld {

    public static void main(String [] args) throws InterruptedException {
        System.out.println("Simulate distributed hello world.");
        Thread.sleep(20000);
        System.out.println("End distributed hello world.");

    }
}
