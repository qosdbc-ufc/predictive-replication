/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.commons;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public final class OutputMessage {

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("hh:mm:ss");

    private OutputMessage() {
    }

    public static void println(String message) {
        System.out.println("[" + dateFormat.format(new Date()) + "]: " + message);
    }

    public static void print(String message) {
        System.out.print("[" + dateFormat.format(new Date()) + "]: " + message);
    }

    public static void println(String message, boolean showTime) {
        System.out.println((showTime ? "[" + dateFormat.format(new Date()) + "]: " : "") + message);
    }

    public static void print(String message, boolean showTime) {
        System.out.print((showTime ? "[" + dateFormat.format(new Date()) + "]: " : "") + message);
    }
}