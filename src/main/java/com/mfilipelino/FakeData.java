package com.mfilipelino;


import java.util.LinkedList;
import java.util.List;

public class FakeData {

    public static List<String> changeLog() {
        List<String> commits = new LinkedList<String>();
        for (int i = 0; i < 10; i++) {
            commits.add("asd " + "mfilipelino@gmail.com");
        }
        return commits;
    }
}
