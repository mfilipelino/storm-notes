package com.mfilipelino;

import junit.framework.TestCase;

import java.util.List;


public class FakeDataTest extends TestCase {

    public void testCase1(){
        List<String> commits = FakeData.changeLog();
        assertEquals(commits.size(), 10);
    }

}