package me.suhyuk.yarn.helloworld.v1;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class HelloWorldClientTest {

    @Test
    public void print_character_and_numeric() {
        String str = "문자열";
        int i = 0;
        assertTrue("문자열0".equals(str + i));
    }
}
