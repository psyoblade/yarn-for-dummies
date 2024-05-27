package me.suhyuk.yarn.helloworld.v1;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class HelloWorldClientTest {

    @Test
    public void print_character_and_numeric() {
        String str = "characters";
        int i = 0;
        assertTrue("characters0".equals(str + i));
    }
}
