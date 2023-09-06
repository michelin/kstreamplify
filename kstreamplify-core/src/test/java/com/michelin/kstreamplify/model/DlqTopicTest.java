package com.michelin.kstreamplify.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

public class DlqTopicTest {

    @Mock
    private DlqTopic dlqTopicMock;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDlqTopicName() {
        DlqTopic dlqTopic = DlqTopic.builder()
                .name("TestTopic")
                .build();

        when(dlqTopicMock.getName()).thenReturn("TestTopic");

        assertEquals("TestTopic", dlqTopic.getName());

        dlqTopic.builder().toString();
    }
}