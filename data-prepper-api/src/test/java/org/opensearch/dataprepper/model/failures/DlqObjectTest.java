/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.failures;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class DlqObjectTest {

    private static final String ISO8601_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private String pluginId;
    private String pluginName;
    private String pipelineName;
    private Object failedData;

    @BeforeEach
    public void setUp() {
        pluginId = randomUUID().toString();
        pluginName = randomUUID().toString();
        pipelineName = randomUUID().toString();
        failedData = randomUUID();
    }

    @Test
    public void testBuild() {

        final DlqObject testObject = DlqObject.builder()
                .withPluginId(pluginId)
                .withPluginName(pluginName)
                .withPipelineName(pipelineName)
                .withFailedData(failedData)
                .build();

        assertThat(testObject, is(notNullValue()));
    }

    @Nested
    public class InvalidBuildParameters {

        private void createTestObject() {
            DlqObject.builder()
                .withPluginId(pluginId)
                .withPluginName(pluginName)
                .withPipelineName(pipelineName)
                .withFailedData(failedData)
                .build();
        }
        @Test
        public void testInvalidPluginId() {
            pluginId = null;
            assertThrows(NullPointerException.class, this::createTestObject);
            pluginId = "";
            assertThrows(IllegalArgumentException.class, this::createTestObject);
        }

        @Test
        public void testInvalidPluginName() {
            pluginName = null;
            assertThrows(NullPointerException.class, this::createTestObject);
            pluginName = "";
            assertThrows(IllegalArgumentException.class, this::createTestObject);
        }

        @Test
        public void testInvalidPipelineName() {
            pipelineName = null;
            assertThrows(NullPointerException.class, this::createTestObject);
            pipelineName = "";
            assertThrows(IllegalArgumentException.class, this::createTestObject);
        }

        @Test
        public void testInvalidFailedData() {
            failedData = null;
            assertThrows(NullPointerException.class, this::createTestObject);
        }
    }

    @Nested
    class Getters {

        private DlqObject testObject;

        @BeforeEach
        public void setup() {

            testObject = DlqObject.builder()
                .withPluginId(pluginId)
                .withPluginName(pluginName)
                .withPipelineName(pipelineName)
                .withFailedData(failedData)
                .build();
        }

        @Test
        public void testGetPluginId() {
            final String actualPluginId = testObject.getPluginId();
            assertThat(actualPluginId, is(notNullValue()));
            assertThat(actualPluginId, is(pluginId));
        }

        @Test
        public void testGetPluginName() {
            final String actualPluginName = testObject.getPluginName();
            assertThat(actualPluginName, is(notNullValue()));
            assertThat(actualPluginName, is(pluginName));
        }

        @Test
        public void testGetPipelineName() {
            final String actualPipelineName = testObject.getPipelineName();
            assertThat(actualPipelineName, is(notNullValue()));
            assertThat(actualPipelineName, is(pipelineName));
        }

        @Test
        public void testGetFailedData() {
            final Object actualFailedData = testObject.getFailedData();
            assertThat(actualFailedData, is(notNullValue()));
            assertThat(actualFailedData, is(failedData));
        }

        @Test
        public void testGetTimestamp() {
            final String string = testObject.getTimestamp();
            assertThat(string, is(notNullValue()));

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(ISO8601_FORMAT_STRING);  // Specify locale to determine human language and cultural norms used in translating that input string.
            Instant actualTimestamp = LocalDateTime.parse(testObject.getTimestamp(), formatter)
                .atZone(ZoneId.systemDefault().normalized())
                .toInstant();

            assertThat(actualTimestamp, is(lessThanOrEqualTo(Instant.now())));
        }

        @Test
        public void testGetVersion() {
            final String string = testObject.getVersion();
            assertThat(string, is(notNullValue()));
            assertThat(string, is(equalTo("1")));
        }
    }


    @Nested
    class EqualsAndToString {

        private DlqObject testObject;

        @BeforeEach
        public void setup() {
            testObject = DlqObject.builder()
                .withPluginId(pluginId)
                .withPluginName(pluginName)
                .withPipelineName(pipelineName)
                .withFailedData(failedData)
                .build();
        }

        @Test
        void equals_returns_false_for_null() {
            assertThat(testObject.equals(null), equalTo(false));
        }

        @Test
        void equals_on_same_instance_returns_true() {
            assertThat(testObject, equalTo(testObject));
        }

        @Test
        void equals_returns_false_for_two_instances_with_different_values() throws InterruptedException {
            TimeUnit.SECONDS.sleep(1); // To ensure that the timestamp is different

            final DlqObject otherTestObject = DlqObject.builder()
                .withPluginId(pluginId)
                .withPluginName(pluginName)
                .withPipelineName(pipelineName)
                .withFailedData(failedData)
                .build();

            assertThat(testObject, is(not(equalTo(otherTestObject))
            ));
        }

        @Test
        void toString_has_all_values() {
            final String string = testObject.toString();

            assertThat(string, notNullValue());
            assertThat(string, allOf(
                containsString("DlqObject"),
                containsString(pluginId),
                containsString(pluginName),
                containsString(pipelineName),
                containsString(failedData.toString())
            ));
        }
    }
}
