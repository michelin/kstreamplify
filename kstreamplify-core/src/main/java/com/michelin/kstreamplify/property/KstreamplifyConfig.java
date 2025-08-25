/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kstreamplify.property;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class KstreamplifyConfig {

    /** The DLQ properties prefix. */
    public static final String DLQ_PROPERTIES_PREFIX = "dlq";
    /** Flag to enable handling of Schema Registry RestClient exceptions during deserialization. */
    public static boolean handleSchemaRegistryRestException = false;
    /** Property key to configure handling of Schema Registry RestClient exceptions in DLQ deserialization. */
    public static final String DLQ_DESERIALIZATION_HANDLER_FORWARD_REST_CLIENT_EXCEPTION =
            "dlq.deserialization-handler.forward-restclient-exception";
}
