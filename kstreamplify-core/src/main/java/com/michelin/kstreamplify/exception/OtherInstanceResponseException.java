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

package com.michelin.kstreamplify.exception;

/**
 * Exception thrown when a response from another instance cannot be read.
 */
public class OtherInstanceResponseException extends RuntimeException {
    private static final String OTHER_INSTANCE_RESPONSE = "Fail to read other instance response";

    /**
     * Constructor.
     *
     * @param cause The cause of the exception
     */
    public OtherInstanceResponseException(Throwable cause) {
        super(OTHER_INSTANCE_RESPONSE, cause);
    }
}
