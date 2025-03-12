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

/** Exception thrown when a key is not found. */
public class UnknownKeyException extends RuntimeException {
    private static final String UNKNOWN_KEY = "Key %s not found";

    /**
     * Constructor.
     *
     * @param key The key that was not found
     */
    public UnknownKeyException(String key) {
        super(String.format(UNKNOWN_KEY, key));
    }
}
