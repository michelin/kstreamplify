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

/** Exception thrown when a file cannot be found. */
public class FileNotFoundException extends RuntimeException {
    private static final String FILE_NOT_FOUND_EXCEPTION = "File %s cannot be found";

    /**
     * Constructor.
     *
     * @param filename The name of the file that cannot be found
     */
    public FileNotFoundException(String filename) {
        super(FILE_NOT_FOUND_EXCEPTION.formatted(filename));
    }

    /**
     * Constructor.
     *
     * @param filename The name of the file that cannot be found
     * @param cause The cause of the exception
     */
    public FileNotFoundException(String filename, Throwable cause) {
        super(FILE_NOT_FOUND_EXCEPTION.formatted(filename), cause);
    }
}
