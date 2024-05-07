package com.michelin.kstreamplify.http.service;

import com.fasterxml.jackson.annotation.JsonIgnore;

abstract class JacksonIgnoreAvroMixIn {
    @JsonIgnore
    public abstract org.apache.avro.Schema getSchema();

    @JsonIgnore
    public abstract org.apache.avro.specific.SpecificData getSpecificData();
}
