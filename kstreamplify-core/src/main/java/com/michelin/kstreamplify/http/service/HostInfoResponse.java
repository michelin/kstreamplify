package com.michelin.kstreamplify.http.service;

/**
 * Host info response.
 *
 * @param host The host
 * @param port The port
 */
public record HostInfoResponse(String host, Integer port) {
}
