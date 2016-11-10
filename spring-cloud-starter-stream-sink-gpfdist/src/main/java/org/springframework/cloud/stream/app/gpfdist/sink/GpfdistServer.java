/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.gpfdist.sink;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.function.Function;

import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Processor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.WorkQueueProcessor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.ipc.netty.NettyState;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.options.ServerOptions;

/**
 * Server implementation around reactor and netty providing endpoint
 * where data can be sent using a gpfdist protocol.
 *
 * @author Janne Valkealahti
 */
public class GpfdistServer {

	private final static Log log = LogFactory.getLog(GpfdistServer.class);

	private final Processor<ByteBuf, ByteBuf> processor;
	private WorkQueueProcessor<ByteBuf> workProcessor;
	private final int port;
	private final int flushCount;
	private final int flushTime;
	private final int batchTimeout;
	private final int batchCount;
	private NettyState server;
	private int localPort = -1;

	/**
	 * Instantiates a new gpfdist server.
	 *
	 * @param processor the processor
	 * @param port the port
	 * @param flushCount the flush count
	 * @param flushTime the flush time
	 * @param batchTimeout the batch timeout
	 * @param batchCount the batch count
	 */
	public GpfdistServer(Processor<ByteBuf, ByteBuf> processor, int port, int flushCount, int flushTime,
			int batchTimeout, int batchCount) {
		this.processor = processor;
		this.port = port;
		this.flushCount = flushCount;
		this.flushTime = flushTime;
		this.batchTimeout = batchTimeout;
		this.batchCount = batchCount;
	}

	/**
	 * Start a server.
	 *
	 * @return the http server
	 * @throws Exception the exception
	 */
	public synchronized NettyState start() throws Exception {
		if (server == null) {
			server = createProtocolListener();
		}
		return server;
	}

	/**
	 * Stop a server.
	 *
	 * @throws Exception the exception
	 */
	public synchronized void stop() throws Exception {
		if (workProcessor != null) {
			workProcessor.onComplete();
		}
		if (server != null) {
			server.dispose();
		}
		workProcessor = null;
		server = null;
	}

	/**
	 * Gets the local port.
	 *
	 * @return the local port
	 */
	public int getLocalPort() {
		return localPort;
	}


	private NettyState createProtocolListener()
			throws Exception {
		workProcessor = WorkQueueProcessor.create("gpfdist-sink-worker", 8192, false);
		Flux<ByteBuf> stream = Flux.from(processor)
				.window(flushCount, Duration.ofSeconds(flushTime))
				.flatMap(s -> s.reduceWith(Unpooled::buffer, ByteBuf::writeBytes))
				.subscribeWith(workProcessor);


		NettyState httpServer = HttpServer
				.create(ServerOptions.on("0.0.0.0", port)
						.eventLoopGroup(new NioEventLoopGroup(10)))
				.newRouter(r -> r.get("/data", (request, response) -> {
					response.chunkedTransfer(false);
					return response
							.addHeader("Content-type", "text/plain")
							.addHeader("Expires", "0")
							.addHeader("X-GPFDIST-VERSION", "Spring Dataflow")
							.addHeader("X-GP-PROTO", "1")
							.addHeader("Cache-Control", "no-cache")
							.addHeader("Connection", "close")
							.send(stream
									.take(batchCount)
									.timeout(Duration.ofSeconds(batchTimeout), Flux.<ByteBuf> empty())
									.concatWith(Flux.just(Unpooled.copiedBuffer(new byte[0])))
									.map(new GpfdistCodecFunction(request.channel().alloc()))
								.doAfterTerminate(()->{response.channel().close();}));
				})).block();

		log.info("Server running using address=[" + httpServer.address() + "]");
		localPort = httpServer.address().getPort();
		return httpServer;
	}


	private static class GpfdistCodecFunction implements Function<ByteBuf, ByteBuf> {

		final byte[] h1 = Character.toString('D').getBytes(Charset.forName("UTF-8"));
		final ByteBufAllocator alloc;

		public GpfdistCodecFunction(ByteBufAllocator alloc) {
			this.alloc = alloc;
		}

		@Override
		public ByteBuf apply(ByteBuf t) {
			return alloc
					.buffer()
					.writeBytes(h1)
					.writeBytes(ByteBuffer
							.allocate(4)
							.putInt(t.readableBytes()).array())
					.writeBytes(t);
		}
	}
}
