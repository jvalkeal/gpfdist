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
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * Gpfdist related function.
 *
 * @author Janne Valkealahti
 */
public class GpfdistCodec implements Function<ByteBuf, ByteBuf> {

	final byte[] h1 = Character.toString('D').getBytes(Charset.forName("UTF-8"));

	final ByteBufAllocator alloc;

	public GpfdistCodec(ByteBufAllocator alloc) {
		this.alloc = alloc;
	}

	@Override
	public ByteBuf apply(ByteBuf t) {
		int size = t.capacity();
		byte[] h2 = ByteBuffer.allocate(4)
							.putInt(size)
							.array();
		return alloc.buffer()
					.writeBytes(h1)
					.writeBytes(h2)
					.writeBytes(t);
	}

}