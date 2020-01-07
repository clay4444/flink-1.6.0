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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.windows;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A {@code Window} is a grouping of elements into finite buckets. Windows have a maximum timestamp
 * which means that, at some point, all elements that go into one window will have arrived.
 *
 * <p>Subclasses should implement {@code equals()} and {@code hashCode()} so that logically
 * same windows are treated the same.
 *
 * 窗口在 Flink 内部就是使用抽象类 Window 来表示，每一个窗口都有一个绑定的最大 timestamp，一旦时间超过这个值表明窗口结束了。
 * Window 有两个具体实现类，分别为 TimeWindow 和 GlobalWindow：
 * TimeWindow 就是时间窗口，每一个时间窗口都有开始时间和结束时间，可以对时间窗口进行合并操作（主要是在 Session Window 中）；
 * GlobalWindow 是一个全局窗口，所有数据都属于该窗口，其最大 timestamp 是 Long.MAX_VALUE，使用单例模式。
 */
@PublicEvolving
public abstract class Window {

	/**
	 * Gets the largest timestamp that still belongs to this window.
	 *
	 * @return The largest timestamp that still belongs to this window.
	 */
	public abstract long maxTimestamp();
}
