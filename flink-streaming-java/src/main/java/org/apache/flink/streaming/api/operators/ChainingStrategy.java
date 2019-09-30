/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Defines the chaining scheme for the operator. When an operator is chained to the
 * predecessor, it means that they run in the same thread. They become one operator
 * consisting of multiple steps.
 *
 * <p>The default value used by the {@link StreamOperator} is {@link #HEAD}, which means that
 * the operator is not chained to its predecessor. Most operators override this with
 * {@link #ALWAYS}, meaning they will be chained to predecessors whenever possible.
 */
//chain 时的策略
@PublicEvolving
public enum ChainingStrategy {

	/**
	 * Operators will be eagerly chained whenever possible.
	 *
	 * <p>To optimize performance, it is generally a good practice to allow maximal
	 * chaining and increase operator parallelism.
	 * 尽可能chain到一起
	 */
	ALWAYS,

	/**
	 * The operator will not be chained to the preceding or succeeding operators.
	 * 永远不会chain到头结点或者后续结点
	 */
	NEVER,

	/**
	 * The operator will not be chained to the predecessor, but successors may chain to this
	 * operator.
	 * 当前operator 不会连接到前边的operator，但是可以作为后续operator的头结点
	 */
	HEAD
}
