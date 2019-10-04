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

package org.apache.flink.runtime.executiongraph;

/**
 * ExecutionEdge 表示 ExecutionVertex 的输入，通过 ExecutionEdge 将 ExecutionVertex 和 IntermediateResultPartition 连接起来，进而在不同的 ExecutionVertex 之间建立联系。
 */
public class ExecutionEdge {

	private final IntermediateResultPartition source;  //ExecutionEdge 上游承接的是IntermediateResultPartition

	private final ExecutionVertex target;    //下游连接的是 ExecutionVertex (具体执行的一个subtask)

	private final int inputNum;

	public ExecutionEdge(IntermediateResultPartition source, ExecutionVertex target, int inputNum) {
		this.source = source;
		this.target = target;
		this.inputNum = inputNum;
	}

	public IntermediateResultPartition getSource() {
		return source;
	}

	public ExecutionVertex getTarget() {
		return target;
	}

	public int getInputNum() {
		return inputNum;
	}

	@Override
	public String toString() {
		return "ExecutionEdge [" + source + " <=> " + target + "]";
	}
}
