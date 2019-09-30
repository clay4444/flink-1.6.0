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

package org.apache.flink.runtime.jobgraph;

/**
 * The ScheduleMode decides how tasks of an execution graph are started.
 * 定义 execution graph 的 task 如何开始，
 */
public enum ScheduleMode {

	/** Schedule tasks lazily from the sources. Downstream tasks are started once their input data are ready */
	// 从 source 开始 实行懒加载策略，上游的任务成功之后，下游的任务才启动
	LAZY_FROM_SOURCES,

	/** Schedules all tasks immediately. */
	//立即启动所有job
	EAGER;
	
	/**
	 * Returns whether we are allowed to deploy consumers lazily.
	 */
	public boolean allowLazyDeployment() {
		return this == LAZY_FROM_SOURCES;
	}
	
}
