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

package org.apache.flink.runtime.minicluster;

/**
 * Enum which defines whether the mini cluster components use a shared RpcService
 * or whether every component gets its own dedicated RpcService started.
 */

/**
 * 定义mini cluster 组件是否使用一个共享的 RpcService 作为全局的RpcService，还是每个组件(jm,tm,rm)都有自己专用的 rpcService服务
 */
public enum RpcServiceSharing {
	SHARED, // a single shared rpc service
	DEDICATED // every component gets his own dedicated rpc service
}
