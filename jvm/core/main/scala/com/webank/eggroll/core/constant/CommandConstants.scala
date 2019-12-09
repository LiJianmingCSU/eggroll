/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.webank.eggroll.core.constant

import com.webank.eggroll.core.command.CommandURI
import com.webank.eggroll.core.util.CommandUtils

object MetadataCommands {
  val prefix = "v1/cluster-manager/metadata"

  val getServerNode = "getServerNode"
  val getServerNodeServiceName = CommandUtils.toServiceName(prefix, getServerNode)
  val GET_SERVER_NODE = new CommandURI(getServerNodeServiceName)

  val getServerNodes = "getServerNodes"
  val getServerNodesServiceName = CommandUtils.toServiceName(prefix, getServerNodes)
  val GET_SERVER_NODES = new CommandURI(getServerNodesServiceName)

  val getOrCreateServerNode = "getOrCreateServerNode"
  val getOrCreateServerNodeServiceName = CommandUtils.toServiceName(prefix, getOrCreateServerNode)
  val GET_OR_CREATE_SERVER_NODE = new CommandURI(getOrCreateServerNodeServiceName)

  val createOrUpdateServerNode = "createOrUpdateServerNode"
  val createOrUpdateServerNodeServiceName = CommandUtils.toServiceName(prefix, createOrUpdateServerNode)
  val CREATE_OR_UPDATE_SERVER_NODE = new CommandURI(createOrUpdateServerNodeServiceName)

  val getStore = "getStore"
  val getStoreServiceName = CommandUtils.toServiceName(prefix, getStore)
  val GET_STORE = new CommandURI(getStoreServiceName)

  val getOrCreateStore = "getOrCreateStore"
  val getOrCreateStoreServiceName = CommandUtils.toServiceName(prefix, getOrCreateStore)
  val GET_OR_CREATE_STORE = new CommandURI(getOrCreateStoreServiceName)

  val deleteStore = "deleteStore"
  val deleteStoreServiceName = CommandUtils.toServiceName(prefix, deleteStore)
  val DELETE_STORE = new CommandURI(deleteStoreServiceName)
}

object SessionCommands {
  val prefix = "v1/cluster-manager/session"

  val getOrCreateSession = new CommandURI(prefix = prefix, name = "getOrCreateSession")
  val getSession = new CommandURI(prefix = prefix, name = "getSession")
  val registerSession = new CommandURI(prefix = prefix, name = "registerSession")
  val getSessionServerNodes = new CommandURI(prefix = prefix, name = "getSessionServerNodes")
  val getSessionRolls = new CommandURI(prefix = prefix, name = "getSessionRolls")
  val getSessionEggs = new CommandURI(prefix = prefix, name = "getSessionEggs")

  val stopSession = "stopSession"
  val stopSessionServiceName = CommandUtils.toServiceName(prefix, stopSession)
  val STOP_SESSION = new CommandURI(stopSessionServiceName)
}

object NodeManagerCommands {
  val prefix = "v1/node-manager/processor"

  val getOrCreateEggs = "getOrCreateEggs"
  val getOrCreateEggsServiceName = CommandUtils.toServiceName(prefix, getOrCreateEggs)
  val GET_OR_CREATE_EGGS = new CommandURI(getOrCreateEggsServiceName)

  val getOrCreateRolls = "getOrCreateRolls"
  val getOrCreateRollsServiceName = CommandUtils.toServiceName(prefix, getOrCreateRolls)
  val GET_OR_CREATE_ROLLS = new CommandURI(getOrCreateRollsServiceName)

  val heartbeat = "heartbeat"
  val heartbeatServiceName = CommandUtils.toServiceName(prefix, heartbeat)
  val HEARTBEAT = new CommandURI(heartbeatServiceName)
}