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

package accord.impl;

import accord.local.Command;
import accord.local.ContextValue;
import accord.primitives.RoutableKey;
import accord.primitives.TxnId;
import com.google.common.collect.ImmutableMap;

public class PostExecuteContext
{
    public final ImmutableMap<TxnId, ContextValue<Command>> commands;
    public final ImmutableMap<RoutableKey, ContextValue<CommandsForKey>> commandsForKey;

    public PostExecuteContext(ImmutableMap<TxnId, ContextValue<Command>> commands, ImmutableMap<RoutableKey, ContextValue<CommandsForKey>> commandsForKey)
    {
        this.commands = commands;
        this.commandsForKey = commandsForKey;
    }
}