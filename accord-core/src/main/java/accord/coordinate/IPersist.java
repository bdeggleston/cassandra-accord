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

package accord.coordinate;

import java.util.function.Consumer;
import javax.annotation.Nullable;

import accord.local.Node;
import accord.primitives.DataConsistencyLevel;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

// TODO: rename
// TODO: this, Persist looks a bit more complex in terms of interface extraction since there's persist and apply, and applyMinimal and applyMaximal
public interface IPersist
{
    interface Factory
    {
        IPersist create(Node node, Topologies topologies, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, @Nullable Consumer<Throwable> onAppliedToQuorum, DataConsistencyLevel writeDataCL);
    }
}
