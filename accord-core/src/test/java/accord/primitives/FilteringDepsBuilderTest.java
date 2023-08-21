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

package accord.primitives;

import accord.local.Node;
import accord.local.Status;
import accord.primitives.FilteringDepsBuilder.Builder;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class FilteringDepsBuilderTest
{
    private static final Node.Id NODE = new Node.Id(0);

    private static TxnId txnId(int v)
    {
        return TxnId.fromValues(1, v, 0, 0);
    }

    private static List<TxnId> txnIds(int... vs)
    {
        ImmutableList.Builder<TxnId> builder = ImmutableList.builder();
        Arrays.sort(vs);
        for (int v : vs)
            builder.add(txnId(v));
        return builder.build();
    }

    private static Timestamp timestamp(int v)
    {
        return Timestamp.fromValues(1, v, NODE);
    }

    private static List<TxnId> build(Builder builder)
    {
        List<TxnId> result = new ArrayList<>();
        builder.build(result::add);
        result.sort(Comparator.naturalOrder());
        return result;
    }

    @Test
    void basicTest()
    {
        Builder builder = new Builder();
        builder.add(txnId(0), Status.Committed, timestamp(0), txnIds());
        builder.add(txnId(1), Status.Committed, timestamp(1), txnIds(0));
        Assertions.assertEquals(txnIds(1), build(builder));
    }

    /**
     * Committed deps should be included if they point to an uncommitted command
     */
    @Test
    void uncommittedTest()
    {
        Builder builder = new Builder();
        builder.add(txnId(0), Status.Accepted, timestamp(0), txnIds());
        builder.add(txnId(1), Status.Committed, timestamp(1), txnIds(0));
        builder.add(txnId(2), Status.Committed, timestamp(2), txnIds(1));
        builder.add(txnId(3), Status.Committed, timestamp(3), txnIds(2));
        Assertions.assertEquals(txnIds(0, 1, 2, 3), build(builder));
    }

    /**
     * Committed txns should be included if they point to a command with a later execution time
     */
    @Test
    void executionOrderTest()
    {
        Builder builder = new Builder();
        builder.add(txnId(0), Status.Committed, timestamp(2), txnIds());
        builder.add(txnId(1), Status.Committed, timestamp(1), txnIds(0));
        Assertions.assertEquals(txnIds(0, 1), build(builder));
    }
}
