package accord.impl.mock;

import accord.api.Data;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Store;
import accord.api.Update;
import accord.api.Write;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

public class MockStore implements Store
{
    public static final Data DATA = new Data() {
        @Override
        public Data merge(Data data)
        {
            return DATA;
        }
    };

    public static final Result RESULT = new Result() {};
    public static final Read READ = (key, commandStore, executeAt, store) -> ImmediateFuture.success(DATA);
    public static final Query QUERY = data -> RESULT;
    public static final Write WRITE = (key, commandStore, executeAt, store) -> ImmediateFuture.success(null);
    public static final Update UPDATE = data -> WRITE;
}
