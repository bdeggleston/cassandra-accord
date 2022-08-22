package accord.messages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import accord.api.Key;
import accord.local.*;
import accord.local.Node.Id;
import accord.topology.Topologies;
import accord.primitives.TxnId;
import accord.primitives.Keys;
import accord.utils.ProvidedForImplementation;

public class WaitOnCommit extends TxnRequest
{
    static class LocalWait implements Listener, TxnOperation
    {
        final Node node;
        final Id replyToNode;
        final TxnId txnId;
        final ReplyContext replyContext;

        final AtomicInteger waitingOn = new AtomicInteger();

        LocalWait(Node node, Id replyToNode, TxnId txnId, ReplyContext replyContext)
        {
            this.node = node;
            this.replyToNode = replyToNode;
            this.txnId = txnId;
            this.replyContext = replyContext;
        }

        @Override
        public Iterable<TxnId> txnIds()
        {
            return Collections.singleton(txnId);
        }

        @Override
        public Iterable<Key> keys()
        {
            return Collections.emptyList();
        }

        @Override
        public synchronized void onChange(Command command)
        {
            switch (command.status())
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                    return;

                case Committed:
                case Executed:
                case Applied:
                case Invalidated:
                case ReadyToExecute:
            }

            command.removeListener(this);
            ack();
        }

        @Override
        public boolean isTransient()
        {
            return true;
        }

        private void ack()
        {
            if (waitingOn.decrementAndGet() == 0)
                node.reply(replyToNode, replyContext, WaitOnCommitOk.INSTANCE);
        }

        void setup(Keys keys, CommandStore instance)
        {
            Command command = instance.command(txnId);
            switch (command.status())
            {
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                    command.addListener(this);
                    instance.progressLog().waiting(txnId, keys);
                    break;

                case Committed:
                case Executed:
                case Applied:
                case Invalidated:
                case ReadyToExecute:
                    ack();
            }
        }

        synchronized void setup(Keys keys)
        {
            List<CommandStore> instances = node.collectLocal(keys, txnId, ArrayList::new);
            waitingOn.set(instances.size());
            CommandStore.onEach(this, instances, instance -> setup(keys, instance));
        }
    }

    public final TxnId txnId;

    public WaitOnCommit(Id to, Topologies topologies, TxnId txnId, Keys keys)
    {
        super(to, topologies, keys);
        this.txnId = txnId;
    }

    @ProvidedForImplementation
    public WaitOnCommit(Keys scope, long waitForEpoch, TxnId txnId)
    {
        super(scope, waitForEpoch);
        this.txnId = txnId;
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Iterable<Key> keys()
    {
        return scope();
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        new LocalWait(node, replyToNode, txnId, replyContext).setup(scope());
    }

    @Override
    public MessageType type()
    {
        return MessageType.WAIT_ON_COMMIT_REQ;
    }

    public static class WaitOnCommitOk implements Reply
    {
        public static final WaitOnCommitOk INSTANCE = new WaitOnCommitOk();

        private WaitOnCommitOk() {}

        @Override
        public MessageType type()
        {
            return MessageType.WAIT_ON_COMMIT_RSP;
        }
    }
}
