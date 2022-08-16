package accord.messages;

import java.util.Set;

import com.google.common.base.Preconditions;

import java.util.Collections;

import accord.api.Key;
import accord.local.*;
import accord.local.Node.Id;
import accord.api.Data;
import accord.topology.Topologies;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.utils.DeterministicIdentitySet;

public class ReadData extends TxnRequest
{
    static class LocalRead implements Listener, TxnOperation
    {
        final TxnId txnId;
        final Node node;
        final Node.Id replyToNode;
        final Keys readKeys;
        final Keys txnKeys;
        final ReplyContext replyContext;

        Data data;
        boolean isObsolete; // TODO: respond with the Executed result we have stored?
        Set<CommandStore> waitingOn;

        LocalRead(TxnId txnId, Node node, Id replyToNode, Keys readKeys, Keys txnKeys, ReplyContext replyContext)
        {
            Preconditions.checkArgument(!readKeys.isEmpty());
            this.txnId = txnId;
            this.node = node;
            this.replyToNode = replyToNode;
            this.readKeys = readKeys;
            this.txnKeys = txnKeys;  // TODO (now): is this needed? Does the read update commands per key?
            this.replyContext = replyContext;
        }

        @Override
        public Iterable<TxnId> expectedTxnIds()
        {
            return Collections.singletonList(txnId);
        }

        @Override
        public Iterable<Key> expectedKeys()
        {
            return txnKeys;
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
                case Committed:
                    return;

                case Executed:
                case Applied:
                case Invalidated:
                    obsolete();
                case ReadyToExecute:
            }

            command.removeListener(this);
            if (!isObsolete)
                read(command);
        }

        @Override
        public boolean isTransient()
        {
            return true;
        }

        private synchronized void readComplete(CommandStore commandStore, Data result)
        {
            data = data == null ? result : data.merge(result);

            waitingOn.remove(commandStore);
            if (waitingOn.isEmpty())
                node.reply(replyToNode, replyContext, new ReadOk(data));
        }

        private void read(Command command)
        {
            // TODO: threading/futures (don't want to perform expensive reads within this mutually exclusive context)
            Data next = command.txn().read(command, readKeys);
            readComplete(command.commandStore(), next);
        }

        void obsolete()
        {
            if (!isObsolete)
            {
                isObsolete = true;
                node.reply(replyToNode, replyContext, new ReadNack());
            }
        }

        synchronized void setup(TxnId txnId, Txn txn, Key homeKey, Keys keys, Timestamp executeAt)
        {
            Key progressKey = node.trySelectProgressKey(txnId, txn.keys, homeKey);
            waitingOn = node.collectLocal(keys, executeAt, DeterministicIdentitySet::new);
            // FIXME: fix/check thread safety
            // FIXME (rebase): rework forEach and mapReduce to not require these to be public
            CommandStores.forEachNonBlocking(waitingOn, this, instance -> {
                Command command = instance.command(txnId);
                command.preaccept(txn, homeKey, progressKey); // ensure pre-accepted
                switch (command.status())
                {
                    default:
                    case NotWitnessed:
                        throw new IllegalStateException();

                    case PreAccepted:
                    case Accepted:
                    case AcceptedInvalidate:
                    case Committed:
                        command.addListener(this);
                        break;

                    case Executed:
                    case Applied:
                    case Invalidated:
                        obsolete();
                        break;

                    case ReadyToExecute:
                        if (!isObsolete)
                            read(command);
                }
            });
        }
    }

    public final TxnId txnId;
    public final Txn txn;
    final Key homeKey;
    public final Timestamp executeAt;

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt)
    {
        super(to, topologies, txn.keys);
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
    }

    public void process(Node node, Node.Id from, ReplyContext replyContext)
    {
        new LocalRead(txnId, node, from, txn.read.keys().intersect(scope()), txn.keys(), replyContext)
            .setup(txnId, txn, homeKey, scope(), executeAt);
    }

    @Override
    public Iterable<TxnId> expectedTxnIds()
    {
        return Collections.singletonList(txnId);
    }

    @Override
    public Iterable<Key> expectedKeys()
    {
        return txn.keys();
    }

    @Override
    public MessageType type()
    {
        return MessageType.READ_REQ;
    }

    public static class ReadReply implements Reply
    {
        @Override
        public MessageType type()
        {
            return MessageType.READ_RSP;
        }

        public boolean isOK()
        {
            return true;
        }
    }

    public static class ReadNack extends ReadReply
    {
        @Override
        public boolean isOK()
        {
            return false;
        }
    }

    public static class ReadOk extends ReadReply
    {
        public final Data data;
        public ReadOk(Data data)
        {
            this.data = data;
        }

        @Override
        public String toString()
        {
            return "ReadOk{" + data + '}';
        }
    }

    @Override
    public String toString()
    {
        return "ReadData{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               '}';
    }
}
