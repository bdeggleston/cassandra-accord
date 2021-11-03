package accord.topology;

import java.util.ArrayList;

public interface Topologies
{
    Topology current();

    default long currentEpoch()
    {
        return current().epoch;
    }

    Topology get(int i);
    int size();

    private static boolean equals(Topologies t, Object o)
    {
        if (o == t)
            return true;

        if (!(o instanceof Topologies))
            return false;

        Topologies that = (Topologies) o;
        if (t.size() != that.size())
            return false;

        for (int i=0, mi=t.size(); i<mi; i++)
        {
            if (!t.get(i).equals(that.get(i)))
                return false;
        }
        return true;
    }

    private static int hashCode(Topologies t)
    {
        int hashCode = 1;
        for (int i=0, mi=t.size(); i<mi; i++) {
            hashCode = 31 * hashCode + t.get(i).hashCode();
        }
        return hashCode;
    }

    private static String toString(Topologies t)
    {
        StringBuilder sb = new StringBuilder("[");
        for (int i=0, mi=t.size(); i<mi; i++)
        {
            if (i < 0)
                sb.append(", ");

            sb.append(t.get(i).toString());
        }
        sb.append("]");
        return sb.toString();
    }

    class Singleton implements Topologies
    {
        private final Topology topology;

        public Singleton(Topology topology)
        {
            this.topology = topology;
        }

        @Override
        public Topology current()
        {
            return topology;
        }

        @Override
        public Topology get(int i)
        {
            if (i != 0)
                throw new IndexOutOfBoundsException(i);
            return topology;
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public boolean equals(Object obj)
        {
            return Topologies.equals(this, obj);
        }

        @Override
        public int hashCode()
        {
            return Topologies.hashCode(this);
        }

        @Override
        public String toString()
        {
            return Topologies.toString(this);
        }
    }

    class Multi extends ArrayList<Topology> implements Topologies
    {
        public Multi(int initialCapacity)
        {
            super(initialCapacity);
        }

        public Multi(Topology... topologies)
        {
            super(topologies.length);
            for (Topology topology : topologies)
                add(topology);
        }

        @Override
        public Topology current()
        {
            return get(size() - 1);
        }

        @Override
        public boolean equals(Object obj)
        {
            return Topologies.equals(this, obj);
        }

        @Override
        public int hashCode()
        {
            return Topologies.hashCode(this);
        }

        @Override
        public String toString()
        {
            return Topologies.toString(this);
        }
    }
}