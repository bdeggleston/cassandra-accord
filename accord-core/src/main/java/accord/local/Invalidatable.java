package accord.local;

public class Invalidatable
{
    private boolean invalidated = false;

    protected String name()
    {
        return getClass().getSimpleName();
    }

    public void checkNotInvalidated()
    {
        if (invalidated)
            throw new IllegalStateException("Cannot access invalidated " + name());
    }

    public void checkInvalidated()
    {
        if (!invalidated)
            throw new IllegalStateException("Expected invalidated " + name());
    }

    public void invalidate()
    {
        invalidated = true;
    }
}
