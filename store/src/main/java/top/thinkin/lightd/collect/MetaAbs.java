package top.thinkin.lightd.collect;

public abstract class MetaAbs {
    protected int version;

    public abstract <T extends MetaDAbs> T convertMetaBytes();
}
