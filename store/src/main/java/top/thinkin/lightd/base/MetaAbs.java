package top.thinkin.lightd.base;

public abstract class MetaAbs {
    protected int version;

    public abstract <T extends MetaDAbs> T convertMetaBytes();
}
