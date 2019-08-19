package top.thinkin.lightd.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class JoinFuture<T> {

    List<CompletableFuture<T>> futures = new ArrayList<>();

    Executor executor;

    public interface Function<T> {
        T call(Object... args);
    }

    /**
     * @param executor 线程池
     * @param clazz    返回类型
     * @param <T>
     * @return
     */
    public static <T> JoinFuture<T> build(Executor executor, Class<T> clazz) {
        JoinFuture joinFuture = new JoinFuture();
        joinFuture.executor = executor;
        return joinFuture;
    }

    /**
     * @param function 执行代码
     * @param args     参数
     */
    public void add(Function<T> function, Object... args) {
        CompletableFuture<T> t = CompletableFuture.supplyAsync(() ->
                function.call(args), executor);
        futures.add(t);
    }

    public List<T> join() {
        return futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
    }
}