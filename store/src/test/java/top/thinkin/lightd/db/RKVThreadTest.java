package top.thinkin.lightd.db;

import lombok.extern.log4j.Log4j2;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.benchmark.JoinFuture;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Log4j2
public class RKVThreadTest {
    static ExecutorService executorService = Executors.newFixedThreadPool(50);

    public static void main(String[] args) throws RocksDBException {
        RKvTest rKvTest = new RKvTest();
        rKvTest.init();
        log.info("start1");

        int i = 10000;
        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        joinFuture.add(a -> {
            try {
                for (int i1 = 0; i1 < i; i1++) {
                    rKvTest.del();
                }
            } catch (Exception e) {
                log.error("del", e);
            }

            return "";
        });

        joinFuture.add(a -> {
            log.info("delPrefix");
            try {
                for (int i1 = 0; i1 < i; i1++) {
                    rKvTest.delPrefix();
                }
            } catch (Exception e) {
                log.error("delPrefix", e);
            }

            return "";
        });


        joinFuture.add(a -> {
            try {
                for (int i1 = 0; i1 < i; i1++) {
                    rKvTest.delTtl();
                }
            } catch (Exception e) {
                log.error("v", e);
            }

            return "";
        });


        joinFuture.add(a -> {
            try {
                for (int i1 = 0; i1 < i; i1++) {
                    rKvTest.get();
                }
            } catch (Exception e) {
                log.error("v", e);
            }

            return "";
        });


        joinFuture.add(a -> {
            try {
                for (int i1 = 0; i1 < i; i1++) {
                    rKvTest.get1();
                }
            } catch (Exception e) {
                log.error("v", e);
            }

            return "";
        });


        joinFuture.add(a -> {
            try {
                for (int i1 = 0; i1 < i; i1++) {
                    rKvTest.getNoTTL();
                }
            } catch (Exception e) {
                log.error("v", e);
            }

            return "";
        });


        joinFuture.add(a -> {
            try {
                for (int i1 = 0; i1 < i; i1++) {
                    rKvTest.getTtl();
                }
            } catch (Exception e) {
                log.error("v", e);
            }

            return "";
        });

        joinFuture.add(a -> {
            try {
                for (int i1 = 0; i1 < i; i1++) {
                    rKvTest.incr();
                }
            } catch (Exception e) {
                log.error("v", e);
            }

            return "";
        });

        joinFuture.add(a -> {
            try {
                for (int i1 = 0; i1 < i; i1++) {
                    rKvTest.keys();
                }
            } catch (Exception e) {
                log.error("v", e);
            }

            return "";
        });


        joinFuture.add(a -> {
            try {
                for (int i1 = 0; i1 < i; i1++) {
                    rKvTest.set2();
                }
            } catch (Exception e) {
                log.error("v", e);
            }

            return "";
        });
        log.info("start");
        joinFuture.join();
        log.info("end");
        executorService.shutdown();
    }
}
