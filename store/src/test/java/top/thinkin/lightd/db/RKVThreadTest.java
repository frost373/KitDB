package top.thinkin.lightd.db;

import lombok.extern.log4j.Log4j2;
import org.rocksdb.RocksDBException;
import top.thinkin.lightd.benchmark.JoinFuture;
import top.thinkin.lightd.exception.KitDBException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Log4j2
public class RKVThreadTest {
    static ExecutorService executorService = Executors.newFixedThreadPool(50);

    public static void main(String[] args) throws RocksDBException, KitDBException, InterruptedException {
        RKvTest rKvTest = new RKvTest();
        rKvTest.init();
        log.info("start1");

        int i = 10;
        JoinFuture<String> joinFuture = JoinFuture.build(executorService, String.class);
        joinFuture.add(a -> {

            try {
                for (int i1 = 0; i1 < i; i1++) {
                    log.info("del");

                    rKvTest.del();
                }
            } catch (Exception e) {
                log.error("del", e);
            }

            return "";
        });

        /*joinFuture.add(a -> {
            try {
                for (int i1 = 0; i1 < i; i1++) {
                    log.info("delPrefix");

                    rKvTest.delPrefix();
                }
            } catch (Exception e) {
                log.error("delPrefix", e);
            }

            return "";
        });*/


        joinFuture.add(a -> {

            try {
                for (int i1 = 0; i1 < i; i1++) {
                    log.info("delTtl");

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
                    log.info("get");

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
                    log.info("get1");

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
                    log.info("getNoTTL");

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
                    log.info("getTtl");

                    //rKvTest.getTtl();
                }
            } catch (Exception e) {
                log.error("v", e);
            }

            return "";
        });

        joinFuture.add(a -> {
            try {
                for (int i1 = 0; i1 < i; i1++) {
                    log.info("incr");

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
                    log.info("keys");

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
                    log.info("set2");

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
