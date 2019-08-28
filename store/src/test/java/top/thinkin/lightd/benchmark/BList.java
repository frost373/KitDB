/*
package top.thinkin.lightd.benchmark;

import cn.hutool.core.collection.CollectionUtil;
import org.rocksdb.RocksDB;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.db.RIterator;
import top.thinkin.lightd.db.RList;

import java.util.ArrayList;
import java.util.List;

public class BList {

    public static void main(String[] args) throws Exception {

        RocksDB.loadLibrary();
        DB db =  DB.build("D:\\temp\\db");
        RList list =   db.getList("benchmarkList");


        int k = 100 * 10000;
        List<byte[]> arrayList = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            arrayList.add((i+"test").getBytes());
        }

        addAll(list, arrayList);

        System.out.println(new String(list.get(0)));
        try {
            long startTime = System.currentTimeMillis(); //获取开始时间

            blpop(list);
            //range(list);
            //iterator(list);
            //deleteDB(list);
            //get(list);
            long endTime = System.currentTimeMillis(); //获取结束时间
            System.out.println("程序运行时间：" + (endTime - startTime) + "ms"); //输出程序运行时间
            System.out.println("benchmark" + ((100.00 * 10000) / (endTime - startTime)) * 1000 + "per second"); //输出程序运行时间

        } finally {
            list.delete();
            db.close();
        }


    }

    private static void delete(RList list) throws Exception {
        list.delete();
    }


    private static void addAll(RList list,List<byte[]> arrayList) throws Exception {
        long startTime = System.currentTimeMillis(); //获取开始时间
        FList<byte[]> fList = new FList(arrayList);
        List<List<byte[]>> lists =  fList.split(10*10000);
        for (List<byte[]> bytes:lists){
            list.addAll(bytes);
        }
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms"); //输出程序运行时间
    }

    private static void range(RList list) throws Exception {
        for(int i = 0; i < 1000; i++){
            List<byte[]> list2 =   list.range(i*1000,(i*1000)+1000);

        }
    }

    private static void get(RList list) throws Exception {
        for (int i = 0; i < 100 * 10000; i++) {
            list.get(i);
        }
    }


    private static void getAll(RList list) throws Exception {
        long[] ints = new long[10001];
        int j = 0;
        for (int i = 0; i < 100 * 10000; i++) {
            ints[j] = i;
            if (i % 10000 == 0) {
                list.get(ints);
                j = 0;
            }
            j++;
        }
    }


    private static void iterator(RList list) throws Exception {
        RIterator<RList> iterator =  list.iterator();
        while (iterator.hasNext()){
            RList.Entry entry = (RList.Entry) iterator.next();
        }
    }


    private static void blpop(RList list) throws Exception {
        while (true){
            java.util.List<byte[]> pops = list.blpop(1);
            if(CollectionUtil.isEmpty(pops)) break;
        }
    }

}
*/
