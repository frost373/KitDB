package top.thinkin.lightd.benchmark;

import cn.hutool.core.collection.CollectionUtil;
import org.rocksdb.RocksDB;
import top.thinkin.lightd.collect.DB;
import top.thinkin.lightd.collect.RIterator;
import top.thinkin.lightd.collect.RList;

import java.util.ArrayList;
import java.util.List;

public class BList {

    public static void main(String[] args) throws Exception {
        int k = 100;
        java.util.List<byte[]> arrayList = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            arrayList.add((i+"test").getBytes());
        }


        RocksDB.loadLibrary();
        DB db =  DB.build("D:\\temp\\db");
        RList list =   db.getList("benchmarkList");
        list.addAll(arrayList);


        try {
            long startTime = System.currentTimeMillis(); //获取开始时间

            //blpop(list);
            //range(list);
            //iterator(list);
            long endTime = System.currentTimeMillis(); //获取结束时间
            System.out.println("程序运行时间：" + (endTime - startTime) + "ms"); //输出程序运行时间
        } finally {
            list.delete();
            db.close();
        }

    }



    private static void range(RList list) throws Exception {
        for(int i = 0; i < 1000; i++){
            List<byte[]> list2 =   list.range(i*1000,(i*1000)+1000);

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
            java.util.List<byte[]> pops = list.blpop(1000);
            if(CollectionUtil.isEmpty(pops)) break;
        }
    }

}
