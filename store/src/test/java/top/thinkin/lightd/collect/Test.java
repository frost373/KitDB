/*
package top.thinkin.lightd.collect;

import org.rocksdb.RocksDB;
import top.thinkin.lightd.db.DB;
import top.thinkin.lightd.db.RList;

public class Test {
    static RocksDB rocksDB;

    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();
        DB db =  DB.build("D:\\temp\\db");

        long startTime = System.currentTimeMillis(); //获取开始时间

        RList list =   db.getList("testList6");


      for (int i = 0; i < 100000; i++) {
            list.add((i+"test").getBytes());
        }

        System.out.println(list.size());

*/
/*
        RIterator<RList> iterator =  list.iterator();

        while (iterator.hasNext()){
            RList.Entry entry = (RList.Entry) iterator.next();
            System.out.println(entry.index +":"+new String(entry.value));
        }

        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms"); //输出程序运行时间*//*


        list.delete();

    }
}
*/
