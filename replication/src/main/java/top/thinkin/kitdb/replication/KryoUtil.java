package top.thinkin.kitdb.replication;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;
import top.thinkin.lightd.base.DBCommand;
import top.thinkin.lightd.base.DBCommandChunk;
import top.thinkin.lightd.base.DBCommandChunkType;
import top.thinkin.lightd.base.SstColumnFamily;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public class KryoUtil {
    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final ThreadLocal<Kryo> kryoLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(
                new StdInstantiatorStrategy()));
        kryo.setReferences(false);
        kryo.register(DBCommandChunk.class);
        return kryo;
    });


    public static Kryo getInstance() {
        return kryoLocal.get();
    }

    public static byte[] writeToByteArray(Object obj) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Output output = new Output(byteArrayOutputStream);

        Kryo kryo = getInstance();
        kryo.writeClassAndObject(output, obj);
        output.flush();

        return byteArrayOutputStream.toByteArray();
    }


    public static void main(String[] args) {
        List<DBCommand> commands = new ArrayList<>();
        commands.add(DBCommand.update("hello".getBytes(), "word".getBytes(), SstColumnFamily.DEFAULT));
        DBCommandChunk dbCommandChunk = new DBCommandChunk(DBCommandChunkType.NOM_COMMIT, commands);
        byte[] bytes = writeToByteArray(dbCommandChunk);
        DBCommandChunk group2 = readFromBytes(bytes, DBCommandChunk.class);
        System.out.println(group2.getCommands().size());
    }


    public static <T> T readFromBytes(byte[] bytes, Class<T> clazz) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        Input input = new Input(byteArrayInputStream);
        Kryo kryo = getInstance();
        return kryo.readObjectOrNull(input, clazz);
    }


}
