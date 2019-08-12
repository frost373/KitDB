package top.thinkin.lightd.collect;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
@Data
public class AtomHandler {
    private int type;
    private int count;
    private List<DBLog> logs =  new ArrayList<>();

    public AtomHandler(int type, int count) {
        this.type = type;
        this.count = count;
    }


    public int addCount(){
        return count++;
    }
    public int subCount(){
        return count--;
    }
}
