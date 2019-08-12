package top.thinkin.lightd.benchmark;

import java.util.*;
import java.util.stream.Collectors;


public class FList<E> extends ArrayList<E> {

    public FList(Collection<? extends E> c) {
         super(c);
    }

    public FList(int size) {
        super(size);
    }

    public interface  FunctionKey<R, E> {
        R call(E arg);
    }


    public interface  FunctionSort<R  extends Comparable<? super R>,E> {
        R call(E arg);
    }


    public static <E> FList<E> build(Collection<? extends E> c){
        return new FList(c);
    }

    public  <R> Map<R, E> toMap( FunctionKey<R,E> functionKey){
        return  this.stream().collect(Collectors.toMap(functionKey::call, n -> n));
    }

    public  <R  extends Comparable<? super R>> List<E> fSort( FunctionSort<R,E> functionSort,boolean desc){
        if(desc){
            return  this.stream().sorted(Comparator.comparing(functionSort::call).reversed()).collect(Collectors.toList());
        }
        return  this.stream().sorted(Comparator.comparing(functionSort::call)).collect(Collectors.toList());
    }



    public  List<List<E>> split( int size) {
        final List<List<E>> result = new ArrayList<>();
        FList<E> subList = new FList<>(size);
        for (E t : this) {
            if (subList.size() >= size) {
                result.add(subList);
                subList = new FList<>(size);
            }
            subList.add(t);
        }
        result.add(subList);
        return result;
    }




}
