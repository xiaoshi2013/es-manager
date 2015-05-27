package com.zyuc.es.util;

import java.io.Serializable;


public class Tuple<V1, V2> implements Serializable  {

  
    private final V1 v1;
    private final V2 v2;

    
    public Tuple(){
    	this.v1=null;
    	this.v2=null;
    	
    }
    public Tuple(V1 v1, V2 v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public V1 v1() {
        return v1;
    }

    public V2 v2() {
        return v2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple tuple = (Tuple) o;

        if (v1 != null ? !v1.equals(tuple.v1) : tuple.v1 != null) return false;
        if (v2 != null ? !v2.equals(tuple.v2) : tuple.v2 != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = v1 != null ? v1.hashCode() : 0;
        result = 31 * result + (v2 != null ? v2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Tuple [v1=" + v1 + ", v2=" + v2 + "]";
    }
}
