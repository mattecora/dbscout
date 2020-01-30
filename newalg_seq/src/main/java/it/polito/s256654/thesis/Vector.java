package it.polito.s256654.thesis;

import java.io.Serializable;
import java.util.Arrays;

public class Vector implements Serializable {

    private static final long serialVersionUID = 1L;

    private double[] feats;

    public Vector() {}

    public Vector(double ...feats) {
        this.feats = feats;
    }

    public double[] getFeats() {
        return feats;
    }

    public void setFeats(double[] feats) {
        this.feats = feats;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(feats);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Vector other = (Vector) obj;
        if (!Arrays.equals(feats, other.feats))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return Arrays.toString(feats);
    }

}