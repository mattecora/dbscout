package it.polito.s256654.thesis;

import java.io.Serializable;
import java.util.Arrays;

public class Vector implements Serializable {

    private static final long serialVersionUID = 1L;

    private long id;
    private double[] feats;

    public Vector() {}

    public Vector(long id, double ...feats) {
        this.id = id;
        this.feats = feats;
    }

    public double[] getFeats() {
        return feats;
    }

    public void setFeats(double[] feats) {
        this.feats = feats;
    }

    public double distanceTo(Vector other) {
        double sum = 0;

        for (int i = 0; i < feats.length; i++)
            sum += Math.pow(this.feats[i] - other.feats[i], 2);

        return Math.sqrt(sum);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(feats);
        result = prime * result + (int) (id ^ (id >>> 32));
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
        if (id != other.id)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return Arrays.toString(feats);
    }

}