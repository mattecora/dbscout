package it.polito.s256654.thesis;

import java.io.Serializable;
import java.util.Arrays;

public class Cell implements Serializable {

    private static final long serialVersionUID = 1L;

    private int[] pos;

    public Cell(int... pos) {
        this.pos = pos;
    }

    public int getPos(int dim) {
        return pos[dim];
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(pos);
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
        Cell other = (Cell) obj;
        if (!Arrays.equals(pos, other.pos))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return Arrays.toString(pos);
    }

}