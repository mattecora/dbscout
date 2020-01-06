package it.polito.s256654.thesis;

import java.io.Serializable;

public class Cell implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private int xPos, yPos;

    public Cell(int xPos, int yPos) {
        this.xPos = xPos;
        this.yPos = yPos;
    }

    public int getxPos() {
        return xPos;
    }

    public int getyPos() {
        return yPos;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + xPos;
        result = prime * result + yPos;
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
        if (xPos != other.xPos)
            return false;
        if (yPos != other.yPos)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "(" + xPos + "," + yPos + ")";
    }

}