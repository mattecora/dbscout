package dbscout.structures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Cell implements Serializable {

    private static final long serialVersionUID = 1L;

    private int[] pos;

    public Cell() {}

    public Cell(int... pos) {
        this.pos = pos;
    }

    public int[] getPos() {
        return pos;
    }

    public void setPos(int[] pos) {
        this.pos = pos;
    }

    public double minSquaredDistanceTo(Cell other) {
        double sum = 0;

        for (int i = 0; i < pos.length; i++) {
            int axisDistance = Math.abs(this.pos[i] - other.pos[i]) - 1;
            sum += axisDistance <= 0 ? 0 : Math.pow(axisDistance, 2);
        }

        return sum / this.pos.length;
    }

    public List<Cell> generateNeighbors() {
        int delta = (int) Math.ceil(Math.sqrt(pos.length));
        List<Cell> neighbors = new ArrayList<>();

        generateNeighborsRec(0, delta, new int[pos.length], neighbors);
        return neighbors;
    }

    private void generateNeighborsRec(int x, int delta, int[] newPos, List<Cell> neighbors) {
        if (x == pos.length) {
            /* Create the new cell */
            Cell newCell = new Cell(Arrays.copyOf(newPos, newPos.length));

            /* Add the cell to the neighbors if its minimum distance is at most eps */
            if (minSquaredDistanceTo(newCell) < 1)
                neighbors.add(new Cell(Arrays.copyOf(newPos, newPos.length)));
            return;
        }

        /* Generate a dimension and go to the next */
        for (int i = this.pos[x] - delta; i <= this.pos[x] + delta; i++) {
            newPos[x] = i;
            generateNeighborsRec(x + 1, delta, newPos, neighbors);
        }
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