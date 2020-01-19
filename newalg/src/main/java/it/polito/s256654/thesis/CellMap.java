package it.polito.s256654.thesis;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class CellMap implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private Set<Cell> cells = new HashSet<>();

    public boolean containsCell(Cell cell) {
        return cells.contains(cell);
    }

    public void putCell(Cell cell) {
        cells.add(cell);
    }

}