package it.polito.s256654.thesis;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CellMap implements Serializable {

    public enum CellType { EMPTY, DENSE, NON_DENSE, CORE, NON_CORE };

    private static final long serialVersionUID = 1L;
    
    private Map<Cell, CellType> cells = new HashMap<>();

    public CellType getCellType(Cell cell) {
        return cells.containsKey(cell) ? cells.get(cell) : CellType.EMPTY;
    }

    public void putCell(Cell cell, CellType cellType) {
        cells.put(cell, cellType);
    }

}