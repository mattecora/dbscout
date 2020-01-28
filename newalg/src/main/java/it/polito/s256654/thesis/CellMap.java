package it.polito.s256654.thesis;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CellMap implements Serializable {

    public enum CellType { EMPTY, DENSE, NON_DENSE };

    private static final long serialVersionUID = 1L;
    
    private Map<Cell, CellType> cells = new HashMap<>();

    public CellType getCellType(Cell cell) {
        return cells.containsKey(cell) ? cells.get(cell) : CellType.EMPTY;
    }

    public void putCell(Cell cell, CellType cellType) {
        cells.put(cell, cellType);
    }

    public int getTotalCellsNum() {
        return cells.keySet().size();
    }

    public int getDenseCellsNum() {
        int n = 0;

        for (Map.Entry<Cell, CellType> e : cells.entrySet()) {
            if (e.getValue() == CellType.DENSE)
                n++;
        }

        return n;
    }

}