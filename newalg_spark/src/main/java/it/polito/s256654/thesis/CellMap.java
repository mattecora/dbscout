package it.polito.s256654.thesis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class CellMap implements Serializable {

    public enum CellType { EMPTY, DENSE, CORE, OTHER };

    private static final long serialVersionUID = 1L;
    
    private Map<Cell, CellType> cells = new HashMap<>();

    public CellType getCellType(Cell cell) {
        return cells.containsKey(cell) ? cells.get(cell) : CellType.EMPTY;
    }

    public CellMap putCell(Cell cell, CellType cellType) {
        cells.put(cell, cellType);
        return this;
    }

    public CellMap combineWith(CellMap other) {
        for (Entry<Cell, CellType> e : other.cells.entrySet())
            cells.put(e.getKey(), e.getValue());

        return this;
    }

    public long getTotalCellsNum() {
        return cells.keySet().size();
    }

    public long getDenseCellsNum() {
        long n = 0;

        for (Map.Entry<Cell, CellType> e : cells.entrySet()) {
            if (e.getValue() == CellType.DENSE)
                n++;
        }

        return n;
    }

    public List<Cell> getNotEmptyNeighborsOf(Cell cell) {
        List<Cell> possibleNeighbors = cell.generateNeighbors();
        List<Cell> effectiveNeighbors = new ArrayList<>(possibleNeighbors.size());

        for (Cell n : possibleNeighbors) {
            if (getCellType(n) != CellType.EMPTY)
                effectiveNeighbors.add(n);
        }

        return effectiveNeighbors;
    }

    public List<Cell> getCoreNeighborsOf(Cell cell) {
        List<Cell> possibleNeighbors = cell.generateNeighbors();
        List<Cell> effectiveNeighbors = new ArrayList<>(possibleNeighbors.size());

        for (Cell n : possibleNeighbors) {
            if (getCellType(n) == CellType.CORE || getCellType(n) == CellType.DENSE)
                effectiveNeighbors.add(n);
        }

        return effectiveNeighbors;
    }

}