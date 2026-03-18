package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.EqualsAndHashCode;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class ColumnRef {
    private final int fileId;
    private final int columnIndex;
    private final String columnName;
}


