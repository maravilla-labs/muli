import React from 'react';
export interface Column<T> {
    header: string;
    key: keyof T;
    width?: number;
    render?: (value: T[keyof T], row: T) => string;
}
interface TableProps<T> {
    columns: Column<T>[];
    data: T[];
    emptyMessage?: string;
}
export declare function Table<T>({ columns, data, emptyMessage, }: TableProps<T>): React.JSX.Element;
export {};
//# sourceMappingURL=table.d.ts.map