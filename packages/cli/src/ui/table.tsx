import React from 'react';
import { Text, Box } from 'ink';
import chalk from 'chalk';

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

function truncate(s: string, maxLen: number): string {
  if (s.length <= maxLen) return s.padEnd(maxLen);
  return s.slice(0, maxLen - 1) + '…';
}

export function Table<T>({
  columns,
  data,
  emptyMessage = 'No items found.',
}: TableProps<T>) {
  if (data.length === 0) {
    return <Text color="gray">{emptyMessage}</Text>;
  }

  const widths = columns.map(col => {
    const headerLen = col.header.length;
    const dataLen = Math.max(
      ...data.map(row => {
        const val = col.render
          ? col.render(row[col.key], row)
          : String(row[col.key] ?? '');
        return val.length;
      }),
    );
    return col.width ?? Math.min(Math.max(headerLen, dataLen), 40);
  });

  const header = widths
    .map((w, i) => ' ' + chalk.bold(columns[i]!.header.padEnd(w)) + ' ')
    .join('│');
  const headerSep = widths.map(w => '─'.repeat(w + 2)).join('┬');
  const rowSep = widths.map(w => '─'.repeat(w + 2)).join('┼');
  const footerSep = widths.map(w => '─'.repeat(w + 2)).join('┴');

  const rows = data.map(row =>
    widths
      .map((w, i) => {
        const col = columns[i]!;
        const val = col.render
          ? col.render(row[col.key], row)
          : String(row[col.key] ?? '');
        return ' ' + truncate(val, w) + ' ';
      })
      .join('│'),
  );

  return (
    <Box flexDirection="column">
      <Text>{'┌' + headerSep + '┐'}</Text>
      <Text>{'│' + header + '│'}</Text>
      <Text>{'├' + rowSep + '┤'}</Text>
      {rows.map((row, i) => (
        <React.Fragment key={i}>
          <Text>{'│' + row + '│'}</Text>
          {i < rows.length - 1 && <Text>{'├' + rowSep + '┤'}</Text>}
        </React.Fragment>
      ))}
      <Text>{'└' + footerSep + '┘'}</Text>
    </Box>
  );
}
