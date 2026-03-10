import React from 'react';
import { Text, Box } from 'ink';
import chalk from 'chalk';
function truncate(s, maxLen) {
    if (s.length <= maxLen)
        return s.padEnd(maxLen);
    return s.slice(0, maxLen - 1) + '…';
}
export function Table({ columns, data, emptyMessage = 'No items found.', }) {
    if (data.length === 0) {
        return React.createElement(Text, { color: "gray" }, emptyMessage);
    }
    const widths = columns.map(col => {
        const headerLen = col.header.length;
        const dataLen = Math.max(...data.map(row => {
            const val = col.render
                ? col.render(row[col.key], row)
                : String(row[col.key] ?? '');
            return val.length;
        }));
        return col.width ?? Math.min(Math.max(headerLen, dataLen), 40);
    });
    const header = widths
        .map((w, i) => ' ' + chalk.bold(columns[i].header.padEnd(w)) + ' ')
        .join('│');
    const headerSep = widths.map(w => '─'.repeat(w + 2)).join('┬');
    const rowSep = widths.map(w => '─'.repeat(w + 2)).join('┼');
    const footerSep = widths.map(w => '─'.repeat(w + 2)).join('┴');
    const rows = data.map(row => widths
        .map((w, i) => {
        const col = columns[i];
        const val = col.render
            ? col.render(row[col.key], row)
            : String(row[col.key] ?? '');
        return ' ' + truncate(val, w) + ' ';
    })
        .join('│'));
    return (React.createElement(Box, { flexDirection: "column" },
        React.createElement(Text, null, '┌' + headerSep + '┐'),
        React.createElement(Text, null, '│' + header + '│'),
        React.createElement(Text, null, '├' + rowSep + '┤'),
        rows.map((row, i) => (React.createElement(React.Fragment, { key: i },
            React.createElement(Text, null, '│' + row + '│'),
            i < rows.length - 1 && React.createElement(Text, null, '├' + rowSep + '┤')))),
        React.createElement(Text, null, '└' + footerSep + '┘')));
}
//# sourceMappingURL=table.js.map