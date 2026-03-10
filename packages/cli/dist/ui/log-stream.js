import React, { useState, useEffect, useRef } from 'react';
import { Box, Text, useApp } from 'ink';
import chalk from 'chalk';
const STATE_COLORS = {
    PENDING: 'yellow',
    SCHEDULED: 'yellow',
    PULLING: 'cyan',
    RUNNING: 'green',
    SUCCEEDED: 'green',
    FAILED: 'red',
    CANCELLED: 'gray',
    TIMED_OUT: 'red',
};
const TERMINAL_STATES = new Set([
    'SUCCEEDED',
    'FAILED',
    'CANCELLED',
    'TIMED_OUT',
]);
export const LogStream = ({ jobId, clients, follow = false, onDone, }) => {
    const [lines, setLines] = useState([]);
    const [status, setStatus] = useState('PENDING');
    const [done, setDone] = useState(false);
    const [error, setError] = useState(null);
    const { exit } = useApp();
    const streamRef = useRef(null);
    useEffect(() => {
        const stream = clients.log.StreamLogs({ job_id: jobId }, clients.meta);
        streamRef.current = stream;
        stream.on('data', (entry) => {
            if (entry.line !== undefined && entry.line !== '') {
                setLines(prev => [
                    ...prev,
                    {
                        stream: String(entry.stream ?? 'STDOUT'),
                        line: String(entry.line),
                        sequence: Number(entry.sequence ?? 0),
                    },
                ]);
            }
        });
        stream.on('error', (err) => {
            // Stream ended or server closed; mark done
            if (!done) {
                setError(err.message);
                setDone(true);
            }
        });
        stream.on('end', () => {
            setDone(true);
        });
        // Poll job status to track state transitions
        const pollInterval = setInterval(async () => {
            try {
                await new Promise((resolve, reject) => {
                    clients.job.GetJobStatus({ job_id: jobId }, clients.meta, (err, res) => {
                        if (err) {
                            reject(err);
                            return;
                        }
                        if (res?.state) {
                            setStatus(String(res.state));
                            if (TERMINAL_STATES.has(String(res.state))) {
                                setDone(true);
                                clearInterval(pollInterval);
                            }
                        }
                        resolve();
                    });
                });
            }
            catch {
                // ignore polling errors
            }
        }, 2000);
        return () => {
            clearInterval(pollInterval);
            streamRef.current?.cancel?.();
        };
    }, [jobId]); // eslint-disable-line react-hooks/exhaustive-deps
    useEffect(() => {
        if (done) {
            const exitCode = status === 'SUCCEEDED' ? 0 : 1;
            onDone?.(exitCode);
            setTimeout(() => exit(), 400);
        }
    }, [done]); // eslint-disable-line react-hooks/exhaustive-deps
    const statusColor = (STATE_COLORS[status] ?? 'white');
    const displayLines = lines.slice(-50);
    return (React.createElement(Box, { flexDirection: "column", borderStyle: "single", borderColor: "gray", paddingX: 1 },
        React.createElement(Box, { justifyContent: "space-between" },
            React.createElement(Text, { bold: true },
                "Job ",
                jobId.slice(0, 8),
                "\u2026"),
            React.createElement(Text, { color: statusColor, bold: true }, status)),
        React.createElement(Box, { flexDirection: "column", marginTop: 1 },
            displayLines.length === 0 && !done && (React.createElement(Text, { color: "gray" }, "Waiting for output\u2026")),
            displayLines.map((l, i) => {
                const streamStr = l.stream.toUpperCase();
                const prefix = streamStr === 'STDERR'
                    ? chalk.red('[stderr]')
                    : streamStr === 'SYSTEM'
                        ? chalk.cyan('[system]')
                        : chalk.dim('[stdout]');
                return (React.createElement(Text, { key: i },
                    prefix,
                    " ",
                    l.line));
            })),
        error && !done && (React.createElement(Box, { marginTop: 1 },
            React.createElement(Text, { color: "red" },
                "Stream error: ",
                error))),
        done && (React.createElement(Box, { marginTop: 1 },
            React.createElement(Text, { color: status === 'SUCCEEDED' ? 'green' : 'red' }, status === 'SUCCEEDED'
                ? '✓ Job completed successfully'
                : `✗ Job ${status.toLowerCase()}`)))));
};
//# sourceMappingURL=log-stream.js.map