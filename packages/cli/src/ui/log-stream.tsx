import React, { useState, useEffect, useRef } from 'react';
import { Box, Text, useApp } from 'ink';
import chalk from 'chalk';
import type { MuliClients } from '../grpc.js';

interface LogLine {
  stream: string;
  line: string;
  sequence: number;
}

interface LogStreamProps {
  jobId: string;
  clients: MuliClients;
  follow?: boolean;
  onDone?: (exitCode: number) => void;
}

const STATE_COLORS: Record<string, string> = {
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

export const LogStream: React.FC<LogStreamProps> = ({
  jobId,
  clients,
  follow = false,
  onDone,
}) => {
  const [lines, setLines] = useState<LogLine[]>([]);
  const [status, setStatus] = useState<string>('PENDING');
  const [done, setDone] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const { exit } = useApp();
  const streamRef = useRef<any>(null);

  useEffect(() => {
    const stream = clients.log.StreamLogs({ job_id: jobId }, clients.meta);
    streamRef.current = stream;

    stream.on('data', (entry: any) => {
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

    stream.on('error', (err: Error) => {
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
        await new Promise<void>((resolve, reject) => {
          clients.job.GetJobStatus(
            { job_id: jobId },
            clients.meta,
            (err: any, res: any) => {
              if (err) { reject(err); return; }
              if (res?.state) {
                setStatus(String(res.state));
                if (TERMINAL_STATES.has(String(res.state))) {
                  setDone(true);
                  clearInterval(pollInterval);
                }
              }
              resolve();
            },
          );
        });
      } catch {
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

  const statusColor = (STATE_COLORS[status] ?? 'white') as any;
  const displayLines = lines.slice(-50);

  return (
    <Box flexDirection="column" borderStyle="single" borderColor="gray" paddingX={1}>
      <Box justifyContent="space-between">
        <Text bold>Job {jobId.slice(0, 8)}…</Text>
        <Text color={statusColor} bold>
          {status}
        </Text>
      </Box>
      <Box flexDirection="column" marginTop={1}>
        {displayLines.length === 0 && !done && (
          <Text color="gray">Waiting for output…</Text>
        )}
        {displayLines.map((l, i) => {
          const streamStr = l.stream.toUpperCase();
          const prefix =
            streamStr === 'STDERR'
              ? chalk.red('[stderr]')
              : streamStr === 'SYSTEM'
              ? chalk.cyan('[system]')
              : chalk.dim('[stdout]');
          return (
            <Text key={i}>
              {prefix} {l.line}
            </Text>
          );
        })}
      </Box>
      {error && !done && (
        <Box marginTop={1}>
          <Text color="red">Stream error: {error}</Text>
        </Box>
      )}
      {done && (
        <Box marginTop={1}>
          <Text color={status === 'SUCCEEDED' ? 'green' : 'red'}>
            {status === 'SUCCEEDED'
              ? '✓ Job completed successfully'
              : `✗ Job ${status.toLowerCase()}`}
          </Text>
        </Box>
      )}
    </Box>
  );
};
