import React, { useState, useEffect } from 'react';
import { Text } from 'ink';
import chalk from 'chalk';

const FRAMES = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

interface SpinnerProps {
  message?: string;
}

export const Spinner: React.FC<SpinnerProps> = ({ message = 'Loading...' }) => {
  const [frame, setFrame] = useState(0);

  useEffect(() => {
    const timer = setInterval(() => {
      setFrame(f => (f + 1) % FRAMES.length);
    }, 80);
    return () => clearInterval(timer);
  }, []);

  return (
    <Text>
      {chalk.cyan(FRAMES[frame]!)} {message}
    </Text>
  );
};
