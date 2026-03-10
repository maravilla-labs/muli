import React, { useState, useEffect } from 'react';
import { Text } from 'ink';
import chalk from 'chalk';
const FRAMES = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
export const Spinner = ({ message = 'Loading...' }) => {
    const [frame, setFrame] = useState(0);
    useEffect(() => {
        const timer = setInterval(() => {
            setFrame(f => (f + 1) % FRAMES.length);
        }, 80);
        return () => clearInterval(timer);
    }, []);
    return (React.createElement(Text, null,
        chalk.cyan(FRAMES[frame]),
        " ",
        message));
};
//# sourceMappingURL=spinner.js.map