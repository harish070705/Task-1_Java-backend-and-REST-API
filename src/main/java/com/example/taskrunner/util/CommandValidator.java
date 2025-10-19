package com.example.taskrunner.util;

import java.util.Arrays;
import java.util.List;

public class CommandValidator {
    // Whitelist of allowed commands (single executable names).
    private static final List<String> WHITELIST = Arrays.asList(
        "echo", "date", "whoami", "uptime", "ls", "cat", "hostname"
    );

    public static boolean isSafe(String command) {
        if (command == null || command.trim().isEmpty()) return false;
        // Basic sanitization: no pipes, no redirections, no backgrounding, no &&, no ;, no $(
        String lower = command.toLowerCase();
        String[] forbidden = {"|", "&&", ";", "$(", "`", ">", "<", "rm ", "sudo", "shutdown", "reboot"};
        for (String f: forbidden) {
            if (lower.contains(f)) return false;
        }

        // Extract the first token (the executable)
        String firstToken = command.trim().split("\\s+")[0];
        // Strip path components like /bin/echo => echo
        if (firstToken.contains("/")) {
            firstToken = firstToken.substring(firstToken.lastIndexOf('/')+1);
        }
        return WHITELIST.contains(firstToken);
    }
}
