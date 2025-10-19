package com.example.taskrunner.service;
import com.example.taskrunner.model.TaskExecution;
import org.springframework.stereotype.Service;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.stream.Collectors;

@Service("localExecutor") 
public class LocalExecutorService implements ExecutorService {

    @Override
    public TaskExecution execute(String command) throws Exception {
        Instant start = Instant.now();

        ProcessBuilder pb = new ProcessBuilder("bash", "-lc", command);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        String output;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            output = br.lines().collect(Collectors.joining("\n"));
        }

        int exit = p.waitFor();
        Instant end = Instant.now();
        String outWithExit = output + "\n[exitCode=" + exit + "]";

        return new TaskExecution(start, end, outWithExit);
    }
}
