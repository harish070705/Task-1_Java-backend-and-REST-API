package com.example.taskrunner.service;
import com.example.taskrunner.model.TaskExecution;

public interface ExecutorService {
    TaskExecution execute(String command) throws Exception;
}
