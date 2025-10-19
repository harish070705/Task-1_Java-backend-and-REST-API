package com.example.taskrunner.service;
import com.example.taskrunner.model.Task;
import com.example.taskrunner.model.TaskExecution;
import com.example.taskrunner.repository.TaskRepository;
import com.example.taskrunner.util.CommandValidator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import java.util.List;
import static org.springframework.http.HttpStatus.*;

@Service
public class TaskService {

    private final TaskRepository repository;
    private final ExecutorService executor;

    public TaskService(TaskRepository repository,
            @Qualifier("k8sExecutor") ExecutorService executor) { 
        this.repository = repository;
        this.executor = executor;
    }

    public List<Task> getAll() {
        return repository.findAll();
    }

    public Task getById(String id) {
        return repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Task not found"));
    }

    public Task save(Task t) {
        if (!CommandValidator.isSafe(t.getCommand())) {
            throw new ResponseStatusException(BAD_REQUEST, "Unsafe command");
        }
        return repository.save(t);
    }

    public void delete(String id) {
        if (!repository.existsById(id)) {
            throw new ResponseStatusException(NOT_FOUND, "Task not found");
        }
        repository.deleteById(id);
    }

    public List<Task> findByName(String namePart) {
        List<Task> found = repository.findByNameContainingIgnoreCase(namePart);
        if (found.isEmpty())
            throw new ResponseStatusException(NOT_FOUND, "No tasks found");
        return found;
    }

    public TaskExecution runTask(String taskId) throws Exception {
        Task t = repository.findById(taskId)
                .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "Task not found"));

        if (!CommandValidator.isSafe(t.getCommand())) {
            throw new ResponseStatusException(BAD_REQUEST, "Unsafe command");
        }

        TaskExecution exec = executor.execute(t.getCommand());
        t.getTaskExecutions().add(exec);
        repository.save(t);
        return exec;
    }
}
