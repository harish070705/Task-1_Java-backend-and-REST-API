package com.example.taskrunner.controller;
import com.example.taskrunner.model.Task;
import com.example.taskrunner.model.TaskExecution;
import com.example.taskrunner.service.TaskService;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/tasks")
public class TaskController {
    private final TaskService service;

    public TaskController(TaskService service) {
        this.service = service;
    }

    // GET /api/tasks or /api/tasks?id=123
    @GetMapping
    public Object getTasks(@RequestParam(value = "id", required = false) String id) {
        if (id == null)
            return service.getAll();
        return service.getById(id);
    }

    // PUT /api/tasks (create/update)
    @PutMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Task putTask(@RequestBody @Validated Task task) {
        return service.save(task);
    }

    // DELETE /api/tasks/{id}
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteTask(@PathVariable String id) {
        service.delete(id);
    }

    // GET /api/tasks/search?name=part
    @GetMapping("/search")
    public List<Task> searchByName(@RequestParam("name") String name) {
        return service.findByName(name);
    }

    // PUT /api/tasks/{id}/run -> triggers execution and stores TaskExecution
    @PutMapping("/{id}/run")
    public TaskExecution runTask(@PathVariable String id) throws Exception {
        return service.runTask(id);
    }
}
