package com.example.taskrunner.service;
import com.example.taskrunner.model.TaskExecution;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;

@Service("k8sExecutor")
@Profile("k8s") 
public class KubernetesExecutorService implements ExecutorService {

    private final KubernetesClient client;

    @Value("${task.k8s.namespace:default}")
    private String namespace;

    @Value("${task.k8s.pod:}")
    private String podName;

    @Value("${task.k8s.container:}")
    private String containerName;

    public KubernetesExecutorService(KubernetesClient client) {
        this.client = client;
    }

    @Override
    public TaskExecution execute(String command) throws Exception {
        Instant start = Instant.now();

        if (podName == null || podName.isEmpty()) {
            throw new IllegalStateException("Pod name not configured (task.k8s.pod)");
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CountDownLatch latch = new CountDownLatch(1);

        ExecWatch watch = client.pods()
                .inNamespace(namespace)
                .withName(podName)
                .inContainer(containerName != null && !containerName.isEmpty() ? containerName : null)
                .writingOutput(baos)
                .writingError(baos)
                .usingListener(new ExecListener() {
                    @Override
                    public void onOpen() {
                    }

                    @Override
                    public void onFailure(Throwable t,
                            io.fabric8.kubernetes.client.dsl.ExecListener.Response response) {
                        latch.countDown();
                    }

                    @Override
                    public void onClose(int code, String reason) {
                        latch.countDown();
                    }
                })
                .exec("bash", "-lc", command);

        latch.await(); 
        String output = baos.toString("UTF-8");
        Instant end = Instant.now();

        watch.close();
        return new TaskExecution(start, end, output);
    }
}
