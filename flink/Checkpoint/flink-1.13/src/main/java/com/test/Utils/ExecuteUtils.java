package com.test.Utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

@Slf4j
public class ExecuteUtils {
    public static void execute(StreamExecutionEnvironment env) throws Exception {
        log.info("----执行流图----");
        log.info(env.getExecutionPlan());
        log.info("----执行流图----");
        JobClient jobClient = null;
        try {
            jobClient = env.executeAsync();
        } catch (Exception e) {
            log.error("start error:", e);
            throw e;
        }
        JobID jobID = jobClient.getJobID();
        log.info("job id is {}", jobID);
        jobClient.getJobExecutionResult().get();

    }

}
