package com.tech.demo.sqoop;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

/**
 * @author xxx_xx
 * @date 2018/4/30
 */
public class SqoopTest {

    private SqoopClient client;

    public SqoopTest() {
        String url = "http://centos-2:12000/sqoop/";
        client = new SqoopClient(url);
    }


    public void saveJob() {
        Long fromLinkId = 1L;
        Long toLinkId = 2L;
        MJob job = client.createJob(fromLinkId, toLinkId);
        job.setName("firstjob");
        job.setCreationUser("hadoop");
        MFromConfig fromJobConfig = job.getFromJobConfig();
        fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue("syn");
        fromJobConfig.getStringInput("fromJobConfig.tableName").setValue("student");
        MToConfig toJobConfig = job.getToJobConfig();
        toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue("/sqoop/");
        Status status = client.saveJob(job);
        job.getPersistenceId();
        if (status.canProceed()) {
            System.out.println("Created Job with Job Id: " + job.getPersistenceId());
        } else {
            System.out.println("Something went wrong creating the job");
        }
    }

    public void startJob() {
        Long jobId = 1L;
        MSubmission submission = client.startJob(jobId);
        System.out.println("Job Submission Status : " + submission.getStatus());
        if (submission.getStatus().isRunning() && submission.getProgress() != -1) {
            System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
        }
        System.out.println("Hadoop job id :" + submission.getExternalJobId());
        System.out.println("Job link : " + submission.getExternalLink());
        Counters counters = submission.getCounters();
        if (counters != null) {
            System.out.println("Counters:");
            for (CounterGroup group : counters) {
                System.out.print("\t");
                System.out.println(group.getName());
                for (Counter counter : group) {
                    System.out.print("\t\t");
                    System.out.print(counter.getName());
                    System.out.print(": ");
                    System.out.println(counter.getValue());
                }
            }
        }

        MSubmission sub = client.getJobStatus(jobId);
        if (sub.getStatus().isRunning() && sub.getProgress() != -1) {
            System.out.println("Progress : " + String.format("%.2f %%", sub.getProgress() * 100));
        }
    }

    public static void main(String[] args) {
        SqoopTest test = new SqoopTest();
        test.saveJob();
        test.startJob();
    }
}
