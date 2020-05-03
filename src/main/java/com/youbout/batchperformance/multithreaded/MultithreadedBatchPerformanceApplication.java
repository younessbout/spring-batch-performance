package com.youbout.batchperformance.multithreaded;

import com.youbout.batchperformance.TransactionVO;
import com.youbout.batchperformance.TransactionVORowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

@SpringBootApplication
@EnableBatchProcessing
public class MultithreadedBatchPerformanceApplication {

    public static void main(String[] args) {
        SpringApplication.run(MultithreadedBatchPerformanceApplication.class, args);
    }

    @Bean
    public Job multithreadedJob(JobBuilderFactory jobBuilderFactory) throws Exception {
        return jobBuilderFactory
                .get("Multithreaded JOB")
                .incrementer(new RunIdIncrementer())
                .flow(multithreadedManagerStep(null))
                .end()
                .build();
    }

    @Bean
    public Step multithreadedManagerStep(StepBuilderFactory stepBuilderFactory) throws Exception {
        return stepBuilderFactory
                .get("Multithreaded : Read -> Process -> Write ")
                .<TransactionVO, TransactionVO>chunk(1000)
                .reader(multithreadedcReader(null))
                .processor(multithreadedchProcessor())
                .writer(multithreadedcWriter())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(64);
        executor.setMaxPoolSize(64);
        executor.setQueueCapacity(64);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("MultiThreaded-");
        return executor;
    }

    @Bean
    public ItemProcessor<TransactionVO, TransactionVO> multithreadedchProcessor() {
        return (transaction) -> {
            Thread.sleep(1);
            //og.info(Thread.currentThread().getName());
            return transaction;
        };
    }

    @Bean
    public ItemReader<TransactionVO> multithreadedcReader(DataSource dataSource) {

        return new JdbcPagingItemReaderBuilder<TransactionVO>()
                .name("Reader")
                .dataSource(dataSource)
                .selectClause("SELECT * ")
                .fromClause("FROM transactions ")
                .whereClause("WHERE ID <= 1000000 ")
                .sortKeys(Collections.singletonMap("ID", Order.ASCENDING))
                .rowMapper(new TransactionVORowMapper())
                .build();
    }

    @Bean
    public FlatFileItemWriter<TransactionVO> multithreadedcWriter() {

        return new FlatFileItemWriterBuilder<TransactionVO>()
                .name("Writer")
                .append(false)
                .resource(new FileSystemResource("transactions_2.txt"))
                .lineAggregator(new DelimitedLineAggregator<TransactionVO>() {
                    {
                        setDelimiter(";");
                        setFieldExtractor(new BeanWrapperFieldExtractor<TransactionVO>() {
                            {
                                setNames(new String[]{"id", "date", "amount", "createdAt"});
                            }
                        });
                    }
                })
                .build();
    }
}
