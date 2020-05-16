package com.youbout.batchperformance.asyncprcessing;

import com.youbout.batchperformance.TransactionVO;
import com.youbout.batchperformance.TransactionVORowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
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
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

@SpringBootApplication
@EnableBatchProcessing
public class AsyncProcessingBatchPerformanceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AsyncProcessingBatchPerformanceApplication.class, args);
    }

    @Bean
    public Job asyncJob(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory
                .get("Asynchronous Processing JOB")
                .incrementer(new RunIdIncrementer())
                .flow(asyncManagerStep(null))
                .end()
                .build();
    }

    @Bean
    public Step asyncManagerStep(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory
                .get("Asynchronous Processing : Read -> Process -> Write ")
                .<TransactionVO, Future<TransactionVO>>chunk(1000)
                .reader(asyncReader(null))
                .processor(asyncProcessor())
                .writer(asyncWriter())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public AsyncItemProcessor<TransactionVO, TransactionVO> asyncProcessor() {
        AsyncItemProcessor<TransactionVO, TransactionVO> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(itemProcessor());
        asyncItemProcessor.setTaskExecutor(taskExecutor());

        return asyncItemProcessor;
    }

    @Bean
    public AsyncItemWriter<TransactionVO> asyncWriter() {
        AsyncItemWriter<TransactionVO> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(itemWriter());
        return asyncItemWriter;
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
    public ItemProcessor<TransactionVO, TransactionVO> itemProcessor() {
        return (transaction) -> {
            Thread.sleep(1);
            return transaction;
        };
    }

    @Bean
    public ItemReader<TransactionVO> asyncReader(DataSource dataSource) {

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
    public FlatFileItemWriter<TransactionVO> itemWriter() {

        return new FlatFileItemWriterBuilder<TransactionVO>()
                .name("Writer")
                .append(false)
                .resource(new FileSystemResource("transactions.txt"))
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
