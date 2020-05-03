package com.youbout.batchperformance.synchronous;

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

import javax.sql.DataSource;
import java.util.Collections;

@SpringBootApplication
@EnableBatchProcessing
public class SynchronousBatchPerformanceApplication {

    public static void main(String[] args) {
        SpringApplication.run(SynchronousBatchPerformanceApplication.class, args);
    }

    @Bean
    public Job synJob(JobBuilderFactory jobBuilderFactory) throws Exception {
        return jobBuilderFactory
                .get("Synchronous JOB")
                .incrementer(new RunIdIncrementer())
                .flow(synManagerStep(null))
                .end()
                .build();
    }

    @Bean
    public Step synManagerStep(StepBuilderFactory stepBuilderFactory) throws Exception {
        return stepBuilderFactory
                .get("Synchronous : Read -> Process -> Write ")
                .<TransactionVO, TransactionVO>chunk(1000)
                .reader(syncReader(null))
                .processor(synchProcessor())
                .writer(syncWriter())
                .build();
    }

    @Bean
    public ItemProcessor<TransactionVO, TransactionVO> synchProcessor() {
        return (transaction) -> {
            Thread.sleep(1);
            return transaction;
        };
    }

    @Bean
    public ItemReader<TransactionVO> syncReader(DataSource dataSource) throws Exception {

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
    public FlatFileItemWriter<TransactionVO> syncWriter() {

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
