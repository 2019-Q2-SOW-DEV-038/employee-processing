package com.conduent.poc.employeeprocessing.config;

import com.conduent.poc.employeeprocessing.model.Employee;
import com.conduent.poc.employeeprocessing.partitioner.EmployeePartitioner;
import com.conduent.poc.employeeprocessing.processor.CustomEmployeeProcessor;
import com.conduent.poc.employeeprocessing.readers.CustomEmployeeReader;
import com.conduent.poc.employeeprocessing.writers.CustomEmployeeWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.List;

@EnableBatchProcessing
@Configuration
public class EmployeeProcessingConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job employeeProcessingJob(){
        return jobBuilderFactory.get("employee-processing")
                .start(processsEmployees())
                .build();
    }

    @Bean
    public Step processsEmployees() {
        return stepBuilderFactory.get("employee-processing-step")
                .listener(employeePartitioner())
                .partitioner("employee-partitioner",employeePartitioner())
                .step(employeeProcessingStep())
                .gridSize(4)
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }


    @Bean
    public Step employeeProcessingStep() {
        return stepBuilderFactory.get("employee-processing-slave-step")
                .<Employee,Employee>chunk(2)
                .reader(customEmployeeReader(null))
                .processor(customEmployeeProcessor(null))
                .writer(customEmployeeWriter())
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<Employee> customEmployeeWriter() {
        return new CustomEmployeeWriter();
    }

    @Bean
    @StepScope
    public ItemProcessor<Employee,Employee> customEmployeeProcessor(@Value("#{stepExecutionContext[ThreadName]}") String threadName) {
        return new CustomEmployeeProcessor(threadName);
    }

    @Bean
    @StepScope
    public ItemReader<Employee> customEmployeeReader(@Value("#{stepExecutionContext[employeeList]}")List<Employee> employees) {
        return new CustomEmployeeReader(employees);
    }

    @Bean
    public Partitioner employeePartitioner() {
        return new EmployeePartitioner(employeeReader());
    }

    @Bean
    public FlatFileItemReader<Employee> employeeReader()
    {
        FlatFileItemReader<Employee> flatFileItemReader=new FlatFileItemReader<>();
        flatFileItemReader.setResource(new ClassPathResource("employee-data.csv"));
        flatFileItemReader.setLineMapper(lineMapper());
        return flatFileItemReader;
    }

    @Bean
    public LineMapper<Employee> lineMapper() {

        DefaultLineMapper<Employee> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("employeeId", "firstName", "lastName");

        BeanWrapperFieldSetMapper<Employee> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Employee.class);

        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(fieldSetMapper);

        return defaultLineMapper;
    }
}
